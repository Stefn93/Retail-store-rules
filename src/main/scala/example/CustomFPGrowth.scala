package example

import java.{util => ju}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag
import org.apache.spark.{HashPartitioner, Logging, Partitioner, SparkContext, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.fpm.FPGrowthModel
import org.apache.spark.mllib.fpm.FPGrowth._
import scala.collection.mutable.ListBuffer


class CustomFPGrowth(private var minSupport: Double,
                           private var numPartitions: Int) extends Logging with Serializable {

  def this() = this(0.3, -1)


  def setMinSupport(minSupport: Double): this.type = {
    require(minSupport >= 0.0 && minSupport <= 1.0,
      s"Minimal support level must be in range [0, 1] but got $minSupport")
    this.minSupport = minSupport
    this
  }


  def setNumPartitions(numPartitions: Int): this.type = {
    require(numPartitions > 0,
      s"""Number of partitions must be positive but got $numPartitions""")
    this.numPartitions = numPartitions
    this
  }


  def run[Item: ClassTag](data: RDD[Array[Item]]): FPGrowthModel[Item] = {
    if (data.getStorageLevel == StorageLevel.NONE) {  // Livello del disco in cui viene caricato il dataset
      logWarning("Input data is not cached.")
    }
    // Numero di transazioni
    val count = data.count()
    // Calcola il minimo numero di occorrenze per rispettare la soglia, per difetto
    val minCount = math.ceil(minSupport * count).toLong
    // Se non imposti automaticamente numPartitions, importa numParts come numero di cores della macchina
    val numParts = if (numPartitions > 0) numPartitions else data.partitions.length
    // Definisce un HashPartitioner che trova il nodo di computazione per ogni partizione in base alla funzione hash % numParts
    val partitioner = new HashPartitioner(numParts)
    // Ottiene gli item frequenti a partire dai dati e dalle loro partizioni
    val freqItems = genFreqItems(data, minCount, partitioner)
    // Ottiene gli itemset frequenti a partire dai freqItems
    val freqItemsets = genFreqItemsets(data, minCount, freqItems, partitioner)
    //Risultato finale
    new FPGrowthModel(freqItemsets)
  }

  /**
    * Generates frequent items by filtering the input data using minimal support level.
    * @param minCount minimum count for frequent itemsets
    * @param partitioner partitioner used to distribute items
    * @return array of frequent pattern ordered by their frequencies
    */
  private def genFreqItems[Item: ClassTag](
                                            data: RDD[Array[Item]],
                                            minCount: Long,
                                            partitioner: Partitioner): Array[Item] = {

    data.flatMap { t =>
      val uniq = t.toSet
      if (t.length != uniq.size) {
        throw new SparkException(s"Items in a transaction must be unique but got ${t.toSeq}.")
      }
      t
    }.map(v => (v, 1L))                 // Associa ad ogni elemento della transazione il valore 1
      .reduceByKey(partitioner, _ + _)  // Somma aggregando per chiave
      .filter(_._2 >= minCount)         // Filtra sulla soglia di occorrenze minima
      .collect()                        // Restituisce l'HashMap
      .sortBy(-_._2)                    // - ordine decrescente, _._2 considerando il numero di occorrenze
      .map(_._1)                        // Da verificare: Ritorna una lista contenente solo gli item che passano la soglia minima
  }

  /**
    * Generate frequent itemsets by building FP-Trees, the extraction is done on each partition.
    * @param data transactions
    * @param minCount minimum count for frequent itemsets
    * @param freqItems frequent items
    * @param partitioner partitioner used to distribute transactions
    * @return an RDD of (frequent itemset, count)
    */
  private def genFreqItemsets[Item: ClassTag](
                                               data: RDD[Array[Item]],
                                               minCount: Long,
                                               freqItems: Array[Item],
                                               partitioner: Partitioner): RDD[FreqItemset[Item]] = {
    val itemToRank = freqItems.zipWithIndex.toMap
    data.flatMap { transaction =>
      genCondTransactions(transaction, itemToRank, partitioner)
    }.aggregateByKey(new FPTree[Int], partitioner.numPartitions)(
      (tree, transaction) => tree.add(transaction, 1L),
      (tree1, tree2) => tree1.merge(tree2))
      .flatMap { case (part, tree) =>
        tree.extract(minCount, x => partitioner.getPartition(x) == part)
      }.map { case (ranks, count) =>
      new FreqItemset(ranks.map(i => freqItems(i)).toArray, count)
    }
  }

  /**
    * Generates conditional transactions.
    * @param transaction a transaction
    * @param itemToRank map from item to their rank
    * @param partitioner partitioner used to distribute transactions
    * @return a map of (target partition, conditional transaction)
    */
  private def genCondTransactions[Item: ClassTag](
                                                   transaction: Array[Item],
                                                   itemToRank: Map[Item, Int],
                                                   partitioner: Partitioner): mutable.Map[Int, Array[Int]] = {
    val output = mutable.Map.empty[Int, Array[Int]]
    // Filter the basket by frequent items pattern and sort their ranks.
    val filtered = transaction.flatMap(itemToRank.get)
    ju.Arrays.sort(filtered)
    val n = filtered.length
    var i = n - 1
    while (i >= 0) {
      val item = filtered(i)
      val part = partitioner.getPartition(item)
      if (!output.contains(part)) {
        output(part) = filtered.slice(0, i + 1)
      }
      i -= 1
    }
    output
  }
}


object FPGrowth {

  /**
    * Frequent itemset.
    * @param items items in this itemset. Java users should call `FreqItemset.javaItems` instead.
    * @param freq frequency
    * @tparam Item item type
    *
    */
  class FreqItemset[Item] (
    val items: Array[Item],
    val freq: Long) extends Serializable {


    def javaItems: java.util.List[Item] = {
      items.toList.asJava
    }

    override def toString: String = {
      s"${items.mkString("{", ",", "}")}: $freq"
    }
  }
}



/**
  * FP-Tree data structure used in FP-Growth.
  * @tparam T item type
  */
class FPTree[T] extends Serializable {

  import FPTree._

  val root: Node[T] = new Node(null)

  private val summaries: mutable.Map[T, Summary[T]] = mutable.Map.empty

  /** Adds a transaction with count. */
  def add(t: Iterable[T], count: Long = 1L): this.type = {
    require(count > 0)
    var curr = root
    curr.count += count
    t.foreach { item =>
      val summary = summaries.getOrElseUpdate(item, new Summary)
      summary.count += count
      val child = curr.children.getOrElseUpdate(item, {
        val newNode = new Node(curr)
        newNode.item = item
        summary.nodes += newNode
        newNode
      })
      child.count += count
      curr = child
    }
    this
  }

  /** Merges another FP-Tree. */
  def merge(other: FPTree[T]): this.type = {
    other.transactions.foreach { case (t, c) =>
      add(t, c)
    }
    this
  }

  /** Gets a subtree with the suffix. */
  private def project(suffix: T): FPTree[T] = {
    val tree = new FPTree[T]
    if (summaries.contains(suffix)) {
      val summary = summaries(suffix)
      summary.nodes.foreach { node =>
        var t = List.empty[T]
        var curr = node.parent
        while (!curr.isRoot) {
          t = curr.item :: t
          curr = curr.parent
        }
        tree.add(t, node.count)
      }
    }
    tree
  }

  /** Returns all transactions in an iterator. */
  def transactions: Iterator[(List[T], Long)] = getTransactions(root)

  /** Returns all transactions under this node. */
  private def getTransactions(node: Node[T]): Iterator[(List[T], Long)] = {
    var count = node.count
    node.children.iterator.flatMap { case (item, child) =>
      getTransactions(child).map { case (t, c) =>
        count -= c
        (item :: t, c)
      }
    } ++ {
      if (count > 0) {
        Iterator.single((Nil, count))
      } else {
        Iterator.empty
      }
    }
  }

  /** Extracts all patterns with valid suffix and minimum count. */
  def extract(
               minCount: Long,
               validateSuffix: T => Boolean = _ => true): Iterator[(List[T], Long)] = {
    summaries.iterator.flatMap { case (item, summary) =>
      if (validateSuffix(item) && summary.count >= minCount) {
        Iterator.single((item :: Nil, summary.count)) ++
          project(item).extract(minCount).map { case (t, c) =>
            (item :: t, c)
          }
      } else {
        Iterator.empty
      }
    }
  }
}

object FPTree {

  /** Representing a node in an FP-Tree. */
  class Node[T](val parent: Node[T]) extends Serializable {
    var item: T = _
    var count: Long = 0L
    val children: mutable.Map[T, Node[T]] = mutable.Map.empty

    def isRoot: Boolean = parent == null
  }

  /** Summary of an item in an FP-Tree. */
  private class Summary[T] extends Serializable {
    var count: Long = 0L
    val nodes: ListBuffer[Node[T]] = ListBuffer.empty
  }
}