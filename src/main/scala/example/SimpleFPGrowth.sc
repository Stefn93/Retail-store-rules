import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD

object SimpleFPGrowth {

    val conf = new SparkConf().setAppName("SimpleFPGrowth")
    val sc = new SparkContext(conf)
    // $example on$
    val data = sc.textFile("retail.dat")
    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))
    val fpg = new FPGrowth()
      .setMinSupport(0.2)
      .setNumPartitions(10)
    val model = fpg.run(transactions)
    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }
    val minConfidence = 0.8
    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent .mkString("[", ",", "]")
          + ", " + rule.confidence)
    }

}