package recommender

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

object FPGrowthExec {
  def main(args: Array[String]): Unit = {
    /** Initial configuration */

    /** PROGRAM START */
    val conf = new SparkConf().setAppName("Custom FP Growth").setMaster("local[2]").set("spark.executor.memory", "1g")
    //val conf = new SparkConf().setAppName("SimpleFPGrowth")
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    /** FP-Growth algorithm and Itemset Generation */
    val data = sc.textFile("processed-transactions.csv")
    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' ').distinct)  //Added Distinct
    //data.collect().foreach(println)
    val fpg = new CustomFPGrowth()
      .setMinSupport(0.075)
      .setNumPartitions(10)
      .setAdaptiveMap(DatasetProcessing.calculateMultipleSupport())
    /** Start time execution calculation*/
    val itemSetTime = System.currentTimeMillis()
    val model = fpg.run(transactions)
    val endItemSetTime = System.currentTimeMillis()-itemSetTime

    model.freqItemsets.collect().foreach {
      itemset => println(DatasetProcessing.getDescriptionFromID(itemset.items).mkString("[", ",", "]") + ", " + itemset.freq)
      //println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq) //OLD VERSION
    }

    /** Rule generation with minConfidence threshold */
    val minConfidence = 0.2
    val rulesTime = System.currentTimeMillis()
    val rules = model.generateAssociationRules(minConfidence)
    val endRulesTime = System.currentTimeMillis()-rulesTime

      rules.collect().foreach { rule =>

      println(
        DatasetProcessing.getDescriptionFromID(rule.antecedent).mkString("[", ",", "]")
          + " => " + DatasetProcessing.getDescriptionFromID(rule.consequent).mkString("[", ",", "]")
          + ", " + rule.confidence)

      /*println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent.mkString("[", ",", "]")
          + ", " + rule.confidence)*/
    }

    /** PROGRAM END */
    println("\nItemsets generation elapsed time: " + endItemSetTime + " ms")
    println("\nRule generation elapsed time: " + endRulesTime + " ms")
    println("\nTotal elapsed time: " + (endItemSetTime + endRulesTime) + " ms")
  }
}
