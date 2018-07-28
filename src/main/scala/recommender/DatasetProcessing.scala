package recommender
import scala.io
import java.io._

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object DatasetProcessing {

  var transactions = ""

  def createTransactionFile() {
    val bufferedSource = io.Source.fromFile("online_retail_edit.csv")
    var no = "536365"
    val pw = new PrintWriter(new File("transactions.csv" ))
    for (line <- bufferedSource.getLines) {
      val cols = line.split(",").map(_.trim)
      if (cols(0).equals(no)){
        transactions += cols(1) + " "
      }
      else{
        transactions += "\n"
        no = cols(0)
        transactions += cols(1) + " "
      }
      //println(s"${cols(0)}|${cols(1)}|${cols(2)}|${cols(5)}|${cols(6)}")
    }
    pw.write(transactions)
    bufferedSource.close
    pw.close()
  }

  def deleteNoise(): Unit = {
    val bufferedSource = io.Source.fromFile("transactions.csv")
    val pw = new PrintWriter(new File("processed-transactions.csv" ))
    for (line <- bufferedSource.getLines) {
      if (!(line.length() < 10)){
        pw.write(line + "\n")
      }
    }
    bufferedSource.close
    pw.close()
  }

  //createTransactionFile()
  //deleteNoise()

  def calculateMultipleSupport(sc: SparkContext): scala.collection.mutable.Map[String,Int] =
  {
    val data = sc.textFile("online_retail_edit.csv")
    //val pw = new PrintWriter(new File("price-value.csv"))

    var map = scala.collection.mutable.Map[String,Int]()
    //var newMap = scala.collection.mutable.Map[String,Float]()

    val items: RDD[Array[String]] = data.map(s => s.trim.split(','))

    items.map( v =>
                if(!map.contains(v(1)))
                  {
                    map.put(v(1),v(5).toFloat.toInt)
                  }
                else
                {
                  val prevVal = map.getOrElse(v(1), 0)
                  val newVal = math.ceil((v(5).toFloat.toInt+prevVal)/2).toInt
                  map.put(v(1), newVal)
                })
    map
  }
  //calculateMultipleSupport()

  def getDescriptionFromID(id: Array[String]): Array[String] = {
    val bufferedSource = io.Source.fromFile("online_retail_edit.csv")
    var id_description = scala.collection.mutable.Map[String,String]()
    for (line <- bufferedSource.getLines)
    {
      val cols = line.split(",").map(_.trim)
      id_description.put(cols(1), cols(2))
    }
    bufferedSource.close
    var res = ArrayBuffer[String]()
    id.foreach( s => res += id_description.getOrElse(s, ""))
    res.toArray
  }

}
