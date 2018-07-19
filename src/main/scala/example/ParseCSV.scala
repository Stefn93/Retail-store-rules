package example
import scala.io
import java.io._

object ParseCSV extends App {

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

  def deleteSingleTransations(): Unit = {
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

  //parseFile()
  //deleteSingleTransations()



  def calculateMultipleSupport(): scala.collection.mutable.Map[String,Int] =
  {
    val bufferedSource = io.Source.fromFile("online_retail_edit.csv")
    val pw = new PrintWriter(new File("price-value.csv"))

    var map = scala.collection.mutable.Map[String,Int]()
    var newMap = scala.collection.mutable.Map[String,Float]()

    for (line <- bufferedSource.getLines)
      {
        val cols = line.split(",").map(_.trim)
        map.put(cols(1), cols(5).toFloat.toInt)
      }

    bufferedSource.getLines

    val max : Float = map.maxBy(_._2)._2
    val min : Float= map.minBy(_._2)._2

    val bufferedSource2 = io.Source.fromFile("online_retail_edit.csv")
    for (line <- bufferedSource2.getLines())
    {
      var cols = line.split(",").map(_.trim)
      newMap.put(cols(1), 1 - (cols(5).toFloat / max))
    }

    map
  }

  //calculateMultipleSupport()

}
