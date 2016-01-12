import org.apache.spark
import org.apache.spark.SparkContext

/**
  * Created by Huangsb on 2016/1/12.
  */

class wordcount {
  def main(args: Array[String]) {
    val spark=new SparkContext()
    val textFile = spark.textFile("hdfs://...")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile("hdfs://...")
  }
}
