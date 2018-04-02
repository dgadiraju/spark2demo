/**
  * Created by itversity on 02/04/18.
  */

import org.apache.spark.{SparkConf, SparkContext}
object GetAllConfigurations {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setMaster("local").
      setAppName("Get all configurations").
      set("spark.ui.port", "12678")
    conf.getAll.foreach(println)
  }

}
