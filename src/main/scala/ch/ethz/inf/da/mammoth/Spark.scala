package ch.ethz.inf.da.mammoth

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Spark Configuration
 */
object Spark {

  def createContext(): SparkContext = {
    val sparkConf = new SparkConf().setAppName("Mammoth")
    sparkConf.set("local", "true")
    sparkConf.setMaster("local[*]")
    new SparkContext(sparkConf)
  }

}
