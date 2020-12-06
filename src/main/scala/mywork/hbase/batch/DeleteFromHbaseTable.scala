package mywork.hbase.batch

import java.io.File

import com.typesafe.config.ConfigFactory
import mywork.hbase.common.HbaseBatchDeletes._
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @author VenkateswaraCh
 */

object DeleteFromHbaseTable {

  val logger = Logger.getLogger(getClass.getName)

  def getSparkSession(master_url: String) = SparkSession.builder().master(master_url).getOrCreate()

  def main(args: Array[String]): Unit = {

    val jobConfigFile = args(0)
    val className = this.getClass.getSimpleName.split('$').head

    //Parse the input configuration file, fetch and initialize the all the variables
    val confParser = ConfigFactory.parseFile(new File(getClass.getResource(jobConfigFile).getPath))
    val Array(hbaseTable, master_url, rowKeysPath) = Array(confParser.getString(className + ".hbaseTable")
      , confParser.getString(className + ".master_url"), confParser.getString(className + ".rowKeysPath"))

    // Initialize the spark Session
    val spark = getSparkSession(master_url)
    spark.conf.set("spark.yarn.access.hadoopFileSystems", className + ".hadoopFileSystems")

    /**
     * Delete from Hbase using rowKeysPath input
     */
    val deleteRowKeys = spark.sparkContext.textFile(rowKeysPath)
    hbaseBatchDelete(deleteRowKeys, hbaseTable, spark.sparkContext)

  }
}
