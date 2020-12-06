package mywork.hbase.batch

import java.io.File

import com.typesafe.config.ConfigFactory
import mywork.hbase.common.HbaseBatchWrites.createHfiles
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
 * @author VenkateswaraCh
 */

object CreateHfilesBulkLoad {

  val logger = Logger.getLogger(getClass.getName)

  def getSparkSession(master_url: String) = SparkSession.builder().master(master_url).getOrCreate()

  def main(args: Array[String]): Unit = {

    val jobConfigFile = args(0)
    val className = this.getClass.getSimpleName.split('$').head

    //Parse the input configuration file, fetch and initialize the all the variables

    val confParser = ConfigFactory.parseFile(new File(getClass.getResource(jobConfigFile).getPath))
    val Array(inputDir, hfilesDir, master_url) = Array(
      confParser.getString(className + ".inputDir"),confParser.getString(className + ".hfilesDir"),confParser.getString(className + ".master_url")
    )
    val Array(hbaseTable, rowKeyName, numRegions, colFamily) = Array(confParser.getString(className + ".hbaseTable"), confParser.getString(className + ".rowKeyName"),
      confParser.getString(className + ".numRegions"), confParser.getString(className + ".colFamily"))

    val salesFields = confParser.getString(className + ".salesFields").split(",").toList

   // Initialize the spark Session
    val spark = getSparkSession(master_url)
    spark.conf.set("spark.yarn.access.hadoopFileSystems", className + ".hadoopFileSystems")

    // Read the data from HDFS and trigger the createHfiles function
    val inputDF = spark.read.parquet(inputDir);
    createHfiles(inputDF, spark, hbaseTable, salesFields, rowKeyName, numRegions.toInt, colFamily, hfilesDir)
  }
}
