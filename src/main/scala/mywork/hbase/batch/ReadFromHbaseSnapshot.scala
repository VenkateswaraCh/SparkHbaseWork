package mywork.hbase.batch

import java.io.File

import com.typesafe.config.ConfigFactory
import mywork.hbase.common.HbaseBatchReads.{saveDataAsParquet, scanFromHbaseSnapshot}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @author VenkateswaraCh
 */

object ReadFromHbaseSnapshot {

  val logger = Logger.getLogger(getClass.getName)

  def getSparkSession(master_url: String) = SparkSession.builder().master(master_url).getOrCreate()

  def main(args: Array[String]): Unit = {
    val jobConfigFile = args(0)

    val className = this.getClass.getSimpleName.split('$').head

    //Parse the input configuration file, fetch and initialize the all the variables
    val confParser = ConfigFactory.parseFile(new File(getClass.getResource(jobConfigFile).getPath))

    val Array(hbaseSnapshot, colFamily, master_url, inputCols, saveDir, snapshotRestoreLocation) = Array(confParser.getString(className + ".hbaseSnapshot"), confParser.getString(className + ".colFamily")
      , confParser.getString(className + ".master_url"), confParser.getString(className + ".inputCols")
      , confParser.getString(className + ".saveDir"), confParser.getString(className + ".snapshotRestoreLocation"))

    // Initialize the spark Session
    val spark = getSparkSession(master_url)
    spark.conf.set("spark.yarn.access.hadoopFileSystems", className + ".hadoopFileSystems")
    val maxVersions = confParser.getInt(className + ".maxVersions")

    /**
     * Read from Hbase Snapshot,restore it and save the restored data to HDFS
     */
    val transformedRows: RDD[Row] = scanFromHbaseSnapshot(spark, hbaseSnapshot, colFamily, snapshotRestoreLocation, maxVersions, inputCols)
    saveDataAsParquet(spark, transformedRows: RDD[Row], inputCols, saveDir)
  }

}
