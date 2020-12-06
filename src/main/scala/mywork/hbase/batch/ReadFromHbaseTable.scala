package mywork.hbase.batch

import java.io.File

import com.typesafe.config.ConfigFactory
import mywork.hbase.common.HbaseBatchReads._
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @author VenkateswaraCh
 */

object ReadFromHbaseTable {

  val logger = Logger.getLogger(getClass.getName)

  def getSparkSession(master_url: String) = SparkSession.builder().master(master_url).getOrCreate()

  def main(args: Array[String]): Unit = {

    val jobConfigFile = args(0)
    val className = this.getClass.getSimpleName.split('$').head

    //Parse the input configuration file, fetch and initialize the all the variables
    val confParser = ConfigFactory.parseFile(new File(getClass.getResource(jobConfigFile).getPath))

    val Array(hbaseTable, colFamily, master_url, inputCols, hbaseScanOutputDir,hbaseScanTimeRangeOutputDir,hbaseGetOutputDir,rowKeysPath) = Array(confParser.getString(className + ".hbaseTable"), confParser.getString(className + ".colFamily"),
      confParser.getString(className + ".master_url"), confParser.getString(className + ".inputCols"), confParser.getString(className + ".hbaseScanOutputDir"),
      confParser.getString(className + ".hbaseScanTimeRangeOutputDir"),confParser.getString(className + ".hbaseGetOutputDir"),confParser.getString(className + ".rowKeysPath") )

    // Initialize the spark Session
    val spark = getSparkSession(master_url)
    spark.conf.set("spark.yarn.access.hadoopFileSystems", className + ".hadoopFileSystems")
    val maxVersions = confParser.getInt(className + ".maxVersions")

    /**
     * Scan from the hbase and persist on to HDFS as Parquet
     */
    val scannedRows: RDD[Row] = scanFromHbase(spark, hbaseTable, colFamily, maxVersions, inputCols)
    saveDataAsParquet(spark, scannedRows, inputCols, hbaseScanOutputDir)

    /**
     * Scan TimeRange (epoch time in milliseconds) from the hbase and persist on to HDFS as Parquet
     */

    val scanRangeStart = args(1).toLong
    val scanRangeEnd = args(2).toLong

    val scannedTimeRangeRows: RDD[Row] = scanFromHbaseTimeRange(spark, hbaseTable, colFamily, maxVersions, inputCols, scanRangeStart, scanRangeEnd)
    saveDataAsParquet(spark, scannedTimeRangeRows, inputCols, hbaseScanTimeRangeOutputDir)

    /**
     * HbaseBatchGet from the hbase and persist on to HDFS as Parquet
     */
    val hbaseRowKeys = spark.sparkContext.textFile(rowKeysPath)
    val hbaseGetRows = hbaseBatchGet(spark.sparkContext,hbaseRowKeys,hbaseTable,colFamily,inputCols)
    saveDataAsParquet(spark, hbaseGetRows, inputCols, hbaseGetOutputDir)
  }
}
