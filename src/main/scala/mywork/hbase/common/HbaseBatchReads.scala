package mywork.hbase.common

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableSnapshotInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * Common Utilities to configure and perform Read operations on Hbase
 * @author VenkateswaraCh
 */

object HbaseBatchReads extends HbaseEnviroment {

  final val hbaseMaxWritesSec: Int = 2000 // UpperLimit on transactions per second

  /**
   *
   * @param sc handle to the sparkContext
   * @return return the number of workers assigned for this application
   */
  def getNumWorkers(sc: SparkContext): Int = sc.getExecutorMemoryStatus.size-1

  /**
   * Scan TimeRange from the hbase and persist on to HDFS as Parquet
   * @param spark handle to the spark session
   * @param hbaseTable hbase table on which the scan is being performed
   * @param colFamily hbase column family, to parse the results returned
   * @param maxVersions maximum number of version we would like to retrieve
   * @param inputCols comma seperated inputcolumns to parse the Result returned by Hbase
   * @param scanRangeStart starttimerange in milliseconds
   * @param scanRangeEnd endtimerange in milliseconds
   * @return parsedHbase Result to RDD of Rows
   */
  def scanFromHbaseTimeRange(spark: SparkSession, hbaseTable: String, colFamily: String, maxVersions: Int, inputCols: String, scanRangeStart: Long, scanRangeEnd: Long): RDD[Row] = {
    // configuration specific to hbase reads/scans
    val hconf = hbaseReadConfig(hbaseTable)
    val scan = new Scan().setMaxVersions(maxVersions)
    scan.setTimeRange(scanRangeStart, scanRangeEnd)
    val scanToBinary = ProtobufUtil.toScan(scan).toByteArray
    hconf.set(TableInputFormat.SCAN, Base64.encodeBytes(scanToBinary))
    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(hconf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    parseHbaseRows(hbaseRDD, colFamily, inputCols)
  }

  /**+
   * Scan from the hbase and persist on to HDFS as Parquet
   * @param spark handle to the spark session
   * @param hbaseTable hbase table on which the scan is being performed
   * @param colFamily hbase column family, to parse the results returned
   * @param maxVersions maximum number of version we would like to retrieve
   * @param inputCols comma seperated inputcolumns to parse the Result returned by Hbase
   * @return parsedHbase Result to RDD of Rows
   */
  def scanFromHbase(spark: SparkSession, hbaseTable: String, colFamily: String, maxVersions: Int, inputCols: String): RDD[Row] = {
    // configuration specific to hbase reads/scans
    val hconf = hbaseReadConfig(hbaseTable)
    val scan = new Scan().setMaxVersions(maxVersions)
    val scanToBinary = ProtobufUtil.toScan(scan).toByteArray
    hconf.set(TableInputFormat.SCAN, Base64.encodeBytes(scanToBinary))
    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(hconf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    parseHbaseRows(hbaseRDD, colFamily, inputCols)
  }

  /**
   * Scan from the hbase snapshots and persist on to HDFS as Parquet
   * @param spark handle to the spark session
   * @param hbaseSnapshot hbase snapshot on which the scan is being performed
   * @param colFamily hbase column family, to parse the results returned
   * @param snapshotRestoreLocation Location on hdfs where the snapshot is restored by replaying the transaction log
   * @param maxVersions maximum number of version we would like to retrieve
   * @param inputCols comma seperated inputcolumns to parse the Result returned by Hbase
   * @return parsedHbase Result to RDD of Rows
   */
  def scanFromHbaseSnapshot(spark: SparkSession, hbaseSnapshot: String, colFamily: String, snapshotRestoreLocation: String, maxVersions: Int, inputCols: String): RDD[Row] = {
    val hconf = getHbaseSnapshotConf(spark)
    hconf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(new Scan().setMaxVersions(maxVersions)).toByteArray))

    val job = Job.getInstance(hconf)
    TableSnapshotInputFormat.setInput(job, hbaseSnapshot, new Path(snapshotRestoreLocation))
    val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(job.getConfiguration, classOf[TableSnapshotInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    parseHbaseRows(hbaseRDD, colFamily, inputCols)
  }

  /**
   * function to parse Result returned by Hbase API and convert to Spark SQL Row
   * @param hbaseRDD Result returned by Spark Hbase API
   * @param colFamily hbase column family, to parse the results returned
   * @param inputCols comma seperated inputcolumns to parse the Result returned by Hbase
   * @return parsed Hbase Result to Spark SQL Row
   */
  private def parseHbaseRows(hbaseRDD: RDD[(ImmutableBytesWritable, Result)], colFamily: String, inputCols: String): RDD[Row] = {

    val columnList: Array[String] = inputCols.split(",").map(_.trim)
    val columnListSize: Int = columnList.size
    val hbaseRows: RDD[org.apache.spark.sql.Row] = hbaseRDD.map(record => {
      val out = record._2

      var values = new ArrayBuffer[String]() // Placeholder to collect all the parsed cells of a Hbase Row
      //Fetching the RowKey and appending it
      values.append(Option(Bytes.toString(out.getRow)).getOrElse(""))

      //Iterate through the colList, barring Index 0, to parse the Result object  from Hbase
      columnList.slice(1, columnListSize)
        .foreach(rec => values.append(
          // Option to handle the null pointer exceptions
          Option(Bytes.toString(out.getValue(Bytes.toBytes(colFamily), Bytes.toBytes(rec)))).getOrElse("")
        ))
      org.apache.spark.sql.Row.fromSeq(values)
    })
    //Return the Parsed Hbase rows
    hbaseRows

  }

  /**
   * function to parse Result returned by Hbase API and convert to Spark SQL Row
   * @param hbaseResult RDD of Hbase Result returned by Hbase Client API
   * @param colFamily hbase column family, to parse the results returned
   * @param inputCols comma seperated inputcolumns to parse the Result returned by Hbase
   * @return
   */
  private def parseHbaseResult(hbaseResult: RDD[Result], colFamily: String, inputCols: String): RDD[Row] = {

    val columnList: Array[String] = inputCols.split(",").map(_.trim)
    val columnListSize: Int = columnList.size
    val hbaseRows: RDD[org.apache.spark.sql.Row] = hbaseResult.map(record => {
      val out = record
      var values = new ArrayBuffer[String]() // Placeholder to collect all the parsed cells of a Hbase Row
      //Fetching the RowKey and appending it
      values.append(Option(Bytes.toString(out.getRow)).getOrElse(""))

      //Iterate through the colList, barring Index 0, to parse the Result object  from Hbase
      columnList.slice(1, columnListSize)
        .foreach(rec => values.append(
          // Option to handle the null pointer exceptions
          Option(Bytes.toString(out.getValue(Bytes.toBytes(colFamily), Bytes.toBytes(rec)))).getOrElse("")
        ))
      org.apache.spark.sql.Row.fromSeq(values)
    })
    //Return the Parsed Hbase rows
    hbaseRows
  }


  /**
   * Function to perform Hbase get using Hbase Client API, this is a Iterator to Iterator with a listBuffer size equal to batchSize
   * @param records  Iterator of String, containing all the rowKeys
   * @param hbaseTable Hbase table on which the get operation is being performed
   * @param batchSize Size of batch being executed on a per task level, this is calculated based on the available CPUS and number of workers
   * @return Return the Result object returned by the Hbase client API
   */
  private def hbaseBatchGet(records: Iterator[String], hbaseTable: String, batchSize: Int): Iterator[Result] = {
    require(maxReqSecond.toInt <= hbaseMaxWritesSec) // Validating against upperlimit
    //Get the HbaseWriteConfiguration
    val conf = hbaseWriteConfig(hbaseTable)
    //Create the connection using the configuration and get handle to the table
    val conn = ConnectionFactory.createConnection(conf)
    val table = conn.getTable(TableName.valueOf(Bytes.toBytes(hbaseTable)))
    val getList: java.util.List[Get] = new java.util.ArrayList[Get]()
    var Array(recCount, totalCount, batchCount) = Array(0, 0, 0)
    var hbaseResult: Iterator[Result] = Iterator[Result]();

    /** Iterate through the input Iterator, copy the Delete object to a temporary placeholder, clear the placeholder after executing each batch
     * Append the result returned by hbase client the iterator
     */
    while (records.hasNext) {
      getList.add(new Get(Bytes.toBytes(records.next())));
      recCount = recCount + 1;
      if (recCount == batchSize) {
        val response: Iterator[Result] = table.get(getList).toIterator
        hbaseResult = hbaseResult.++(response)
        getList.clear()
        totalCount = totalCount + recCount
        batchCount = batchCount + 1
        recCount = 0
        log.info(s"[ ** ] Executing Thread.sleep(500) [ ** ] ")
        Thread.sleep(500)
      }
      else if (!records.hasNext && recCount <= batchSize) {
        val response: Iterator[Result] = table.get(getList).toIterator
        hbaseResult = hbaseResult.++(response)
        getList.clear()
        totalCount = totalCount + recCount
        batchCount = batchCount + 1
        recCount = 0
        conn.close()
      }

    }
    log.info(s"[ ** ] Input Records Size : ${totalCount} [ ** ] ")
    log.info(s"[ ** ] Total Batches Executed : ${batchCount} [ ** ] ")
    log.info(s"[ ** ] hbaseBatchGetAPI Execution Complete")

    hbaseResult // Return the result returned by the Hbase client API
  }

  /**
   *
   * @param sc SparkContext handle
   * @param rowkeysRDD RDD with the rowkeys identified for get operation
   * @param hbaseTable hbase table on which the scan is being performed
   * @param colFamily hbase column family, to parse the results returned
   * @param inputCols comma seperated inputcolumns to parse the Result returned by Hbase
   * @return
   */
  def hbaseBatchGet(sc: SparkContext, rowkeysRDD: RDD[String], hbaseTable: String, colFamily: String, inputCols: String): RDD[Row] = {
    require(maxReqSecond.toInt <= hbaseMaxWritesSec) // Validating against upperlimit
    val numWorkers = getNumWorkers(sc)
    //Size of batch being executed on a per task level, this is calculated based on the available CPUS and number of workers
    val batchSize: Int = maxReqSecond / numWorkers;
    val hbaseResult = rowkeysRDD.mapPartitions(rec => hbaseBatchGet(rec, hbaseTable, batchSize))
    parseHbaseResult(hbaseResult, colFamily, inputCols)
  }

  /**
   *
   * @param spark SparkSession Handle
   * @param parsedRows Parsed Hbase Rows
   * @param inputCols comma seperated inputcolumns to parse the Result returned by Hbase.
   * @param saveDir Output location on HDFS where data is being saved.
   */
  def saveDataAsParquet(spark: SparkSession, parsedRows: RDD[Row], inputCols: String, saveDir: String): Unit = {
    var dataSchema = new StructType()
    inputCols.split(",").foreach(colName => {
      dataSchema = dataSchema.add(colName.trim, StringType)
    })
    val sourceData = spark.createDataFrame(parsedRows, dataSchema)
    println("[ *** ] Writing HBASE snapshot to parquet in Progress")

    sourceData.write.mode("overwrite").save(saveDir)
    println("[ *** ] Writing to parquet complete")

  }
}