package mywork.hbase.common

import mywork.hbase.batch.BulkLoadPartitioner
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * Common Utilities to configure and perform Write operations on Hbase
 * @author VenkateswaraCh
 */

object HbaseBatchWrites extends HbaseEnviroment {

  final val hbaseMaxWritesSec: Int = 2000 // UpperLimit on transactions per second

  /**
   *
   * @param sc handle to the sparkContext
   * @return return the number of workers assigned for this application
   */
  def getNumWorkers(sc: SparkContext): Int = sc.getExecutorMemoryStatus.size-1

  /**
   * function to list all the fields and values of the Case class object
   * @param record
   * @tparam T
   * @return
   */
  def listFields[T](record: T): List[(String, String)] = {
    val fields = (List[(String, String)]() /: record.getClass.getDeclaredFields) { (a, f) =>
      f.setAccessible(true)
      a :+ ((f.getName, f.get(record).asInstanceOf[String]))
    }
    fields
  }

  /**
   *function to convert the input case class object into Hbase Put record
   * @param record input Case class object
   * @param rowKey name of the rowKey field
   * @param colFamily Name of Column family into which the row is being inserted into.
   * @tparam T
   * @return
   */
  def convertToHbasePut[T](record: T, rowKey: String, colFamily: String) = {
    val put = new Put(Bytes.toBytes(rowKey))
    listFields(record).
      map(rec => put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(rec._1), Bytes.toBytes(rec._2)))
    (new ImmutableBytesWritable(Bytes.toBytes(rowKey)), put)
  }

  /**
   * Function to perform Hbase Put using Hbase Client API, this is a Iterator to Iterator with a listBuffer size equal to batchSize
   * @param records  Iterator of String, containing all the rowKeys
   * @param hbaseTable Hbase table on which the get operation is being performed
   * @param batchSize Size of batch being executed on a per task level, this is calculated based on the available CPUS and number of workers to the application
   */
  private def hbaseBatchPut(records: Iterator[Put], hbaseTable: String, batchSize: Int): Unit = {
    require(maxReqSecond.toInt <= hbaseMaxWritesSec) // Validating against upper limit
    //Get the HbaseWriteConfiguration
    val conf = hbaseWriteConfig(hbaseTable)

    //Create the connection using the configuration and get handle to the table
    val conn = ConnectionFactory.createConnection(conf)
    val table = conn.getTable(TableName.valueOf(Bytes.toBytes(hbaseTable)))
    val putList: ListBuffer[Put] = new ListBuffer[Put]();
    var Array(recCount, totalCount, batchCount) = Array(0, 0, 0)

    /** Iterate through the input Iterator, copy the Delete object to a temporary placeholder, clear the placeholder after executing each batch
     */
    while (records.hasNext) {
      putList += records.next();
      recCount = recCount + 1;
      if (recCount == batchSize) {
        table.put(putList.asJava)
        putList.clear()
        totalCount = totalCount + recCount
        batchCount = batchCount + 1
        recCount = 0
        log.info(s"[ ** ] Executing Thread.sleep(500) [ ** ] ")
        Thread.sleep(500)
      }
      else if (!records.hasNext && recCount <= batchSize) {
        table.put(putList.asJava)
        putList.clear()
        totalCount = totalCount + recCount
        batchCount = batchCount + 1
        recCount = 0
        conn.close()
      }

    }
    log.info(s"[ ** ] Input Records Size : ${totalCount} [ ** ] ")
    log.info(s"[ ** ] Total Batches Executed : ${batchCount} [ ** ] ")
    log.info(s"[ ** ] hbaseBatchPut API Execution Complete")
  }

  /**
   *
   * @param hbaseRDD input RDD of hbase Put Objects
   * @param hbaseTable input hbase table on which the operation is being performed
   * @param sc handle to the spark context
   */
  def hbaseBatchPut(hbaseRDD: RDD[Put], hbaseTable: String, sc: SparkContext): Unit = {
    require(maxReqSecond.toInt <= hbaseMaxWritesSec) // Validating against upperLimit
    val numWorkers = getNumWorkers(sc)
    val batchSize: Int = maxReqSecond / numWorkers;
    hbaseRDD.foreachPartition(rec => hbaseBatchPut(rec, hbaseTable, batchSize))
  }

  /**
   *
   * @param inputDF input data frame
   * @param spark handle to spark session
   * @param hbaseTable input hbase table on which the operation is being performed
   * @param fieldsList list of the fields/columns
   * @param rowKeyName name of the rowKey
   * @param numRegions number of regions on the target hbase table
   * @param colFamily Name of Column family into which the row is being inserted into.
   * @param saveDir output directory on hdfs where the Hfiles are being saved into.
   */
  def createHfiles(inputDF: DataFrame,spark:SparkSession, hbaseTable: String, fieldsList: List[String], rowKeyName: String, numRegions: Int, colFamily: String, saveDir: String): Unit = {
    import spark.implicits._
    val hconf = getHbaseBaseConf()
    hconf.set(TableInputFormat.INPUT_TABLE, hbaseTable)
    hconf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    hconf.set("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
    val inputHbaseRDD = inputDF.map(r => {
      val rowKey = r.getAs[String](rowKeyName)
      (rowKey, r.getValuesMap[String](fieldsList))
    }).rdd

    // Iterate over input hbase rdd, flatten the resultant tuple to rowkey, col qualifier level, custom partition according to region boundaries
    val partitionedHbaseRDD = inputHbaseRDD.flatMap(inputRow => {
      val hbaseRows = new ListBuffer[(String, (String, String))]
      val rowKey = inputRow._1
      // Iterate Over the values map and filter out for null values, custom partition according to region boundaries
      inputRow._2.filter(_._2 != null)
        .map(rec => {
          val cell = (rowKey, (rec._1, rec._2))
          hbaseRows += cell
        })
      hbaseRows
    }).repartitionAndSortWithinPartitions(new BulkLoadPartitioner(numRegions))

    val hbaseRows = partitionedHbaseRDD.map(r => {
      val key = new ImmutableBytesWritable(Bytes.toBytes(r._1))
      // (key, (rowkey, col fam, col qualifier, col value))
      (key, new KeyValue(Bytes.toBytes(r._1), Bytes.toBytes(colFamily), Bytes.toBytes(r._2._1), Bytes.toBytes(r._2._2)))
    })


    val job = new Job(hconf, "create_hfiles")
    val conn = ConnectionFactory.createConnection(hconf)
    val table = conn.getTable(TableName.valueOf(Bytes.toBytes(hbaseTable)))
    val tableRegionLocator = conn.getRegionLocator(table.getName)
    HFileOutputFormat2.configureIncrementalLoad(job, table, tableRegionLocator);

    val conf = job.getConfiguration
    hbaseRows.saveAsNewAPIHadoopFile(saveDir, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], conf)
  }

}