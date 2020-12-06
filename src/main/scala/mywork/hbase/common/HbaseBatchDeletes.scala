package mywork.hbase.common

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * Common Utilities to configure and perform Delete operations on Hbase
 * @author VenkateswaraCh
 */

object HbaseBatchDeletes extends HbaseEnviroment {

  final val hbaseMaxWritesSec: Int = 2000 // UpperLimit on write transactions per second

  /**
   *
   * @param sc handle to the sparkContext
   * @return return the number of workers assigned for this application
   */
  def getNumWorkers(sc: SparkContext): Int = sc.getExecutorMemoryStatus.size-1

  /**
   * HbaseBatchDelete function use batch delete API to perform the deletes in a batch, This is Iterator to Iterator transformation with a temporary placeholder - listBuffer of size equal to batchSize
   * @param records  Iterator of String, containing all the rowKeys
   * @param hbaseTable Hbase table on which the get operation is being performed
   * @param batchSize Size of batch being executed on a per task level, this is calculated based on the available CPUS and number of workers
   */
  private def hbaseBatchDelete(records: Iterator[String], hbaseTable: String, batchSize: Int): Unit = {
    require(maxReqSecond.toInt <= hbaseMaxWritesSec) // Validating against the UpperLimit
    //Get the HbaseWriteConfiguration
    val conf = hbaseWriteConfig(hbaseTable)
    //Create the connection using the configuration and get handle to the table
    val conn = ConnectionFactory.createConnection(conf)
    val table = conn.getTable(TableName.valueOf(Bytes.toBytes(hbaseTable)))
    val deleteList: ListBuffer[Delete] = new ListBuffer[Delete]();
    var Array(recCount, totalCount, batchCount) = Array(0, 0, 0)

    /** Iterate through the input Iterator, copy the Delete object to a temporary placeholder,
    *   clear the placeholder after executing each batch
    */
    while (records.hasNext) {
      deleteList += new Delete(Bytes.toBytes(records.next()));
      recCount = recCount + 1;
      if (recCount == batchSize) {
        table.delete(deleteList.asJava)
        deleteList.clear()
        totalCount = totalCount + recCount
        batchCount = batchCount + 1
        recCount = 0
        log.info(s"[ ** ] Executing Thread.sleep(500) [ ** ] ")
        Thread.sleep(500)
      }
      else if (!records.hasNext && recCount <= batchSize) {
        table.delete(deleteList.asJava)
        deleteList.clear()
        totalCount = totalCount + recCount
        batchCount = batchCount + 1
        recCount = 0
        conn.close()
      }

    }
    log.info(s"[ ** ] Input Records Size : ${totalCount} [ ** ] ")
    log.info(s"[ ** ] Total Batches Executed : ${batchCount} [ ** ] ")
    log.info(s"[ ** ] hbaseBatchDelete API Execution Complete [ ** ]")
  }


  /**+
   * hbaseBatchDelete function takes rowKeys as input and executes hbasebatchDelete Iterator function foreach partition
   * @param rowkeysRDD RDD with the rowkeys identified for deletion
   * @param hbaseTable Name of the hbase table where deletion is being performed
   * @param sc SparkContext handle
   */
  def hbaseBatchDelete(rowkeysRDD: RDD[String], hbaseTable: String, sc: SparkContext): Unit = {
    require(maxReqSecond.toInt <= hbaseMaxWritesSec) // Validating against the UpperLimit
    rowkeysRDD.count() > 0 match {
      case true => {
        val numWorkers = getNumWorkers(sc)
        val batchSize:Int = maxReqSecond/numWorkers;
        rowkeysRDD.foreachPartition(rec => hbaseBatchDelete(rec, hbaseTable, batchSize))
      }
      case false => {
        log.error(s"[ ** ] ERROR Input rowkeysRDD RDD Count equals Zero [ ** ]")
        sys.exit(1)
      }
    }
  }

}