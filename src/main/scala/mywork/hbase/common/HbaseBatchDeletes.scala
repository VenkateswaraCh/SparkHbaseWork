package mywork.hbase.common

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * Common Utilities to configure and perform HbaseDelete operations on Hbase
 *
 * @author VenkateswaraCh
 */

object HbaseBatchDeletes extends HbaseEnviroment {

  def hbaseMaxWritesSec: Int = 2000 // UpperLimit on write transactions per second

  def getNumWorkers(sc: SparkContext): Int = sc.getExecutorMemoryStatus.size

  /**
   * Iterator to Iterator transformation with a listBuffer size equal to batchSize
   */
  private def hbaseBatchDelete(records: Iterator[String], hbaseTable: String, batchSize: Int): Unit = {
    require(maxReqSecond.toInt <= hbaseMaxWritesSec) // Validating against threshold set forth by platform
    val conf = hbaseWriteConfig(hbaseTable)
    val conn = ConnectionFactory.createConnection(conf)
    val table = conn.getTable(TableName.valueOf(Bytes.toBytes(hbaseTable)))
    val deleteList: ListBuffer[Delete] = new ListBuffer[Delete]();
    var Array(recCount, totalCount, batchCount) = Array(0, 0, 0)
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
    log.info(s"[ ** ] hbaseBatchPut API Execution Complete")
  }


  def hbaseBatchDelete(hbaseRDD: RDD[String], hbaseTable: String, sc: SparkContext): Unit = {
    require(maxReqSecond.toInt <= hbaseMaxWritesSec) // Validating against threshold set forth by platform
    hbaseRDD.count() > 0 match {
      case true => {
        val numWorkers = getNumWorkers(sc)
        val batchSize:Int = maxReqSecond/numWorkers;
        hbaseRDD.foreachPartition(rec => hbaseBatchDelete(rec, hbaseTable, batchSize))
      }
      case false => {
        log.error(s"[**] ERROR Input hbase RDD Count equals Zero [**]")
        sys.exit(1)
      }
    }
  }

}