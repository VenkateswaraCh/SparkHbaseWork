package mywork.hbase.batch

/**
 * A Partitioner implementation that will separate records to different HBase Regions based on region boundaries
 * @param numRegions total number of regions on the hbase table
 */

class BulkLoadPartitioner(numRegions: Int)
  extends org.apache.spark.Partitioner {

  override def numPartitions: Int = numRegions

  override def getPartition(key: Any): Int = {

    val rowkeyStr = key.asInstanceOf[String]
    var partitionNum = 0
    var rowkey = 0;
    val pattern = "[^0-9]".r
    if (!pattern.findAllMatchIn(rowkeyStr).isEmpty) partitionNum = 10;
    else {
      rowkey = rowkeyStr.toInt
      if (rowkey < 498580) partitionNum = 0;
      else if (rowkey >= 498580 && rowkey < 507666) {
        partitionNum = 1
      }
      else if (rowkey >= 507666 && rowkey < 516719) {
        partitionNum = 2
      }
      else if (rowkey >= 516719 && rowkey < 525728) {
        partitionNum = 3
      }
      else if (rowkey >= 525728 && rowkey < 534908) {
        partitionNum = 4
      }
      else if (rowkey >= 534908 && rowkey < 544913) {
        partitionNum = 5
      }
      else if (rowkey >= 544913 && rowkey < 553856) {
        partitionNum = 6
      }
      else if (rowkey >= 553856 && rowkey < 563095) {
        partitionNum = 7
      }
      else if (rowkey >= 563095 && rowkey < 572483) {
        partitionNum = 8
      }
      else if (rowkey >= 572483 && rowkey < 581587) {
        partitionNum = 9
      }
    }
    partitionNum
  }
}