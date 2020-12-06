package mywork.hbase.batch

import java.io.File

import com.typesafe.config.ConfigFactory
import mywork.hbase.common.HbaseBatchWrites.{convertToHbasePut, hbaseBatchPut}
import mywork.hbase.common.HbaseEnviroment
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_set, struct, to_json}

/**
 * extending HbaseEnviroment trait to get the HbaseWriteConfig and implement saveAsNewHadoopAPI()
 */

object WriteToHbaseTable extends HbaseEnviroment {

  val logger = Logger.getLogger(getClass.getName)

  def getSparkSession(master_url: String) = SparkSession.builder().master(master_url).getOrCreate()

  case class SalesOrders(Invoice: String, orders: String)

  def main(args: Array[String]): Unit = {

    val jobConfigFile = args(0)
    val className = this.getClass.getSimpleName.split('$').head

    //Parse the input configuration file, fetch and initialize the all the variables
    val confParser = ConfigFactory.parseFile(new File(getClass.getResource(jobConfigFile).getPath))

    val Array(hbaseTable, colFamily, master_url, sourcePath) = Array(confParser.getString(className + ".hbaseTable"), confParser.getString(className + ".colFamily")
      , confParser.getString(className + ".master_url"), confParser.getString(className + ".sourcePath"))

    // Initialize the spark Session
    val spark = getSparkSession(master_url)
    spark.conf.set("spark.yarn.access.hadoopFileSystems", className + ".hadoopFileSystems")
    import spark.implicits._

    // Source the sales data, parse and create a struct of all the line items
    val salesDF = spark.read.option("header", "true").csv(sourcePath)
      .withColumnRenamed("Customer ID", "Customer_ID")
      .withColumn("Order", struct(col("StockCode"), col("Description"),
        col("Quantity"), col("InvoiceDate"),
        col("Price"), col("Customer_ID"),
        col("Country")
      )
      )
    // group the sales data,  and create an array struct of all the line items within the order
    val salesPayload = salesDF.groupBy("Invoice")
      .agg(collect_set("Order").alias("Orders"))
      .withColumn("orders", to_json(col("Orders")))
      .drop("Order")
      .as[SalesOrders]

    /**
     * Write to Base using HbaseBatch Put API (One of the approaches)
     */
    val hbaseRdd: RDD[Put] = salesPayload.rdd.map(input => convertToHbasePut(input, input.Invoice, colFamily)).map(_._2)
    hbaseBatchPut(hbaseRdd, hbaseTable, spark.sparkContext)

    /**
     * Write to Base using saveAsNewAPIHadoopDataset Spark function (other approach),
     * extending Hbase environment trait to get the HbaseWriteConfig
     */
    val job = Job.getInstance(hbaseWriteConfig(hbaseTable))

    val hbaseSaveAsRdd: RDD[(ImmutableBytesWritable, Put)] = salesPayload.rdd.map(input => convertToHbasePut(input, input.Invoice, colFamily))
    hbaseSaveAsRdd.saveAsNewAPIHadoopDataset(job.getConfiguration)


  }
}
