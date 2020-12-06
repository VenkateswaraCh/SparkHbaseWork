package mywork.hbase.common
import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

trait HbaseEnviroment {
  private final val envConfigFile = "/conf/HbaseEnv.conf"

  val log = Logger.getLogger(getClass.getName)

  val maxReqSecond = ConfigFactory.parseFile(new File(getClass.getResource(envConfigFile).getPath)).getInt("hbase.maxReqSecond")

  /**
   * Function to parse the input configuration file and return the base hbase config object
   * @return return the base hbase configuration object
   */
  def getHbaseBaseConf(): Configuration ={

    val conf = ConfigFactory.parseFile(new File(getClass.getResource(envConfigFile).getPath))

    val hbaseconf = HBaseConfiguration.create()
    hbaseconf.set("hbase.zookeeper.quorum", conf.getString("hbase.quorum"))
    hbaseconf.set("hbase.zookeeper.property.clientPort", conf.getString("hbase.clientPort"))
    hbaseconf.set("hbase.rootdir", conf.getString("hbase.rootdir"))
    hbaseconf.set("fs.defaultFS", conf.getString("hadoop.fs.defaultFS"))
    hbaseconf.set("dfs.nameservices", conf.getString("hadoop.dfs.nameservices"))
    hbaseconf
  }

  /**
   * Function to parse the input configuration file and return the hbase Spanpshotconfig object
   * @param spark handle to the spark session
   * @return return the hbase configuration object required for scanning hbase snapshots
   */
  def getHbaseSnapshotConf(spark:SparkSession): Configuration ={

    val conf = ConfigFactory.parseFile(new File(getClass.getResource(envConfigFile).getPath))

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.defaultFS", conf.getString("hadoop.fs.defaultFS"))
    hadoopConf.set("dfs.nameservices", conf.getString("hadoop.dfs.nameservices"))

    val hbaseconf = HBaseConfiguration.create(hadoopConf)
    hbaseconf.set("hbase.zookeeper.quorum", conf.getString("hbase.quorum"))
    hbaseconf.set("hbase.zookeeper.property.clientPort", conf.getString("hbase.clientPort"))
    hbaseconf.set("hbase.rootdir", conf.getString("hbase.rootdir"))

    hbaseconf
  }

  /**
   * Function to parse the input configuration file and return the  hbase WriteConfiguration object
   * @param hbaseTable Hbase table on which write operation is being performed
   * @return return the hbase configuration object required for Writing to hbase
   */
  def hbaseWriteConfig(hbaseTable: String): Configuration = {
    val hconf = getHbaseBaseConf();
    log.info("[ ** ] Initializing the HBase configuration Object [ ** ]")
    hconf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTable)
    hconf.set("mapreduce.job.output.key.class", classOf[Text].getName)
    hconf.set("mapreduce.job.output.value.class", classOf[LongWritable].getName)
    hconf.set("mapreduce.outputformat.class", classOf[TableOutputFormat[Text]].getName)
    hconf
  }

  /**
   * Function to parse the input configuration file and return the  hbase ReadConfiguration object
   * @param hbaseTable Hbase table on which read operation is being performed
   * @return return the hbase configuration object required for Reading to hbase
   */
  def hbaseReadConfig(hbaseTable: String): Configuration = {
    val hconf = getHbaseBaseConf();
    log.info("[ ** ] Initializing the HBase configuration Object [ ** ]")
    hconf.set("mapreduce.inputformat.class", "org.apache.hadoop.hbase.mapreduce.TableInputFormat")
    hconf.set("hbase.mapreduce.inputtable", hbaseTable)
    hconf.set(TableInputFormat.INPUT_TABLE, hbaseTable)
    hconf
  }
}
