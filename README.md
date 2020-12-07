Welcome to the SparkWork!

# Overview
 
This Project to summarize and publish all the work I have done on HBASE using Spark as ETL. This page is broadly divided into below sections.

## 1. DataSet 

I used a sample retail sales dataset from Kaggle to implement this project, Please find the reference link below.
https://www.kaggle.com/mashlyn/online-retail-ii-uci

## 2. Read From HBASE 

Reading from HBASE table can be performed in three ways Scan, ScanTimeRange, HBASE client Get API. In this project I have implemented HBASE full scan, HBASE time range scan, and using HBASE Client Get API.

## 3. Read From HBASE Snapshot 
 
HBASE Snapshot provide a best way to back up and recover a HBASE table, snapshots can also be used to perform a point in time scans seamlessly offline without putting pressure on the HBASE cluster, HBASE snapshot's are good until a major compaction occurs on the table, once the major compaction occurs, WAL is reconstructed so, we may not be able to get the most out of this feature.
Major benefit of this approach is taking pressure off the HBASE cluster and using resources from regular ETL pool to scan the point in time WAL and restore the data onto HDFS (can be storage of your choice)

## 4. Write To HBASE 

Writing to HBASE can be performed using Batch Put API provided by HBASE client or using spark's saveAsNewAPIHadoopDataset API which uses Hadoop TableOutputFormat. These writes go through the HBASE region server, making an entry on the WAL.

## 5. Create HBASE HFiles and bulkLoading 

This approach of writing to HBASE is performed bypassing the region server, using the HBASE bulk loader client. To make the most of this approach you can use it as a one time load operation by creating pre-splits and custom partitioning the data on Spark, this way when you perform the bulkload, all the HFiles will straight away move to RegionServer. In case your input data is not well organized then the bulk load operation will result in performing Put operation using the HBASE client API, if you encounter this, it means you need to pre-split or group your HFiles according to the region boundaries.

## 6. Delete From HBASE 

Deleting from HBASE is straight forward, use the HBASE client API and the rowKey to generate the Delete object and execute it on the table, after the delete is complete WAL marks a tombstone for the rowKey


