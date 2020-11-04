#!/usr/bin/env python
# -*- coding:utf-8 -*-

""" 
:Description: 
:Owner: tao_chen
:Create time: 2020/10/28
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession

if __name__ == '__main__':
    sc = SparkContext()
    spark = SparkSession(sc)
    # 建表
    data = spark.range(0, 5)
    data.write.format("delta").save("/data_test/tao_chen/delta-table", mode='overwrite')

    # 查看该目录
    # hdfs dfs -ls -h /data_test/tao_chen/delta-table
    # drwxrwx---   - tao_chen intsig          0 2020-10-30 17:50 /data_test/tao_chen/delta-table/_delta_log
    # -rw-rw----   3 tao_chen intsig        429 2020-10-30 17:50 /data_test/tao_chen/delta-table/part-00000-fe74141f-5cee-4bb7-8dc8-349fe23521c8-c000.snappy.parquet
    # -rw-rw----   3 tao_chen intsig        437 2020-10-30 17:50 /data_test/tao_chen/delta-table/part-00001-d14d5bf3-0b32-40d4-8029-23073af0c4f9-c000.snappy.parquet
    # -rw-rw----   3 tao_chen intsig        437 2020-10-30 17:50 /data_test/tao_chen/delta-table/part-00002-4ea8bcf5-84cf-4713-b232-d8208f055853-c000.snappy.parquet

    # 元数据
    # {"commitInfo":{"timestamp":1604051415750,"operation":"WRITE","operationParameters":{"mode":"Overwrite","partitionBy":"[]"},"isBlindAppend":false,"operationMetrics":{"numFiles":"3","numOutputBytes":"1303","numOutputRows":"5"}}}
    # {"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
    # {"metaData":{"id":"12bc893b-1cae-4b42-962f-8427a80e660a","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1604051403848}}
    # {"add":{"path":"part-00000-fe74141f-5cee-4bb7-8dc8-349fe23521c8-c000.snappy.parquet","partitionValues":{},"size":429,"modificationTime":1604051404276,"dataChange":true}}
    # {"add":{"path":"part-00001-d14d5bf3-0b32-40d4-8029-23073af0c4f9-c000.snappy.parquet","partitionValues":{},"size":437,"modificationTime":1604051415122,"dataChange":true}}
    # {"add":{"path":"part-00002-4ea8bcf5-84cf-4713-b232-d8208f055853-c000.snappy.parquet","partitionValues":{},"size":437,"modificationTime":1604051404276,"dataChange":true}}

    # 读表
    df = spark.read.format("delta").load("/data_test/tao_chen/delta-table")
    df.show()

    # 更新表
    data = spark.range(5, 10)
    data.write.format("delta").mode("overwrite").save("/data_test/tao_chen/delta-table")

    # 此时该目录的文件
    # drwxrwx---   - tao_chen intsig          0 2020-10-30 18:00 /data_test/tao_chen/delta-table/_delta_log
    # -rw-rw----   3 tao_chen intsig        429 2020-10-30 18:00 /data_test/tao_chen/delta-table/part-00000-4027f7f0-bd30-4182-a0a2-31270ea42187-c000.snappy.parquet
    # -rw-rw----   3 tao_chen intsig        429 2020-10-30 17:50 /data_test/tao_chen/delta-table/part-00000-fe74141f-5cee-4bb7-8dc8-349fe23521c8-c000.snappy.parquet
    # -rw-rw----   3 tao_chen intsig        437 2020-10-30 18:00 /data_test/tao_chen/delta-table/part-00001-ca17e1c0-6baa-4b94-8bc9-88a526ccc41e-c000.snappy.parquet
    # -rw-rw----   3 tao_chen intsig        437 2020-10-30 17:50 /data_test/tao_chen/delta-table/part-00001-d14d5bf3-0b32-40d4-8029-23073af0c4f9-c000.snappy.parquet
    # -rw-rw----   3 tao_chen intsig        437 2020-10-30 18:00 /data_test/tao_chen/delta-table/part-00002-47b8cdd0-a4e0-499d-be82-3fd00a4e6066-c000.snappy.parquet
    # -rw-rw----   3 tao_chen intsig        437 2020-10-30 17:50 /data_test/tao_chen/delta-table/part-00002-4ea8bcf5-84cf-4713-b232-d8208f055853-c000.snappy.parquet

    # _delta_log此时的文件
    # -rw-rw----   3 tao_chen intsig       1089 2020-10-30 17:50 /data_test/tao_chen/delta-table/_delta_log/00000000000000000000.json
    # -rw-rw----   3 tao_chen intsig       1179 2020-10-30 18:00 /data_test/tao_chen/delta-table/_delta_log/00000000000000000001.json

    # {"commitInfo":{"timestamp":1604052028555,"operation":"WRITE","operationParameters":{"mode":"Overwrite","partitionBy":"[]"},"readVersion":0,"isBlindAppend":false,"operationMetrics":{"numFiles":"3","numOutputBytes":"1303","numOutputRows":"5"}}}
    # {"add":{"path":"part-00000-4027f7f0-bd30-4182-a0a2-31270ea42187-c000.snappy.parquet","partitionValues":{},"size":429,"modificationTime":1604052027250,"dataChange":true}}
    # {"add":{"path":"part-00001-ca17e1c0-6baa-4b94-8bc9-88a526ccc41e-c000.snappy.parquet","partitionValues":{},"size":437,"modificationTime":1604052027244,"dataChange":true}}
    # {"add":{"path":"part-00002-47b8cdd0-a4e0-499d-be82-3fd00a4e6066-c000.snappy.parquet","partitionValues":{},"size":437,"modificationTime":1604052028081,"dataChange":true}}
    # {"remove":{"path":"part-00000-fe74141f-5cee-4bb7-8dc8-349fe23521c8-c000.snappy.parquet","deletionTimestamp":1604052028554,"dataChange":true}}
    # {"remove":{"path":"part-00002-4ea8bcf5-84cf-4713-b232-d8208f055853-c000.snappy.parquet","deletionTimestamp":1604052028555,"dataChange":true}}
    # {"remove":{"path":"part-00001-d14d5bf3-0b32-40d4-8029-23073af0c4f9-c000.snappy.parquet","deletionTimestamp":1604052028555,"dataChange":true}}

    # 条件更新
    from delta_python import *
    from pyspark.sql.functions import *

    deltaTable = DeltaTable.forPath(spark, "/data_test/tao_chen/delta-table")
    deltaTable.toDF().show()

    # Update every even value by adding 100 to it
    deltaTable.update(
        condition = expr("id % 2 == 0"),
        set = { "id": expr("id + 100") })

    # Delete every even value
    deltaTable.delete(condition = expr("id % 2 == 0"))

    # 两次更新后_delta_log下的文件
    # -rw-rw----   3 tao_chen intsig       1089 2020-10-30 17:50 /data_test/tao_chen/delta-table/_delta_log/00000000000000000000.json
    # -rw-rw----   3 tao_chen intsig       1179 2020-10-30 18:00 /data_test/tao_chen/delta-table/_delta_log/00000000000000000001.json
    # -rw-rw----   3 tao_chen intsig        920 2020-10-30 18:08 /data_test/tao_chen/delta-table/_delta_log/00000000000000000002.json
    # -rw-rw----   3 tao_chen intsig        923 2020-10-30 18:08 /data_test/tao_chen/delta-table/_delta_log/00000000000000000003.json

    # hdfs dfs -cat /data_test/tao_chen/delta-table/_delta_log/00000000000000000002.json
    # {"commitInfo":{"timestamp":1604052509261,"operation":"UPDATE","operationParameters":{"predicate":"((id#980L % cast(2 as bigint)) = cast(0 as bigint))"},"readVersion":1,"isBlindAppend":false,"operationMetrics":{"numRemovedFiles":"2","numAddedFiles":"2","numUpdatedRows":"2","numCopiedRows":"2"}}}
    # {"remove":{"path":"part-00001-ca17e1c0-6baa-4b94-8bc9-88a526ccc41e-c000.snappy.parquet","deletionTimestamp":1604052509017,"dataChange":true}}
    # {"remove":{"path":"part-00002-47b8cdd0-a4e0-499d-be82-3fd00a4e6066-c000.snappy.parquet","deletionTimestamp":1604052509017,"dataChange":true}}
    # {"add":{"path":"part-00000-5a898092-a1f4-40df-a109-5f4b80a82f2a-c000.snappy.parquet","partitionValues":{},"size":437,"modificationTime":1604052509226,"dataChange":true}}
    # {"add":{"path":"part-00001-38a6130f-3ebb-4a68-b794-1641d1ae6d5a-c000.snappy.parquet","partitionValues":{},"size":437,"modificationTime":1604052509252,"dataChange":true}}

    # hdfs dfs -cat /data_test/tao_chen/delta-table/_delta_log/00000000000000000003.json
    # {"commitInfo":{"timestamp":1604052513038,"operation":"DELETE","operationParameters":{"predicate":"[\"((`id` % CAST(2 AS BIGINT)) = CAST(0 AS BIGINT))\"]"},"readVersion":2,"isBlindAppend":false,"operationMetrics":{"numRemovedFiles":"2","numDeletedRows":"2","numAddedFiles":"2","numCopiedRows":"2"}}}
    # {"remove":{"path":"part-00001-38a6130f-3ebb-4a68-b794-1641d1ae6d5a-c000.snappy.parquet","deletionTimestamp":1604052513036,"dataChange":true}}
    # {"remove":{"path":"part-00000-5a898092-a1f4-40df-a109-5f4b80a82f2a-c000.snappy.parquet","deletionTimestamp":1604052513036,"dataChange":true}}
    # {"add":{"path":"part-00000-4f4e01c3-41fb-4b7c-ab2c-9d09faa6f9b8-c000.snappy.parquet","partitionValues":{},"size":429,"modificationTime":1604052513030,"dataChange":true}}
    # {"add":{"path":"part-00001-4c31f9a1-cb69-4600-b223-39ceecd2f894-c000.snappy.parquet","partitionValues":{},"size":429,"modificationTime":1604052513032,"dataChange":true}}

    # In [15]: deltaTable.history().show(20, False)
    # +-------+-----------------------+------+--------+---------+-------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+-----------------------------------------------------------------------------------+
    # |version|timestamp              |userId|userName|operation|operationParameters                                                |job |notebook|clusterId|readVersion|isolationLevel|isBlindAppend|operationMetrics                                                                   |
    # +-------+-----------------------+------+--------+---------+-------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+-----------------------------------------------------------------------------------+
    # |3      |2020-10-30 18:08:33.056|null  |null    |DELETE   |[predicate -> ["((`id` % CAST(2 AS BIGINT)) = CAST(0 AS BIGINT))"]]|null|null    |null     |2          |null          |false        |[numRemovedFiles -> 2, numDeletedRows -> 2, numAddedFiles -> 2, numCopiedRows -> 2]|
    # |2      |2020-10-30 18:08:29.32 |null  |null    |UPDATE   |[predicate -> ((id#980L % cast(2 as bigint)) = cast(0 as bigint))] |null|null    |null     |1          |null          |false        |[numRemovedFiles -> 2, numAddedFiles -> 2, numUpdatedRows -> 2, numCopiedRows -> 2]|
    # |1      |2020-10-30 18:00:28.621|null  |null    |WRITE    |[mode -> Overwrite, partitionBy -> []]                             |null|null    |null     |0          |null          |false        |[numFiles -> 3, numOutputBytes -> 1303, numOutputRows -> 5]                        |
    # |0      |2020-10-30 17:50:15.894|null  |null    |WRITE    |[mode -> Overwrite, partitionBy -> []]                             |null|null    |null     |null       |null          |false        |[numFiles -> 3, numOutputBytes -> 1303, numOutputRows -> 5]                        |
    # +-------+-----------------------+------+--------+---------+-------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+-----------------------------------------------------------------------------------+


    # Upsert (merge) new data
    newData = spark.range(0, 20)

    deltaTable.alias("oldData") \
        .merge(
        newData.alias("newData"),
        "oldData.id = newData.id") \
        .whenMatchedUpdate(set = { "id": col("newData.id") }) \
        .whenNotMatchedInsert(values = { "id": col("newData.id") }) \
        .execute()

    deltaTable.toDF().show()
    deltaTable.history()
    # +-------+-----------------------+------+--------+---------+-------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    # |version|timestamp              |userId|userName|operation|operationParameters                                                |job |notebook|clusterId|readVersion|isolationLevel|isBlindAppend|operationMetrics                                                                                                                                                                                              |
    # +-------+-----------------------+------+--------+---------+-------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    # |4      |2020-10-30 18:16:53.587|null  |null    |MERGE    |[predicate -> (oldData.`id` = newData.`id`)]                       |null|null    |null     |3          |null          |false        |[numTargetRowsCopied -> 0, numTargetRowsDeleted -> 0, numTargetFilesAdded -> 21, numTargetRowsInserted -> 17, numTargetRowsUpdated -> 3, numOutputRows -> 20, numSourceRows -> 20, numTargetFilesRemoved -> 3]|
    # |3      |2020-10-30 18:08:33.056|null  |null    |DELETE   |[predicate -> ["((`id` % CAST(2 AS BIGINT)) = CAST(0 AS BIGINT))"]]|null|null    |null     |2          |null          |false        |[numRemovedFiles -> 2, numDeletedRows -> 2, numAddedFiles -> 2, numCopiedRows -> 2]                                                                                                                           |
    # |2      |2020-10-30 18:08:29.32 |null  |null    |UPDATE   |[predicate -> ((id#980L % cast(2 as bigint)) = cast(0 as bigint))] |null|null    |null     |1          |null          |false        |[numRemovedFiles -> 2, numAddedFiles -> 2, numUpdatedRows -> 2, numCopiedRows -> 2]                                                                                                                           |
    # |1      |2020-10-30 18:00:28.621|null  |null    |WRITE    |[mode -> Overwrite, partitionBy -> []]                             |null|null    |null     |0          |null          |false        |[numFiles -> 3, numOutputBytes -> 1303, numOutputRows -> 5]                                                                                                                                                   |
    # |0      |2020-10-30 17:50:15.894|null  |null    |WRITE    |[mode -> Overwrite, partitionBy -> []]                             |null|null    |null     |null       |null          |false        |[numFiles -> 3, numOutputBytes -> 1303, numOutputRows -> 5]                                                                                                                                                   |
    # +-------+-----------------------+------+--------+---------+-------------------------------------------------------------------+----+--------+---------+-----------+--------------+-------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

    # 时间旅行
    df = spark.read.format("delta").option("versionAsOf", 4).load("/data_test/tao_chen/delta-table")
    df.show()

    # Structured Streaming写入，即使其他批流作业也在操作这张表
    # Delta Lake 事务日志保证了exactly-once语义
    streamingDf = spark.readStream.format("rate").load()
    stream = streamingDf.selectExpr("value as id").writeStream.format("delta").option("checkpointLocation", "/data_test/tao_chen/delta-table/checkpoint").start("/data_test/tao_chen/delta-table")
    stream.stop()
