package com.redislabs.provider.redis.sql

import com.redislabs.provider.redis._
import com.redislabs.provider.redis.rdd.{Keys, RedisKeysRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StringType, StructType, StructField}
import redis.clients.jedis.Protocol

case class RedisRelation(parameters: Map[String, String], userSchema: StructType)
                        (@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan with InsertableRelation with Keys {

  val tableName: String = parameters.getOrElse("table", "PANIC")

  val redisConfig: RedisConfig = {
    new RedisConfig({
        if ((parameters.keySet & Set("host", "port", "auth", "dbNum", "timeout")).size == 0) {
          new RedisEndpoint(sqlContext.sparkContext.getConf)
        } else {
          val host = parameters.getOrElse("host", Protocol.DEFAULT_HOST)
          val port = parameters.getOrElse("port", Protocol.DEFAULT_PORT.toString).toInt
          val auth = parameters.getOrElse("auth", null)
          val dbNum = parameters.getOrElse("dbNum", Protocol.DEFAULT_DATABASE.toString).toInt
          val timeout = parameters.getOrElse("timeout", Protocol.DEFAULT_TIMEOUT.toString).toInt
          new RedisEndpoint(host, port, auth, dbNum, timeout)
        }
      }
    )
  }

  val partitionNum: Int = parameters.getOrElse("partitionNum", 3.toString).toInt

  override def schema: StructType = {
    userSchema
  }

  def insert(data: DataFrame, overwrite: Boolean): Unit = {
  }

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
//    val filtersByAttr = filters.map(f => (getAttr(f), f)).groupBy(_._1).mapValues(a => a.map(p => p._2))
    new RedisKeysRDD(sqlContext.sparkContext, redisConfig, tableName + ":*", partitionNum, null).
      mapPartitions {
        partition: Iterator[String] => {
          groupKeysByNode(redisConfig.hosts, partition).flatMap {
            x => {
              val conn = x._1.endpoint.connect()
              val rowKeys: Array[String] = filterKeysByType(conn, x._2, "hash")
              rowKeys.foreach(println)
              val res = rowKeys.map {
                key => {
                  conn.hmget(key, requiredColumns: _*).toArray
                }
              }
              conn.close
              res
            }
          }.toIterator.map(x => Row.fromSeq(x))
        }
      }
  }

  private def getAttr(f: Filter): String = {
    f match {
      case EqualTo(attribute, value) => attribute
      case GreaterThan(attribute, value) => attribute
      case GreaterThanOrEqual(attribute, value) => attribute
      case LessThan(attribute, value) => attribute
      case LessThanOrEqual(attribute, value) => attribute
      case In(attribute, values) => attribute
      case IsNull(attribute) => attribute
      case IsNotNull(attribute) => attribute
      case StringStartsWith(attribute, value) => attribute
      case StringEndsWith(attribute, value) => attribute
      case StringContains(attribute, value) => attribute
    }
  }
}

class DefaultSource extends RelationProvider with SchemaRelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    RedisRelation(parameters, null)(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    RedisRelation(parameters, schema)(sqlContext)
  }
}

