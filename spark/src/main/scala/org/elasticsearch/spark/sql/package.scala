package org.elasticsearch.spark;

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame


package object sql {

  implicit def sqlContextFunctions(sc: SQLContext)= new SQLContextFunctions(sc)

  class SQLContextFunctions(sc: SQLContext) extends Serializable {
    def esRDD() = EsSparkSQL.esRDD(sc)
    def esRDD(resource: String) = EsSparkSQL.esRDD(sc, resource)
    def esRDD(resource: String, query: String) = EsSparkSQL.esRDD(sc, resource, query)
    def esRDD(cfg: scala.collection.Map[String, String]) = EsSparkSQL.esRDD(sc, cfg)
  }
  
  implicit def sparkSchemaRDDFunctions(rdd: DataFrame) = new SparkSchemaRDDFunctions(rdd)

  class SparkSchemaRDDFunctions(rdd: DataFrame) extends Serializable {
    def saveToEs(resource: String) { EsSparkSQL.saveToEs(rdd, resource) }
    def saveToEs(resource: String, cfg: scala.collection.Map[String, String]) { EsSparkSQL.saveToEs(rdd, resource, cfg) }
    def saveToEs(cfg: scala.collection.Map[String, String]) { EsSparkSQL.saveToEs(rdd, cfg)    }
  }
}