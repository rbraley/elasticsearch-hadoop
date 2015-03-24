package org.elasticsearch.spark.sql.api.java

import java.util.{Map => JMap}
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.{Map => SMap}
import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_QUERY
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_READ
import org.elasticsearch.spark.sql.EsSparkSQL
import org.elasticsearch.spark.sql.JavaEsRowRDD
import org.elasticsearch.spark.sql.MappingUtils

object JavaEsSparkSQL {

  // specify the return types to make sure the bytecode is generated properly (w/o any scala.collections in it)
  def esRDD(jsc: SQLContext): DataFrame = esRDD(jsc, SMap.empty[String, String])
  def esRDD(jsc: SQLContext, resource: String): DataFrame = esRDD(jsc, Map(ES_RESOURCE_READ -> resource))
  def esRDD(jsc: SQLContext, resource: String, query: String): DataFrame = esRDD(jsc, Map(ES_RESOURCE_READ -> resource, ES_QUERY -> query))
  def esRDD(jsc: SQLContext, cfg: JMap[String, String]): DataFrame = esRDD(jsc, cfg.asScala)
  
  private def esRDD(jsc: SQLContext, cfg: SMap[String, String]): DataFrame = {
    val rowRDD = new JavaEsRowRDD(jsc.sparkContext, cfg)
    val schema = SQLUtils.asJavaDataType(MappingUtils.discoverMapping(rowRDD.esCfg)).asInstanceOf[StructType]
    jsc.createDataFrame(rowRDD, schema)
  }

  def saveToEs(jrdd: DataFrame, resource: String) = EsSparkSQL.saveToEs(SQLUtils.baseSchemaRDD(jrdd) , resource)
  def saveToEs(jrdd: DataFrame, resource: String, cfg: JMap[String, String]) = EsSparkSQL.saveToEs(SQLUtils.baseSchemaRDD(jrdd), resource, cfg.asScala)
  def saveToEs(jrdd: DataFrame, cfg: JMap[String, String]) = EsSparkSQL.saveToEs(SQLUtils.baseSchemaRDD(jrdd), cfg.asScala)
}