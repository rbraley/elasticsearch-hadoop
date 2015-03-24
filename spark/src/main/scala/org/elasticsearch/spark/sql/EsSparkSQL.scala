package org.elasticsearch.spark.sql

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.Map

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_QUERY
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_READ
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.ES_RESOURCE_WRITE
import org.elasticsearch.hadoop.cfg.PropertiesSettings
import org.elasticsearch.spark.cfg.SparkSettingsManager

object EsSparkSQL {

  def esRDD(sc: SQLContext): DataFrame = esRDD(sc, Map.empty[String, String])
  def esRDD(sc: SQLContext, resource: String): DataFrame = esRDD(sc, Map(ES_RESOURCE_READ -> resource))
  def esRDD(sc: SQLContext, resource: String, query: String): DataFrame = esRDD(sc, Map(ES_RESOURCE_READ -> resource, ES_QUERY -> query))
  def esRDD(sc: SQLContext, cfg: Map[String, String]): DataFrame = {
    val rowRDD = new ScalaEsRowRDD(sc.sparkContext, cfg)
    val schema = MappingUtils.discoverMapping(rowRDD.esCfg)
    sc.createDataFrame(rowRDD, schema)
  }

//  def esRDD(jsc: SQLContext): DataFrame = esRDD(jsc, Map.empty[String, String])
//  def esRDD(jsc: SQLContext, resource: String): DataFrame = esRDD(jsc, Map(ES_RESOURCE_READ -> resource))
//  def esRDD(jsc: SQLContext, resource: String, query: String): DataFrame = esRDD(jsc, Map(ES_RESOURCE_READ -> resource, ES_QUERY -> query))
//  def esRDD(jsc: SQLContext, cfg: Map[String, String]): DataFrame = {
//    val rowRDD = new JavaEsRowRDD(jsc.sqlContext.sparkContext, cfg)
//    val schema = Utils.asJavaDataType(MappingUtils.discoverMapping(rowRDD.esCfg)).asInstanceOf[JStructType]
//    jsc.createDataFrame(rowRDD, schema)
//  }
  
  def saveToEs(srdd: DataFrame, resource: String) {
    saveToEs(srdd, Map(ES_RESOURCE_WRITE -> resource))
  }
  def saveToEs(srdd: DataFrame, resource: String, cfg: Map[String, String]) {
    saveToEs(srdd, collection.mutable.Map(cfg.toSeq: _*) += (ES_RESOURCE_WRITE -> resource))
  }
  def saveToEs(srdd: DataFrame, cfg: Map[String, String]) {
    val sparkCfg = new SparkSettingsManager().load(srdd.sqlContext.sparkContext.getConf)
    val esCfg = new PropertiesSettings().load(sparkCfg.save())
    esCfg.merge(cfg.asJava)
    
    srdd.sqlContext.sparkContext.runJob(srdd.rdd, new EsSchemaRDDWriter(srdd.schema, esCfg.save()).write _)
  }
}