package org.elasticsearch.spark.sql.api.java;

import org.apache.spark.sql.DataFrame;

abstract class SQLUtils {
	// access given field through Java as Scala doesn't allow it...
	static DataFrame baseSchemaRDD(DataFrame schemaRDD) {
		return schemaRDD;
	}
	
	static org.apache.spark.sql.types.DataType asJavaDataType(org.apache.spark.sql.types.DataType scalaDataType) {
		return scalaDataType;
	}
}

