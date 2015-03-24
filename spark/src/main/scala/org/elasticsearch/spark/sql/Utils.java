package org.elasticsearch.spark.sql;

import org.elasticsearch.hadoop.serialization.FieldType;
import org.elasticsearch.hadoop.serialization.dto.mapping.Field;

abstract class Utils {
	
	static FieldType extractType(Field field) {
		return field.type();
	}

	static org.apache.spark.sql.types.DataType asScalaDataType(org.apache.spark.sql.types.DataType javaDataType) {
		return javaDataType;
	}

	static org.apache.spark.sql.types.DataType asJavaDataType(org.apache.spark.sql.types.DataType scalaDataType) {
		return scalaDataType;
	}
}

