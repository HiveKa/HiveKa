package org.apache.hadoop.hive.kafka;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.serde2.avro.TypeInfoToSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AvroSchemaGenerator {
  private TypeInfoToSchema typeInfoToSchema;

  public AvroSchemaGenerator() {
    this.typeInfoToSchema = new TypeInfoToSchema();
  }

  public Schema getSchema(String columnNamesStr, String columnTypesStr,
                          String columnCommentsStr, String namespace, String name,
                          String doc) {
    List<String> columnNames = Arrays.asList(columnNamesStr.split(","));
    List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypesStr);
    List<String> columnComments;
    if (columnCommentsStr.isEmpty()) {
      columnComments = new ArrayList<String>();
    } else {
      columnComments = Arrays.asList(columnCommentsStr.split(","));
    }

    return typeInfoToSchema.convert(columnNames, columnTypes, columnComments, namespace, name, doc);
  }
}
