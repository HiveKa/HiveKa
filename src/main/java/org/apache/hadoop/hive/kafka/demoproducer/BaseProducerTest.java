package org.apache.hadoop.hive.kafka.demoproducer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.Test;
import org.junit.Assert;

public class BaseProducerTest {

  @Test
  public void testSerializeAvro() throws Exception {


    Schema schema = new Schema.Parser().parse("{\n" +
        "\t\"type\":\"record\",\n" +
        "\t\"name\":\"test_schema_1\",\n" +
        "\t\"fields\" : [ {\n" +
        "\t\t\"name\":\"a\",\n" +
        "\t\t\"type\":\"int\"\n" +
        "\t\t},\n" +
        "\t\t{\n" +
        "\t\t\"name\":\"b\",\n" +
        "\t\t\"type\":\"string\"\n" +
        "\t\t}\n" +
        "\t\t]\n" +
        "}");

    GenericRecord event = new GenericData.Record(schema);

    event.put("a", 1);
    event.put("b", "static string");

    byte[] m = BaseProducer.serializeAvro(schema,event);

    DatumReader<GenericData.Record> reader = new GenericDatumReader<GenericData.Record>(schema);
    BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(m,null);

    GenericRecord record = reader.read(null,binaryDecoder);

    Assert.assertEquals(record,event);




  }
}