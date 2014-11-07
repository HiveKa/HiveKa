/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.kafka.demoproducer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

import com.google.common.collect.Lists;
import kafka.producer.KeyedMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.io.IOUtils;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.producer.ProducerConfig;
import java.util.UUID;

public class DemoProducer {

  private final Producer<String, byte[]> kafkaProducer;

  public DemoProducer(Properties props) {

    kafkaProducer = new Producer<String, byte[]>(new ProducerConfig(props));
  }

  public void publish(GenericRecord event, String topic, Schema schema) {
    try {
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(stream, null);
      DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
      datumWriter.write(event, binaryEncoder);
      binaryEncoder.flush();
      IOUtils.closeQuietly(stream);

      Message m = new Message(stream.toByteArray());
      KeyedMessage<String,byte[]> km = new KeyedMessage<String, byte[]>(topic,m.buffer().array());
      kafkaProducer.send(km);
    } catch (IOException e) {
      throw new RuntimeException("Avro serialization failure", e);
    }
  }

  public static void main(String[] args) {
    Properties props = new Properties();


    String topic = args[0];
    int iters = Integer.parseInt(args[1]);
    props.put("metadata.broker.list", args[2]);
    props.put("request.required.acks", "-1");
    props.put("serializer.class", "kafka.serializer.DefaultEncoder");
    props.put("key.serializer.class", "kafka.serializer.StringEncoder");
    props.put("producer.type", "sync");

    DemoProducer demo = new DemoProducer(props);

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

    for (int i = 1; i < iters; i++) {
      event.put("a", i);
      event.put("b", "static string");
      demo.publish(event, topic, schema);
    }
    demo.kafkaProducer.close();
  }
}
