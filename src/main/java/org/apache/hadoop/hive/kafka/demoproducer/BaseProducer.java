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

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

public class BaseProducer {

  private final Producer<String, byte[]> kafkaProducer;

  public BaseProducer(Properties props) {

    kafkaProducer = new Producer<String, byte[]>(new ProducerConfig(props));
  }

  public static byte[] serializeAvro(Schema schema, GenericRecord event) throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(stream, null);
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    datumWriter.write(event, binaryEncoder);
    binaryEncoder.flush();
    IOUtils.closeQuietly(stream);


    return stream.toByteArray();
  }

  public void publish(GenericRecord event, String topic, Schema schema) {
    try {
      byte[] m = serializeAvro(schema, event);
      KeyedMessage<String,byte[]> km = new KeyedMessage<String, byte[]>(topic,m);
      kafkaProducer.send(km);
    } catch (IOException e) {
      throw new RuntimeException("Avro serialization failure", e);
    }
  }

  public void close() {
    kafkaProducer.close();
  }
}
