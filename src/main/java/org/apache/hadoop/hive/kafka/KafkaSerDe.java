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

package org.apache.hadoop.hive.kafka;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

import java.util.Properties;

public class KafkaSerDe extends LazyBinarySerDe {

  private static final Log LOG = LogFactory.getLog(KafkaSerDe.class);
  private static final String TABLE_NAME = "name";


  public KafkaSerDe() throws SerDeException {
    super();
  }

  /**
   * Initialize the SerDe with configuration and table information.
   */
  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    super.initialize(conf, tbl);
  }

  /**
   * Returns the Writable class that would be returned by the serialize method.
   * This is used to initialize SequenceFile header.
   */
  @Override
  public Class<? extends Writable> getSerializedClass() {
    return BytesWritable.class;
  }

  /**
   * Deserialize a table record to a lazybinary struct.
   */
  @Override
  public Object deserialize(Writable field) throws SerDeException {
    LOG.info("Deserializing from KafkaSerde");
    return super.deserialize(field);
  }
}
