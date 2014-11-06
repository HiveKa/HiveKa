package org.apache.hadoop.hive.kafka.camus;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public interface IKafkaKey {
  String getServer();

  String getService();

  long getTime();

  String getTopic();

  //String getNodeId();

  int getPartition();

  long getBeginOffset();

  long getOffset();

  long getChecksum();

  long getMessageSize();

  MapWritable getPartitionMap();

  void put(Writable key, Writable value);
}
