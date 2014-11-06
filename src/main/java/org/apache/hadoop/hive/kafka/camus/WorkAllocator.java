package org.apache.hadoop.hive.kafka.camus;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public abstract class WorkAllocator {

  protected Properties props;

  public void init(Properties props){
      this.props = props;
  }

  public abstract InputSplit[] allocateWork(List<CamusRequest> requests,
      JobConf conf) throws IOException ;


}
