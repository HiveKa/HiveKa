package org.apache.hadoop.hive.kafka.camus;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

public class BaseAllocator extends WorkAllocator{

  protected Properties props;

  public void init(Properties props){
      this.props = props;
  }
  
  protected void reverseSortRequests(List<CamusRequest> requests){
    // Reverse sort by size
    Collections.sort(requests, new Comparator<CamusRequest>() {
      @Override
      public int compare(CamusRequest o1, CamusRequest o2) {
        if (o2.estimateDataSize() == o1.estimateDataSize()) {
          return 0;
        }
        if (o2.estimateDataSize() < o1.estimateDataSize()) {
          return -1;
        } else {
          return 1;
        }
      }
    });
  }

  @Override
  public InputSplit[] allocateWork(List<CamusRequest> requests,
      JobConf conf) throws IOException {
    int numTasks = conf.getInt("mapred.map.tasks", 30);
    
    reverseSortRequests(requests);

    List<InputSplit> kafkaETLSplits = new ArrayList<InputSplit>();
    Path[] tablePaths = FileInputFormat.getInputPaths(conf);

    for (int i = 0; i < numTasks; i++) {
      if (requests.size() > 0) {
        kafkaETLSplits.add(new KafkaSplit(tablePaths[0]));
      }
    }

    for (CamusRequest r : requests) {
      getSmallestMultiSplit(kafkaETLSplits).addRequest(r);
    }

    InputSplit[] inputSplits = new InputSplit[kafkaETLSplits.size()];

    return kafkaETLSplits.toArray(inputSplits);
  }
  
  protected KafkaSplit getSmallestMultiSplit(List<InputSplit> kafkaETLSplits)
      throws IOException {
    KafkaSplit smallest = (KafkaSplit) kafkaETLSplits.get(0);

    for (int i = 1; i < kafkaETLSplits.size(); i++) {
      KafkaSplit challenger = (KafkaSplit) kafkaETLSplits.get(i);
      if ((smallest.getLength() == challenger.getLength() && smallest
          .getNumRequests() > challenger.getNumRequests())
          || smallest.getLength() > challenger.getLength()) {
        smallest = challenger;
      }
    }

    return smallest;
  }


}
