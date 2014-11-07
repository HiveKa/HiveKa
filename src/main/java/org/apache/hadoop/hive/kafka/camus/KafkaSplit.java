package org.apache.hadoop.hive.kafka.camus;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

public class KafkaSplit extends FileSplit {
  private List<CamusRequest> requests = new ArrayList<CamusRequest>();
  private long length = 0;
  private String currentTopic = "";
  private Path path;

  public KafkaSplit() {
    // TODO: Passing single spaced path " " is not the best way.
    // need to figure out better ways of handling this.
    // path is properly initialized when readFields is called
    this(new Path(" "));
  }

  public KafkaSplit(Path path) {
    super(path, 0, 0, (String[]) null);
    this.path = path;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      CamusRequest r = new KafkaRequest();
      r.readFields(in);
      requests.add(r);
      length += r.estimateDataSize();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(requests.size());
    for (CamusRequest r : requests)
      r.write(out);
  }

  @Override
  public long getLength() {
    return length;
  }

  public int getNumRequests() {
    return requests.size();
  }

  @Override
  public String[] getLocations() throws IOException {
    return new String[] {};
  }

  public void addRequest(CamusRequest request) {
    requests.add(request);
    length += request.estimateDataSize();
  }

  public CamusRequest popRequest() {
    if (requests.size() > 0){
      for (int i = 0; i < requests.size(); i++) {
        // return all request for each topic before returning another topic
        if (requests.get(i).getTopic().equals(currentTopic))
          return requests.remove(i);
      }
      CamusRequest cr = requests.remove(requests.size() - 1);
      currentTopic = cr.getTopic();
      return cr;
    }
    else
      return null;
  }

  @Override
  public Path getPath() {
    return path;
  }
}
