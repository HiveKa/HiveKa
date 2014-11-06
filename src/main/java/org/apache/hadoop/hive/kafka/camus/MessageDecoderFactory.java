package org.apache.hadoop.hive.kafka.camus;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;

import java.util.Map.Entry;
import java.util.Properties;

public class MessageDecoderFactory {
    
    public static MessageDecoder<?,?> createMessageDecoder(JobConf conf, String topicName){
        MessageDecoder<?,?> decoder;
        try {
            decoder = (MessageDecoder<?,?>) KafkaAvroMessageDecoder.class.newInstance();
            
            Properties props = new Properties();
            for (Entry<String, String> entry : conf){
                props.put(entry.getKey(), entry.getValue());
            }
            
            decoder.init(props, topicName);
            
            return decoder;
        } catch (Exception e) {
            throw new MessageDecoderException(e);
        }    
    }

}
