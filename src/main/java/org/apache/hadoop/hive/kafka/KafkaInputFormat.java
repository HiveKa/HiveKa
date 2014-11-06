package org.apache.hadoop.hive.kafka;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.kafka.camus.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContext;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Input format for a Kafka pull job.
 */
public class KafkaInputFormat implements InputFormat<KafkaKey, CamusWrapper> {

	public static final String KAFKA_BLACKLIST_TOPIC = "kafka.blacklist.topics";
	public static final String KAFKA_WHITELIST_TOPIC = "kafka.whitelist.topics";

	public static final String KAFKA_MOVE_TO_LAST_OFFSET_LIST = "kafka.move.to.last.offset.list";

	public static final String KAFKA_CLIENT_BUFFER_SIZE = "kafka.client.buffer.size";
	public static final String KAFKA_CLIENT_SO_TIMEOUT = "kafka.client.so.timeout";

	public static final String KAFKA_MAX_PULL_HRS = "kafka.max.pull.hrs";
	public static final String KAFKA_MAX_PULL_MINUTES_PER_TASK = "kafka.max.pull.minutes.per.task";
	public static final String KAFKA_MAX_HISTORICAL_DAYS = "kafka.max.historical.days";

	public static final String CAMUS_MESSAGE_DECODER_CLASS = "camus.message.decoder.class";
	public static final String ETL_IGNORE_SCHEMA_ERRORS = "etl.ignore.schema.errors";
	public static final String ETL_AUDIT_IGNORE_SERVICE_TOPIC_LIST = "etl.audit.ignore.service.topic.list";

	public static final String CAMUS_WORK_ALLOCATOR_CLASS = "camus.work.allocator.class";
	public static final String CAMUS_WORK_ALLOCATOR_DEFAULT = "com.linkedin.camus.workallocater.BaseAllocator";

	private static Logger log = null;

	public KafkaInputFormat()
  {
	  if (log == null)
	    log = Logger.getLogger(getClass());
  }
	
	public static void setLogger(Logger log){
	  KafkaInputFormat.log = log;
	}

	@Override
	public RecordReader<KafkaKey, CamusWrapper> getRecordReader(
			InputSplit split, JobConf conf, Reporter reporter) throws IOException {
		return new KafkaRecordReader(split, conf, reporter);
	}

  public static String getKafkaBrokers(JobConf job) {
    return job.get(KafkaBackedTableProperties.KAFKA_URI);
  }

	/**
	 * Gets the metadata from Kafka
	 * 
	 * @param conf
	 * @return
	 */
	public List<TopicMetadata> getKafkaMetadata(JobConf conf) {
		ArrayList<String> metaRequestTopics = new ArrayList<String>();
		String brokerString = getKafkaBrokers(conf);
		if (brokerString.isEmpty())
			throw new InvalidParameterException("kafka.brokers must contain at least one node");
                List<String> brokers = Arrays.asList(brokerString.split("\\s*,\\s*"));
		Collections.shuffle(brokers);
		boolean fetchMetaDataSucceeded = false;
		int i = 0;
		List<TopicMetadata> topicMetadataList = null;
		Exception savedException = null;
		while (i < brokers.size() && !fetchMetaDataSucceeded) {
			SimpleConsumer consumer = createConsumer(conf, brokers.get(i));
			log.info(String.format("Fetching metadata from broker %s with client id %s for %d topic(s) %s",
			brokers.get(i), consumer.clientId(), metaRequestTopics.size(), metaRequestTopics));
			try {
				topicMetadataList = consumer.send(new TopicMetadataRequest(metaRequestTopics)).topicsMetadata();
				fetchMetaDataSucceeded = true;
			} catch (Exception e) {
				savedException = e;
				log.warn(String.format("Fetching topic metadata with client id %s for topics [%s] from broker [%s] failed",
					consumer.clientId(), metaRequestTopics, brokers.get(i)), e);
			} finally {
				consumer.close();
				i++;
			}
		}
		if (!fetchMetaDataSucceeded) {
			throw new RuntimeException("Failed to obtain metadata!", savedException);
		}
		return topicMetadataList;
	}
 
	private SimpleConsumer createConsumer(JobConf conf, String broker) {
		if (!broker.matches(".+:\\d+"))
			throw new InvalidParameterException("The kakfa broker " + broker + " must follow address:port pattern");
		String[] hostPort = broker.split(":");
    //TODO: get from conf
		SimpleConsumer consumer = new SimpleConsumer(
			hostPort[0],
			Integer.valueOf(hostPort[1]),
			30000,
			1024 * 1024,
			"hive_kafka_client");
		return consumer;
	}

	/**
	 * Gets the latest offsets and create the requests as needed
	 * 
	 * @param conf
	 * @param offsetRequestInfo
	 * @return
	 */
	public ArrayList<CamusRequest> fetchLatestOffsetAndCreateKafkaRequests(
			JobConf conf,
			HashMap<LeaderInfo, ArrayList<TopicAndPartition>> offsetRequestInfo) {
		ArrayList<CamusRequest> finalRequests = new ArrayList<CamusRequest>();
		for (LeaderInfo leader : offsetRequestInfo.keySet()) {
			SimpleConsumer consumer = new SimpleConsumer(leader.getUri()
					.getHost(), leader.getUri().getPort(),
					30000,
					1024 * 1024,
					"hive_kafka_client");
			// Latest Offset
			PartitionOffsetRequestInfo partitionLatestOffsetRequestInfo = new PartitionOffsetRequestInfo(
					kafka.api.OffsetRequest.LatestTime(), 1);
			// Earliest Offset
			PartitionOffsetRequestInfo partitionEarliestOffsetRequestInfo = new PartitionOffsetRequestInfo(
					kafka.api.OffsetRequest.EarliestTime(), 1);
			Map<TopicAndPartition, PartitionOffsetRequestInfo> latestOffsetInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
			Map<TopicAndPartition, PartitionOffsetRequestInfo> earliestOffsetInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
			ArrayList<TopicAndPartition> topicAndPartitions = offsetRequestInfo
					.get(leader);
			for (TopicAndPartition topicAndPartition : topicAndPartitions) {
				latestOffsetInfo.put(topicAndPartition,
						partitionLatestOffsetRequestInfo);
				earliestOffsetInfo.put(topicAndPartition,
						partitionEarliestOffsetRequestInfo);
			}

			OffsetResponse latestOffsetResponse = consumer
					.getOffsetsBefore(new OffsetRequest(latestOffsetInfo,
							kafka.api.OffsetRequest.CurrentVersion(), "hive_kafka_client"));
			OffsetResponse earliestOffsetResponse = consumer
					.getOffsetsBefore(new OffsetRequest(earliestOffsetInfo,
							kafka.api.OffsetRequest.CurrentVersion(), "hive_kafka_client"));
			consumer.close();
			for (TopicAndPartition topicAndPartition : topicAndPartitions) {
				long latestOffset = latestOffsetResponse.offsets(
						topicAndPartition.topic(),
						topicAndPartition.partition())[0];
				long earliestOffset = earliestOffsetResponse.offsets(
						topicAndPartition.topic(),
						topicAndPartition.partition())[0];
				
				//TODO: factor out kafka specific request functionality 
				CamusRequest etlRequest = new KafkaRequest(conf,
						topicAndPartition.topic(), Integer.toString(leader
								.getLeaderId()), topicAndPartition.partition(),
						leader.getUri());
				etlRequest.setLatestOffset(latestOffset);
				etlRequest.setEarliestOffset(earliestOffset);
				finalRequests.add(etlRequest);
			}
		}
		return finalRequests;
	}

	public String createTopicRegEx(HashSet<String> topicsSet) {
		String regex = "";
		StringBuilder stringbuilder = new StringBuilder();
		for (String whiteList : topicsSet) {
			stringbuilder.append(whiteList);
			stringbuilder.append("|");
		}
		regex = "(" + stringbuilder.substring(0, stringbuilder.length() - 1)
				+ ")";
		Pattern.compile(regex);
		return regex;
	}

	public List<TopicMetadata> filterWhitelistTopics(
			List<TopicMetadata> topicMetadataList,
			HashSet<String> whiteListTopics) {
		ArrayList<TopicMetadata> filteredTopics = new ArrayList<TopicMetadata>();
		String regex = createTopicRegEx(whiteListTopics);
		for (TopicMetadata topicMetadata : topicMetadataList) {
			if (Pattern.matches(regex, topicMetadata.topic())) {
				filteredTopics.add(topicMetadata);
			} else {
				log.info("Discarding topic : " + topicMetadata.topic());
			}
		}
		return filteredTopics;
	}

	@Override
	public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
		ArrayList<CamusRequest> finalRequests;
		HashMap<LeaderInfo, ArrayList<TopicAndPartition>> offsetRequestInfo = new HashMap<LeaderInfo, ArrayList<TopicAndPartition>>();
		try {

			// Get Metadata for all topics
			List<TopicMetadata> topicMetadataList = getKafkaMetadata(conf);

			// Filter any white list topics
			HashSet<String> whiteListTopics = new HashSet<String>(
					Arrays.asList(getKafkaWhitelistTopic(conf)));
			if (!whiteListTopics.isEmpty()) {
				topicMetadataList = filterWhitelistTopics(topicMetadataList,
						whiteListTopics);
			}

			// Filter all blacklist topics
			HashSet<String> blackListTopics = new HashSet<String>(
					Arrays.asList(getKafkaBlacklistTopic(conf)));
			String regex = "";
			if (!blackListTopics.isEmpty()) {
				regex = createTopicRegEx(blackListTopics);
			}
			for (TopicMetadata topicMetadata : topicMetadataList) {
				if (Pattern.matches(regex, topicMetadata.topic())) {
					log.info("Discarding topic (blacklisted): "
							+ topicMetadata.topic());
				} else if (!createMessageDecoder(conf, topicMetadata.topic())) {
					log.info("Discarding topic (Decoder generation failed) : "
							+ topicMetadata.topic());
				} else if (topicMetadata.errorCode() != ErrorMapping.NoError()) {
                  log.info("Skipping the creation of ETL request for Whole Topic : "
                      + topicMetadata.topic()
                      + " Exception : "
                      + ErrorMapping
                              .exceptionFor(topicMetadata
                                      .errorCode()));
				} else {
					for (PartitionMetadata partitionMetadata : topicMetadata
							.partitionsMetadata()) {
					      // We only care about LeaderNotAvailableCode error on partitionMetadata level
					      // Error codes such as ReplicaNotAvailableCode should not stop us.
						if (partitionMetadata.errorCode() == ErrorMapping.LeaderNotAvailableCode()) {
							log.info("Skipping the creation of ETL request for Topic : "
									+ topicMetadata.topic()
									+ " and Partition : "
									+ partitionMetadata.partitionId()
									+ " Exception : "
									+ ErrorMapping
											.exceptionFor(partitionMetadata
													.errorCode()));
						} else {
	                        if (partitionMetadata.errorCode() != ErrorMapping.NoError()) {
	                          log.warn("Receiving non-fatal error code, Continuing the creation of ETL request for Topic : "
                                    + topicMetadata.topic()
                                    + " and Partition : "
                                    + partitionMetadata.partitionId()
                                    + " Exception : "
                                    + ErrorMapping
                                            .exceptionFor(partitionMetadata
                                                    .errorCode()));
	                        }
							LeaderInfo leader = new LeaderInfo(new URI("tcp://"
									+ partitionMetadata.leader()
											.getConnectionString()),
									partitionMetadata.leader().id());
							if (offsetRequestInfo.containsKey(leader)) {
								ArrayList<TopicAndPartition> topicAndPartitions = offsetRequestInfo
										.get(leader);
								topicAndPartitions.add(new TopicAndPartition(
										topicMetadata.topic(),
										partitionMetadata.partitionId()));
								offsetRequestInfo.put(leader,
										topicAndPartitions);
							} else {
								ArrayList<TopicAndPartition> topicAndPartitions = new ArrayList<TopicAndPartition>();
								topicAndPartitions.add(new TopicAndPartition(
										topicMetadata.topic(),
										partitionMetadata.partitionId()));
								offsetRequestInfo.put(leader,
										topicAndPartitions);
							}

						}
					}
				}
			}
		} catch (Exception e) {
			log.error(
					"Unable to pull requests from Kafka brokers. Exiting the program",
					e);
			return null;
		}
		// Get the latest offsets and generate the KafkaRequests
		finalRequests = fetchLatestOffsetAndCreateKafkaRequests(conf,
				offsetRequestInfo);

		Collections.sort(finalRequests, new Comparator<CamusRequest>() {
			public int compare(CamusRequest r1, CamusRequest r2) {
				return r1.getTopic().compareTo(r2.getTopic());
			}
		});

		log.info("The requests from kafka metadata are: \n" + finalRequests);		
		//writeRequests(finalRequests, context);
		Map<CamusRequest, KafkaKey> offsetKeys = getPreviousOffsets(
				FileInputFormat.getInputPaths(conf),conf);
		Set<String> moveLatest = getMoveToLatestTopicsSet(conf);
		for (CamusRequest request : finalRequests) {
			if (moveLatest.contains(request.getTopic())
					|| moveLatest.contains("all")) {
			    log.info("Moving to latest for topic: " + request.getTopic());
			  //TODO: factor out kafka specific request functionality 
			    KafkaKey oldKey = offsetKeys.get(request);
			    KafkaKey newKey = new KafkaKey(request.getTopic(), ((KafkaRequest)request).getLeaderId(),
              request.getPartition(), 0, request
              .getLastOffset());
			    
			    if (oldKey != null)
			      newKey.setMessageSize(oldKey.getMessageSize());
			    
			    offsetKeys.put(request, newKey);
			}

			KafkaKey key = offsetKeys.get(request);

			if (key != null) {
				request.setOffset(key.getOffset());
				request.setAvgMsgSize(key.getMessageSize());
			}

			if (request.getEarliestOffset() > request.getOffset()
					|| request.getOffset() > request.getLastOffset()) {
				if(request.getEarliestOffset() > request.getOffset())
				{
					log.error("The earliest offset was found to be more than the current offset: " + request);
					log.error("Moving to the earliest offset available");
				}
				else
				{
					log.error("The current offset was found to be more than the latest offset: " + request);
					log.error("Moving to the earliest offset available");
				}
				request.setOffset(request.getEarliestOffset());
				offsetKeys.put(
						request,
					//TODO: factor out kafka specific request functionality 
						new KafkaKey(request.getTopic(), ((KafkaRequest)request).getLeaderId(),
								request.getPartition(), 0, request
										.getOffset()));
			}
			log.info(request);
		}

		//writePrevious(offsetKeys.values(), context);
		
		WorkAllocator allocator = getWorkAllocator(conf);
		Properties props = new Properties();
		props.putAll(conf.getValByRegex(".*"));
		allocator.init(props);
		
		return allocator.allocateWork(finalRequests, conf);
	}

	private Set<String> getMoveToLatestTopicsSet(JobConf conf) {
		Set<String> topics = new HashSet<String>();

		String[] arr = getMoveToLatestTopics(conf);

		if (arr != null) {
			for (String topic : arr) {
				topics.add(topic);
			}
		}

		return topics;
	}

	private boolean createMessageDecoder(JobConf conf, String topic) {
		try {
			MessageDecoderFactory.createMessageDecoder(conf, topic);
			return true;
		} catch (Exception e) {
		  log.error("failed to create decoder", e);
			return false;
		}
	}
/*
	private void writePrevious(Collection<KafkaKey> missedKeys, JobContext context)
			throws IOException {
		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path output = FileOutputFormat.getOutputPath(context);

		if (fs.exists(output)) {
			fs.mkdirs(output);
		}

		output = new Path(output, KafkaMultiOutputFormat.OFFSET_PREFIX
				+ "-previous");
		SequenceFile.Writer writer = SequenceFile.createWriter(fs,
        context.getConfiguration(), output, KafkaKey.class,
        NullWritable.class);

		for (KafkaKey key : missedKeys) {
			writer.append(key, NullWritable.get());
		}

		writer.close();
	}
*/

/*	private void writeRequests(List<CamusRequest> requests, JobContext context)
			throws IOException {
		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path output = FileOutputFormat.getOutputPath(context);

		if (fs.exists(output)) {
			fs.mkdirs(output);
		}

		output = new Path(output, KafkaMultiOutputFormat.REQUESTS_FILE);
		SequenceFile.Writer writer = SequenceFile.createWriter(fs,
        context.getConfiguration(), output, KafkaRequest.class,
        NullWritable.class);

		for (CamusRequest r : requests) {
		  //TODO: factor out kafka specific request functionality 
			writer.append((KafkaRequest) r, NullWritable.get());
		}
		writer.close();
	}
*/

	private Map<CamusRequest, KafkaKey> getPreviousOffsets(Path[] inputs,
			JobConf conf) throws IOException {
		Map<CamusRequest, KafkaKey> offsetKeysMap = new HashMap<CamusRequest, KafkaKey>();
		for (Path input : inputs) {
			FileSystem fs = input.getFileSystem(conf);
			for (FileStatus f : fs.listStatus(input, new OffsetFileFilter())) {
				log.info("previous offset file:" + f.getPath().toString());
				SequenceFile.Reader reader = new SequenceFile.Reader(fs,
						f.getPath(), conf);
				KafkaKey key = new KafkaKey();
				while (reader.next(key, NullWritable.get())) {
				//TODO: factor out kafka specific request functionality 
					CamusRequest request = new KafkaRequest(conf,
							key.getTopic(), key.getLeaderId(),
							key.getPartition());
					if (offsetKeysMap.containsKey(request)) {

						KafkaKey oldKey = offsetKeysMap.get(request);
						if (oldKey.getOffset() < key.getOffset()) {
							offsetKeysMap.put(request, key);
						}
					} else {
						offsetKeysMap.put(request, key);
					}
					key = new KafkaKey();
				}
				reader.close();
			}
		}
		return offsetKeysMap;
	}
	
	 public static void setWorkAllocator(JobContext job, Class<WorkAllocator> val) {
	    job.getConfiguration().setClass(CAMUS_WORK_ALLOCATOR_CLASS, val, WorkAllocator.class);
	  }

	  public static WorkAllocator getWorkAllocator(JobConf job) {
	    try {
        return (WorkAllocator) job.getClass(BaseAllocator.class.getName(), BaseAllocator.class).newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
	  }

	public static void setMoveToLatestTopics(JobContext job, String val) {
		job.getConfiguration().set(KAFKA_MOVE_TO_LAST_OFFSET_LIST, val);
	}

	public static String[] getMoveToLatestTopics(JobConf job) {
		return job.getStrings(KAFKA_MOVE_TO_LAST_OFFSET_LIST);
	}

	public static void setKafkaClientBufferSize(JobContext job, int val) {
		job.getConfiguration().setInt(KAFKA_CLIENT_BUFFER_SIZE, val);
	}

	public static int getKafkaClientBufferSize(JobContext job) {
		return job.getConfiguration().getInt(KAFKA_CLIENT_BUFFER_SIZE,
				2 * 1024 * 1024);
	}

	public static void setKafkaClientTimeout(JobContext job, int val) {
		job.getConfiguration().setInt(KAFKA_CLIENT_SO_TIMEOUT, val);
	}

	public static int getKafkaClientTimeout(JobContext job) {
		return job.getConfiguration().getInt(KAFKA_CLIENT_SO_TIMEOUT, 60000);
	}

	public static void setKafkaMaxPullHrs(JobContext job, int val) {
		job.getConfiguration().setInt(KAFKA_MAX_PULL_HRS, val);
	}

	public static int getKafkaMaxPullHrs(JobContext job) {
		return job.getConfiguration().getInt(KAFKA_MAX_PULL_HRS, -1);
	}

	public static void setKafkaMaxPullMinutesPerTask(JobContext job, int val) {
		job.getConfiguration().setInt(KAFKA_MAX_PULL_MINUTES_PER_TASK, val);
	}

	public static int getKafkaMaxPullMinutesPerTask(JobContext job) {
		return job.getConfiguration().getInt(KAFKA_MAX_PULL_MINUTES_PER_TASK,
				-1);
	}

	public static void setKafkaMaxHistoricalDays(JobContext job, int val) {
		job.getConfiguration().setInt(KAFKA_MAX_HISTORICAL_DAYS, val);
	}

	public static int getKafkaMaxHistoricalDays(JobContext job) {
		return job.getConfiguration().getInt(KAFKA_MAX_HISTORICAL_DAYS, -1);
	}

	public static void setKafkaBlacklistTopic(JobContext job, String val) {
		job.getConfiguration().set(KAFKA_BLACKLIST_TOPIC, val);
	}

	public static String[] getKafkaBlacklistTopic(JobConf job) {
		if (job.get(KAFKA_BLACKLIST_TOPIC) != null && !(job.get(KAFKA_BLACKLIST_TOPIC).isEmpty())) {
			return job.getStrings(KAFKA_BLACKLIST_TOPIC);
		} else {
			return new String[] {};
		}
	}

	public static void setKafkaWhitelistTopic(JobContext job, String val) {
		job.getConfiguration().set(KAFKA_WHITELIST_TOPIC, val);
	}

	public static String[] getKafkaWhitelistTopic(JobConf job) {
		if (job.get(KAFKA_WHITELIST_TOPIC) != null
				&& !job.get(KAFKA_WHITELIST_TOPIC).isEmpty()) {
			return job.getStrings(KAFKA_WHITELIST_TOPIC);
		} else {
			return new String[] {};
		}
	}

	public static void setKafkaIgnoreSchemaErrors(JobContext job, boolean val) {
		job.getConfiguration().setBoolean(ETL_IGNORE_SCHEMA_ERRORS, val);
	}

	public static boolean getKafkaIgnoreSchemaErrors(JobContext job) {
		return job.getConfiguration().getBoolean(ETL_IGNORE_SCHEMA_ERRORS,
				false);
	}

	public static void setKafkaAuditIgnoreServiceTopicList(JobContext job,
			String topics) {
		job.getConfiguration().set(ETL_AUDIT_IGNORE_SERVICE_TOPIC_LIST, topics);
	}


	public static void setMessageDecoderClass(JobContext job,
			Class<MessageDecoder> cls) {
		job.getConfiguration().setClass(CAMUS_MESSAGE_DECODER_CLASS, cls,
        MessageDecoder.class);
	}

	public static Class<MessageDecoder> getMessageDecoderClass(JobContext job) {
		return (Class<MessageDecoder>) job.getConfiguration().getClass(
				CAMUS_MESSAGE_DECODER_CLASS, KafkaAvroMessageDecoder.class);
	}

	private class OffsetFileFilter implements PathFilter {

		@Override
		public boolean accept(Path arg0) {
			return arg0.getName()
					.startsWith("kafka_offset_");
		}
	}
}
