package org.apache.hadoop.hive.kafka.camus;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.kafka.AvroSchemaGenerator;
import org.apache.hadoop.hive.kafka.KafkaBackedTableProperties;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Properties;

public class KafkaAvroMessageDecoder extends MessageDecoder<byte[], Record> {
	protected DecoderFactory decoderFactory;
	protected SchemaRegistry<Schema> registry;
	private Schema latestSchema;
  private static Logger log = null;
	
	@Override
	public void init(Properties props, String topicName) {
    super.init(props, topicName);
    if (log == null) {
      log = Logger.getLogger(getClass());
    }

    try {
      SchemaRegistry<Schema> registry = (SchemaRegistry<Schema>) Class
          .forName("org.apache.hadoop.hive.kafka.camus" +
              ".MemorySchemaRegistry").newInstance();

      registry.init(props);

      this.registry = new CachedSchemaRegistry<Schema>(registry);

      Schema.Parser parser = new Schema.Parser();
      Schema schema;

      final String schemaFile = props.getProperty(KafkaBackedTableProperties
          .KAFKA_AVRO_SCHEMA_FILE, null);

      if (schemaFile != null && !schemaFile.isEmpty()) {

        Path pt = new Path(schemaFile);
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        StringBuilder line = new StringBuilder();
        String tempLine = br.readLine();
        while (tempLine != null) {
          line.append(tempLine);
          tempLine = br.readLine();
        }
        log.info("Setting expected schema, from file, to " + line.toString());
        try {
          schema = parser.parse(line.toString());
        } catch (Exception e) {
          throw new RuntimeException("Failed to parse Avro schema from " + schemaFile, e);
        }
      } else {

        AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGenerator();

        schema = avroSchemaGenerator.getSchema(props.getProperty
                (KafkaBackedTableProperties.COLUMN_NAMES), props.getProperty(KafkaBackedTableProperties
                .COLUMN_TYPES), props.getProperty(KafkaBackedTableProperties.COLUMN_COMMENTS),
            "avro.schema.namespace", "avro.schema.name", "avro.schema.doc");

        log.info("Setting expected schema, from hive table schema, to " + schema);
      }

      this.latestSchema = schema;
    } catch (Exception e) {
      throw new MessageDecoderException(e);
    }

    decoderFactory = DecoderFactory.get();
  }

	private class MessageDecoderHelper {
		//private Message message;
		private ByteBuffer buffer;
		private Schema schema;
		private int start;
		private int length;
		private Schema targetSchema;
		private static final byte MAGIC_BYTE = 0x0;
		private final SchemaRegistry<Schema> registry;
		private final String topicName;
		private byte[] payload;

		public MessageDecoderHelper(SchemaRegistry<Schema> registry,
				String topicName, byte[] payload) {
			this.registry = registry;
			this.topicName = topicName;
			//this.message = message;
			this.payload = payload;
		}

		public ByteBuffer getBuffer() {
			return buffer;
		}

		public Schema getSchema() {
			return schema;
		}

		public int getStart() {
			return start;
		}

		public int getLength() {
			return length;
		}

		public Schema getTargetSchema() {
      return new Schema.Parser().parse(targetSchema.toString());
		}

		private ByteBuffer getByteBuffer(byte[] payload) {
			ByteBuffer buffer = ByteBuffer.wrap(payload);
			//if (buffer.get() != MAGIC_BYTE)
			//	throw new IllegalArgumentException("Unknown magic byte!");
			return buffer;
		}

		public MessageDecoderHelper invoke() {
			buffer = getByteBuffer(payload);
			//String id = Integer.toString(buffer.getInt());
			//schema = registry.getSchemaByID(topicName, id);
			//if (schema == null)
			//	throw new IllegalStateException("Unknown schema id: " + id);

			start = buffer.position() + buffer.arrayOffset();
      //length = buffer.limit() - 5;
      length = buffer.limit();

			// try to get a target schema, if any
			targetSchema = latestSchema;
			return this;
		}
	}

	public AvroGenericRecordWritable decode(byte[] payload) {
		try {
			MessageDecoderHelper helper = new MessageDecoderHelper(registry,
					topicName, payload).invoke();
      DatumReader<Record> reader = new GenericDatumReader<Record>(helper.getTargetSchema());

      log.debug("Trying to read kafka payload");
      log.debug("buffer: " + helper.getBuffer());
      log.debug("start: " + helper.getStart());
      log.debug("length: " + helper.getLength());
      log.debug("target schema: " + helper.getTargetSchema());
      log.debug("schema: " + helper.getSchema());
		  GenericRecord record = reader.read(null, decoderFactory.binaryDecoder(helper.getBuffer().array(),
          helper.getStart(), helper.getLength(), null));
      log.debug("Read kafka payload as " + record);

      AvroGenericRecordWritable grw = new AvroGenericRecordWritable(record);
      grw.setFileSchema(latestSchema);

      return grw;
	
		} catch (IOException e) {
			throw new MessageDecoderException(e);
		}
	}

	public static class CamusAvroWrapper extends CamusWrapper<Record> {

	    public CamusAvroWrapper(Record record) {
            super(record);
            Record header = (Record) super.getRecord().get("header");
   	        if (header != null) {
               if (header.get("server") != null) {
                   put(new Text("server"), new Text(header.get("server").toString()));
               }
               if (header.get("service") != null) {
                   put(new Text("service"), new Text(header.get("service").toString()));
               }
            }
        }
	    
	    @Override
	    public long getTimestamp() {
	        Record header = (Record) super.getRecord().get("header");

	        if (header != null && header.get("time") != null) {
	            return (Long) header.get("time");
	        } else if (super.getRecord().get("timestamp") != null) {
	            return (Long) super.getRecord().get("timestamp");
	        } else {
	            return System.currentTimeMillis();
	        }
	    }
	}
}
