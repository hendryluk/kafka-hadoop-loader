package co.gridport.kafka.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaInputFormat implements InputFormat<LongWritable, BytesWritable> {

	private static Logger log = LoggerFactory.getLogger(KafkaInputFormat.class);

    @Override
	public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf conf,
			int numSplits) throws IOException {
		ZkUtils zk = new ZkUtils(
            conf.get("kafka.zk.connect"),
            conf.getInt("kafka.zk.sessiontimeout.ms", 10000),
            conf.getInt("kafka.zk.connectiontimeout.ms", 10000)
        );
        String[] inputTopics = conf.get("kafka.topics").split(",");
        String consumerGroup = conf.get("kafka.groupid");
        List<InputSplit> splits = new ArrayList<InputSplit>();
        for(String topic: inputTopics)
        {
        	log.info("Getting broker partition for topic: {}", topic);
            List<String> brokerPartitions = zk.getBrokerPartitions(topic);
            for(String partition: brokerPartitions) {
            	log.info("Partition: {}", partition);
                
                String[] brokerPartitionParts = partition.split("-");
                String brokerId = brokerPartitionParts[0];
                String partitionId = brokerPartitionParts[1];
                long lastConsumedOffset = zk.getLastConsumedOffset(consumerGroup, topic, partition) ;
                InputSplit split = new KafkaInputSplit(
                    brokerId, 
                    zk.getBrokerName(brokerId), 
                    topic, 
                    Integer.valueOf(partitionId), 
                    lastConsumedOffset
                );
                splits.add(split);
            }
        }
        zk.close();
        log.info("returning splits {}", splits.size());
        return splits.toArray(new InputSplit[splits.size()]);
	}

	@Override
	public org.apache.hadoop.mapred.RecordReader<LongWritable, BytesWritable> getRecordReader(
			org.apache.hadoop.mapred.InputSplit split, JobConf job,
			Reporter reporter) throws IOException {
		log.info("getRecordReader {}", split);
		return new KafkaInputRecordReader(split, job);
	}

}
