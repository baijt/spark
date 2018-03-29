package com.sparkstreaming.learn.spark;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


@Component
public class SparkStreamingKafka {
	
	
	@Value("${spark.appname}")
	private String appName;
	@Value("${spark.master}")
	private String master;
	@Value("${spark.seconds}")
	private long second;
	@Value("${kafka.metadata.broker.list}")
	private String metadataBrokerList;
	@Value("${kafka.auto.offset.reset}")
	private String autoOffsetReset;
	@Value("${kafka.topics}")
	private String kafkaTopics;
	@Value("${kafka.group.id}")
	private String kafkaGroupId;

	
	public void processSparkStreaming(){
		//创建sparkconf
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("test1");
		//根据sparkconf 创建JavaStreamingContext
		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(1));
		
		//3.配置kafka
		
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "192.168.0.110:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", kafkaGroupId);
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		
		
//		Collection<String> topics = Arrays.asList("testAstreaming");
		
		  Set<String> topics =new HashSet<String>();
	         topics.add("boottest");
		
		
		JavaInputDStream<ConsumerRecord<String, String>> stream =
				  KafkaUtils.createDirectStream(
				    streamingContext,
				    LocationStrategies.PreferConsistent(),
				    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
				  );
		
//		streamingContext.sparkContext().broadcast(kafkaParams);
		
//		stream.mapToPair(record-> new Tuple2<>(record.key(),record.value()));

        // 6.spark rdd转化和行动处理  
        stream.foreachRDD(new VoidFunction2<JavaRDD<ConsumerRecord<String, String>>, Time>() {  
  
            private static final long serialVersionUID = 1L;  
  
            @Override  
            public void call(JavaRDD<ConsumerRecord<String, String>> v1, Time v2) throws Exception {  
  
                List<ConsumerRecord<String, String>> consumerRecords = v1.collect();  
  
                System.out.println("获取消息:" + consumerRecords.size());  
  
            }  
        });
		
		streamingContext.start();
		
		try {
			streamingContext.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
