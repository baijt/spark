package com.sparkstreaming.learn.spark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import scala.Tuple2;


@Component
public class SparkStreamingKafka  implements java.io.Serializable{
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
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
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("test1").set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		//根据sparkconf 创建JavaStreamingContext
		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(10));
		
		//3.配置kafka
		
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "192.168.0.110:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", kafkaGroupId);
		kafkaParams.put("auto.offset.reset", "earliest");
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
		 JavaDStream<String> words = stream.flatMap(new FlatMapFunction<ConsumerRecord<String,String>, String>() {

			@Override
			public Iterator<String> call(ConsumerRecord<String, String> arg0)
					throws Exception {
				System.err.println(arg0.value());
				return   Arrays.asList(arg0.value().split(" +")).iterator(); 
			}
		});
		
		 
		 JavaPairDStream<String, Integer> javaPairDStream = words.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String arg0) throws Exception {
				return new Tuple2<String, Integer>(arg0, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0+arg1;
			}
		});
		 javaPairDStream.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>() {
			
			@Override
			public void call(JavaPairRDD<String, Integer> arg0) throws Exception {
				arg0.collect().stream().forEach(s->{
					System.err.println( s._1+"="+s._2);
				});
			}
		});
		
//        stream.foreachRDD(new VoidFunction2<JavaRDD<ConsumerRecord<String, String>>, Time>() {  
//  
//            private static final long serialVersionUID = 1L;  
//  
//            @Override  
//            public void call(JavaRDD<ConsumerRecord<String, String>> v1, Time v2) throws Exception {  
//  
//                List<ConsumerRecord<String, String>> consumerRecords = v1.collect();  
//  
//                System.out.println("获取消息:" + consumerRecords.size());  
//                if (consumerRecords.size()>0) {
//                	System.out.println("获取消息:" + consumerRecords.get(0).value());  
//				}
//  
//            }  
//        });
		
		streamingContext.start();
		
		try {
			streamingContext.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
