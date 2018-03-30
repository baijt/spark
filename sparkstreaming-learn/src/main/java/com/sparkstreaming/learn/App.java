package com.sparkstreaming.learn;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.sparkstreaming.learn.spark.SparkStreamingKafka;


/**
 * Hello world!
 *
 */
public class App 
{
	public static void main(String[] args) {
		
		ApplicationContext applicationContext = new ClassPathXmlApplicationContext("applicationContext.xml");
		
		SparkStreamingKafka sparkStreamingKafka = applicationContext.getBean(SparkStreamingKafka.class);
		sparkStreamingKafka.processSparkStreaming();
		
		
		
		
	}
}
