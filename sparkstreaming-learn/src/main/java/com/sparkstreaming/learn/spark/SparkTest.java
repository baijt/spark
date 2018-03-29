package com.sparkstreaming.learn.spark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;


public class SparkTest {
	
	public static void main(String[] args) {
		//sparkConfig 配置
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("wordCount");
		//spark 上下文
		JavaSparkContext js = new JavaSparkContext(conf);
		JavaRDD<String> lines = js.textFile("E:\520.txt");
		//Tran
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>(){

			@Override
			public Iterator<String> call(String line) throws Exception {
				return (Iterator<String>) Arrays.asList(line.split(" +"));
			}
		});
		JavaPairRDD <String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String arg0) throws Exception {
				return new Tuple2<String, Integer>(arg0, 1);
			}
		});
		
		JavaPairRDD<String, Integer> pair2 = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				return arg0+arg1;
			}
		});
		
		pair2.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			@Override
			public void call(Tuple2<String, Integer> arg0) throws Exception {
				System.err.println(arg0._1+"=="+arg0._2);
				
			}
		});
		js.close();
		
	}

}
