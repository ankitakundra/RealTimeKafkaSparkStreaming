package com.spark.machine.simulation.machinedata;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapred.JobConf;

import scala.Tuple2;



public class JavaKafkaSparkStreaming implements Serializable{

	
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) throws IOException {
		String topic_name = "machinedata";

		SparkConf sparkconf = new SparkConf().setAppName("kafkaStreaming");
		JavaSparkContext sc = new JavaSparkContext(sparkconf);
		JavaStreamingContext jsc = new JavaStreamingContext(sc,
				Durations.seconds(2));
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("group.id", "test");
		Set<String> topic = new HashSet<String>();
		topic.add(topic_name);
		JavaPairInputDStream<String, String> messages = KafkaUtils
				.createDirectStream(jsc, java.lang.String.class,
						java.lang.String.class, StringDecoder.class,
						StringDecoder.class, kafkaParams, topic);
		
		JavaPairDStream<String, String> records = messages
				.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {

					public Tuple2<String, String> call(
							Tuple2<String, String> args) {
						return new Tuple2<String, String>(args._1, args._2);
					}
				});
		
		records.foreachRDD(new Function<JavaPairRDD<String,String>,Void>(){
			public Void call(JavaPairRDD<String, String> arg0) throws Exception {
			JavaPairRDD<ImmutableBytesWritable,Put> hbaseput=arg0.mapToPair(new HbaseConvertToPut());
			JobConf jobConf = new HbaseLoader().HbaseConnection();
			hbaseput.saveAsNewAPIHadoopDataset(jobConf);
				return null;
			}});
		records.print();
		jsc.start();
		jsc.awaitTermination();
		jsc.close();

}
}
