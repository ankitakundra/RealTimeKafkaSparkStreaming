package com.spark.machine.simulation.machinedata;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer {
	 
	  public static void main(String[] args) {
	    Properties props = new Properties();
	    props.put("bootstrap.servers", "localhost:9092");
	    props.put("group.id", "test");
	    //props.put("enable.auto.commit", "true");
	    //props.put("auto.commit.interval.ms", "1000");
	    //props.put("session.timeout.ms", "30000");
	    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    props.put("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
	 
	    KafkaConsumer<String, Integer> kafkaConsumer = new KafkaConsumer(props);
	    kafkaConsumer.subscribe(Arrays.asList("machinedata"));
	    while (true) {
	      ConsumerRecords<String, Integer> records = kafkaConsumer.poll(100);
	      for (ConsumerRecord<String, Integer> record : records) {
	        System.out.printf("key = %s, value = %d", record.key(), record.value());
	        System.out.println();
	      }
	    }
	 
	  }
	 
	}