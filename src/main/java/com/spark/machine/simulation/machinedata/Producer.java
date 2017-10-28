package com.spark.machine.simulation.machinedata;

import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {

	public static void main(String args[]) throws Exception {
		Logger LOGGER = LoggerFactory.getLogger(Producer.class);
		String topicName = "machinedata";
		String names[] = { "EVE-100-WALL-E", "HARLIE-189-MQ-TY",
				"SOPHIE-143-SPD", "SHROUD-560-V" };
		Random r = new Random();
		int low = 20;
		int high = 200;
		int i = 1;
		int counter =1;
		// Configure the Producer
		Properties configProperties = new Properties();
		configProperties.put("bootstrap.servers", "localhost:9092");
		//configProperties.put("acks", "all");
		//configProperties.put("retries", 0);
		//configProperties.put("batch.size", 16384);
		//configProperties.put("linger.ms", 1);
		//configProperties.put("buffer.memory", 33554432);
		configProperties.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		configProperties.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		org.apache.kafka.clients.producer.Producer producer = null;
		try {
			producer = new KafkaProducer<String, String>(configProperties);
			while (i > 0) {
				Thread.sleep(2000);
				Integer temperature = r.nextInt(high - low) + low;
				String machine_id = RandomStringUtils.randomAlphanumeric(6);
				int index = r.nextInt(names.length);
				String machine_names = names[index];
				String data = counter+","+machine_names + ","
						+ temperature;
				// ProducerRecord<String, String> rec = new
				// ProducerRecord<String, String>(topicName, data);
				producer.send(new ProducerRecord<String, String>(topicName,machine_id
						,data));
				System.out.println("message sent successfully "+data);
				counter++;
			}
		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			producer.close();
		}
		producer.close();
	}
}