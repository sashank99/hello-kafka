package com.sashank.kafka.samples;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaGroupProducerApp {
	
	public static void main(String[] args) {

		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092, localhost:9093");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		KafkaProducer<String,String> myProducer = new KafkaProducer<>(properties);
		
		try{
			int count=0;
			while(count<100){
				myProducer.send(new ProducerRecord<String,String>("my-big-topic","abcdefghijklmnopqrstuvwxyz"));
				count++;
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			myProducer.close();
		}
		
	}

}
