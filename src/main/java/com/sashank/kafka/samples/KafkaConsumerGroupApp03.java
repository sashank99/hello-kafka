package com.sashank.kafka.samples;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerGroupApp03 {

	public static void main(String[] args) {
		
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092, localhost:9093");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("group.id", "test-group");
		
		KafkaConsumer<String, String> myConsumer= new KafkaConsumer<>(properties);
		
		ArrayList<String> topics= new ArrayList<>();
		topics.add("my-big-topic");
		
		myConsumer.subscribe(topics);
		
		try{
			while(true){
				
				ConsumerRecords<String,String> records = myConsumer.poll(10);
				
				for(ConsumerRecord<String,String> record:records){
					
					System.out.println(String.format("Topic: %s, Partition %d, Value: %s",
							record.topic(), record.partition(), record.value().toUpperCase()));
					
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			myConsumer.close();
		}
		
	}

}
