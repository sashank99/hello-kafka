package com.sashank.kafka.samples;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class KafkaConsumerAssignApp {

	public static void main(String[] args) {
		
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092, localhost:9093");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		//properties.put("group-id", "test");
		
		KafkaConsumer<String, String> myConsumer= new KafkaConsumer<>(properties);
		
		ArrayList<TopicPartition> topics= new ArrayList<>();
		TopicPartition myTopicPart0 = new TopicPartition("my-other-topic", 0);
		TopicPartition myTopicPart2 = new TopicPartition("my-other-topic1", 2);
		topics.add(myTopicPart0);
		topics.add(myTopicPart2);
		
		myConsumer.assign(topics);
		
		try{
			while(true){
				
				ConsumerRecords<String,String> records = myConsumer.poll(10);
				
				for(ConsumerRecord<String,String> record:records){
					
					System.out.println(String.format("Topic: %s, Partition %d, offset: %d, Key: %s, Value: %s",
							record.topic(), record.partition(), record.offset(), record.key(), record.value()));
					
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			myConsumer.close();
		}
		
	}

}
