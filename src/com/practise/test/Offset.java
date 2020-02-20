package com.practise.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Offset {
	public static void main(String[] args) {
		String bootstrapServers = "<server>";
		String groupId = "kfk_mystique_mbaku_snapshot";
		String topicName = "<topic name>";
		int totalPartitions = 32;

		Properties configProperties = new Properties();
		configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

		/*
		 * CODE TO GET THE BEGINEEING AND END OFFSET
		 * 
		 */

		try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(configProperties);) {
			kafkaConsumer.subscribe(Arrays.asList(topicName));
			Map<TopicPartition, Long> startOffset = new HashMap<TopicPartition, Long>(); 
			Map<TopicPartition, Long> endOffset = new HashMap<TopicPartition, Long>(); 


			Collection<TopicPartition> newPartitions = new ArrayList<>();
			for (int i = 0; i < totalPartitions; i++) {
				newPartitions.add(new TopicPartition(topicName, i));
			}
			startOffset = kafkaConsumer.beginningOffsets(newPartitions);
			endOffset = kafkaConsumer.endOffsets(newPartitions);
			 
			Set<TopicPartition> keys = startOffset.keySet();
			for(TopicPartition var : keys) {
				if(endOffset.get(var)- startOffset.get(var) == 0) {
					System.out.println("No data in partition: " + var );
				}else {
					continue;
				}
			}
			System.out.println("End of application");
			
			/*
			 * System.out.println("-----Beginning offset------"); for
			 * (Map.Entry<TopicPartition, Long> entry : startOffset.entrySet()) {
			 * System.out.println(entry.getKey() + " " + entry.getValue()); }
			 * startOffset.clear();
			 * 
			 * System.out.println("-----Ending offset------"); for
			 * (Map.Entry<TopicPartition, Long> entry : endOffset.entrySet()) {
			 * System.out.println(entry.getKey() + " " + entry.getValue()); }
			 */
			
			
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

}
