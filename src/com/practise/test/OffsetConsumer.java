package com.practise.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class OffsetConsumer {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		String bootstrapServers = "<server>";
		String groupId = "test";
		String topicName = "<topic name>";
		int totalPartitions = 32;

		Properties configProperties = new Properties();
		configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

		/*
		 * CODE TO READ FROM THE GIVEN TOPIC
		 * 
		 */

		try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(configProperties);) {
			kafkaConsumer.subscribe(Arrays.asList(topicName));
			while (true) {
				for (int i = 0; i < totalPartitions; i++) {
					kafkaConsumer.poll(100);
					kafkaConsumer.seekToEnd(Collections.singletonList(new TopicPartition(topicName, i)));
					long currentOffset = kafkaConsumer.position(new TopicPartition(topicName, i)) - 1;
					System.out.println("Partition: " + i + " Last Offset: " + currentOffset);
				}
				System.out.println("**********************************");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(configProperties);) {
			kafkaConsumer.subscribe(Arrays.asList(topicName)); // while(true) {
			ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
			for (TopicPartition partition : records.partitions()) {
				List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);

				for (ConsumerRecord<String, String> record : partitionRecords) {
					System.out.println(record.offset() + " :" + record.partition());
					
				}

				long startOffset = partitionRecords.get(0).offset();
				long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
				System.out.println(
						"Partition: " + partition + "- StartOffset: " + startOffset + " LastOffset: " + lastOffset);
				kafkaConsumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
				System.out.println("********************");
			} // }

		}

		/*
		 * CODE TO GET THE BEGINEEING AND END OFFSET
		 * 
		 */

		try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(configProperties);) {
			kafkaConsumer.subscribe(Arrays.asList(topicName));
			Map<TopicPartition, Long> res = new HashMap<TopicPartition, Long>(); 
			// while(true) {

			ConsumerRecords<String, String> records = kafkaConsumer.poll(Integer.MAX_VALUE);
			Collection<TopicPartition> newPartitions = new ArrayList<>();
			for (int i = 0; i < totalPartitions; i++) {
				newPartitions.add(new TopicPartition(topicName, i));
			}
			res = kafkaConsumer.beginningOffsets(newPartitions);
			System.out.println("-----Beginning offset------");
			for (Map.Entry<TopicPartition, Long> entry : res.entrySet()) {
				System.out.println(entry.getKey() + " " + entry.getValue());
			}
			res.clear();
			res = kafkaConsumer.endOffsets(newPartitions);

			System.out.println("-----Ending offset------");
			for (Map.Entry<TopicPartition, Long> entry : res.entrySet()) {
				System.out.println(entry.getKey() + " " + entry.getValue());
			}
			
		}

		try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(configProperties);) {
			kafkaConsumer.subscribe(Arrays.asList(topicName));
			Map<TopicPartition, Long> res = new HashMap<>();
			Collection<TopicPartition> newPartitions = new ArrayList<>();
			long endOffset = 0;

			newPartitions.add(new TopicPartition(topicName, 0));
			while (true) {

				res = kafkaConsumer.beginningOffsets(newPartitions);
				System.out.println("-----Beginning offset------");
				for (Map.Entry<TopicPartition, Long> entry : res.entrySet()) {
					System.out.println(entry.getKey() + " " + entry.getValue());
				}
				res.clear();
				res = kafkaConsumer.endOffsets(newPartitions);

				System.out.println("-----Ending offset------");
				for (Map.Entry<TopicPartition, Long> entry : res.entrySet()) {
					endOffset = entry.getValue();
					System.out.println(entry.getKey() + " " + entry.getValue());
				}

				ConsumerRecords<String, String> records = kafkaConsumer.poll(Integer.MAX_VALUE);
				records.forEach(record -> {
					System.out.println(record.offset() + " : " + record.value());
				});
				kafkaConsumer.seek(new TopicPartition(topicName, 0), endOffset);
			
			}
		}

	}

}
