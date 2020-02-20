package com.practise.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Consumer {
	public static void main(String[] args) {
        
        String bootstrapServers = "<server>";
        String groupId = "test";
        String topicName = "<topic name>";
        int totalPartitions = 1;
        
        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
        kafkaConsumer.subscribe(Arrays.asList(topicName));
        //Collection<TopicPartition> newPartitions = new ArrayList<>();
        TopicPartition newPartitions = (new TopicPartition(topicName, 0));

        //kafkaConsumer.assign(newPartitions);
        
        /*
        for(int i=0; i<totalPartitions; i++) {
            
        	kafkaConsumer.seekToEnd(Collections.singletonList(new TopicPartition(topicName, i)));
            kafkaConsumer.poll(0);
            long currentOffset = kafkaConsumer.position(new TopicPartition(topicName, i)) -1;
            System.out.println("Partition: " + i + "Last Offset: " + currentOffset);
        }
        */
        
        
        int batchSize = 1;
        int count = 0;
        
        String fileName = "test.log";
        
        File file = new File(fileName);

       
        // if file doesnt exists, then create it
        if (!file.exists()) {
            try {
				file.createNewFile();
			} catch (IOException e) {
				
				e.printStackTrace();
			}
        }
        

        
while(true)
{	//kafkaConsumer.seekToEnd(Collections.singleton(newPartitions));
	
	 try(FileWriter fw = new FileWriter(file.getAbsoluteFile(),true);
			  BufferedWriter bw = new BufferedWriter(fw);){
        	ConsumerRecords<String,String> records = kafkaConsumer.poll(1000);
        	
        	kafkaConsumer.seekToEnd(Collections.singleton(newPartitions));
        	long currentOffset = kafkaConsumer.position(newPartitions);
        	
        	records.forEach(data -> {
        		System.out.println(data.value());
        		try {
					bw.write( data.value() + "\n");
				} catch (IOException e) {
					e.printStackTrace();
				} 
        	});
			
			  records.partitions().forEach(record -> {
			  System.out.println("Current Offset for Partition :" + record +" "+
			  kafkaConsumer.position(record));
			  
			  });
        	
        	System.out.println("Current Offset for Partition :" + newPartitions +" "+currentOffset);
	 } catch (IOException e) {
		e.printStackTrace();
	}
        	
}
        	
        	
        	
        	/*
			 * records.forEach(val -> {
			 * System.out.println(val.offset()+": "+val.partition()+": "+val.value()
			 * +": "+val.key());} );
			 */

    }
}
