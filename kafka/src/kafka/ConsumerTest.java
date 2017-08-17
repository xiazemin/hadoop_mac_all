package kafka;
import java.util.Properties;
import java.awt.List;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import kafka.serializer.StringDecoder;
import kafka.server.KafkaConfig;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerConnector;
import kafka.utils.VerifiableProperties;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.common.Config;
import kafka.consumer.Consumer;

public class ConsumerTest {
	public static void main(String[] args) throws Exception {
//	      if(args.length == 0){
//	         System.out.println("Enter topic name");
//	         return;
//	      }
//	      //Kafka consumer configuration settings
//	      String topicName = args[0].toString();
		 String topicName="Multibrokerapplication";
	      Properties props = new Properties();
	      
	      props.put("bootstrap.servers", "localhost:9092");
	      props.put("auto.offset.reset", "smallest"); //必须要加要读旧数据
	      props.put("group.id", "test");
	      props.put("enable.auto.commit", "true");
	      props.put("auto.commit.interval.ms", "1000");
	      props.put("session.timeout.ms", "30000");
	      props.put("key.deserializer", 
	         "org.apache.kafka.common.serialization.StringDeserializer");
	      props.put("value.deserializer", 
	         "org.apache.kafka.common.serialization.StringDeserializer");
	      props.put("partition.assignment.strategy", "range");
	      KafkaConsumer<String, String> consumer = new KafkaConsumer
	         <String, String>(props);
	      
	      //Kafka Consumer subscribes list of topics here.
	      consumer.subscribe(topicName);//Arrays.asList(topicName))
	      //consumer.subscribe(Arrays.asList(topicName,"test"));

	      //print the topic name
	      System.out.println("Subscribed to topic "+topicName);
	      int i = 0;
	      
	      while (true) {
	        Map<String,ConsumerRecords<String, String>> records = consumer.poll(100000);
	      //  System.out.printf(records.toString()); 
	        try{
	        	for (ConsumerRecords<String, String> record: records.values()){
           if(record!=null){
	        	System.out.println(record.toString());
           }else{
        	   System.out.println("no message");
        	  
           }
	        	}
	        }catch (Exception e) {
				// TODO: handle exception
          	  System.out.println(e.toString());
          	 break;
			}
	    
	         
	         // print the offset,key and value for the consumer records.
//	         System.out.printf("offset = %d, key = %s, value = %s\n", 
//	            record.offset(), record.key(), record.value());
	        consumer.close();
	      }
	     
	      
	      
	      
	      
//	      ConsumerConfig config = new ConsumerConfig(props);  
//	      
//	      ConsumerConnector  consumer = (ConsumerConnector)Consumer.createJavaConsumerConnector(config);
//	      // = kafka.consumer.Consumer.createJavaConsumerConnector(config); 
//	      Map<String, Integer> topicCountMap = new HashMap<String, Integer>();  
//	        topicCountMap.put(topicName, new Integer(1));  
//	  
//	        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());  
//	        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());  
//	  
//	        Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams((scala.collection.Map<String, Object>)topicCountMap,  
//	                keyDecoder, valueDecoder);  
//	        KafkaStream<String, String> stream = consumerMap.get("mytesttopic").get(0);  
//	        ConsumerIterator<String, String> it = stream.iterator();  
//	        while (it.hasNext())  
//	            System.out.println(it.next().message());  
 
	
//	      ConsumerConfig config = new ConsumerConfig(props); 
//	      ConsumerConnector consumer= (ConsumerConnector) kafka.consumer.Consumer.createJavaConsumerConnector(config);   
//	      // 设置Topic=>Thread Num映射关系, 构建具体的流  
//	        Map<String, Integer> topickMap = new HashMap<String, Integer>();  
//	        topickMap.put(topicName, 1);  
//	        Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = consumer.createMessageStreams((scala.collection.Map<String, Object>) topickMap);  
//	        KafkaStream<byte[], byte[]> stream = streamMap.get(topicName).get(0);  
//	        ConsumerIterator<byte[], byte[]> it = stream.iterator();  
//	        System.out.println("*********Results********");  
//	        while (it.hasNext()) {  
//	            System.err.println("get data:" + new String(it.next().message()));  
//	            try {  
//	                Thread.sleep(1000);  
//	            } catch (InterruptedException e) {  
//	                e.printStackTrace();  
//	            }  
//	        } 
	      
	}	            
}
