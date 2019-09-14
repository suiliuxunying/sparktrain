package com.shao.spark.kafka;

import kafka.Kafka;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * kafka消费者
 */
public class KafkaConsumer extends Thread{
    private String topic ;

    public KafkaConsumer(String topic){
        this.topic=topic;

    }
    private ConsumerConnector reateConnector(){
        Properties properties=new Properties();
        properties.put("group.id",KafkaProperties.GROUP_ID);
        properties.put("zookeeper.connect",KafkaProperties.ZOOKEEPER);
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties)) ;
    }
    public void run(){
        ConsumerConnector consumerConnector = reateConnector();
        Map<String,Integer> topicCountMap =new HashMap<String, Integer>();
        topicCountMap.put(topic ,1);//topic名字 ，id
        // String:topic
        //List<KafkaStream<byte[], byte[]>>:数据流
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStream=
                consumerConnector.createMessageStreams(topicCountMap);
        KafkaStream<byte[],byte[]> stream=messageStream.get(topic).get(0);
        ConsumerIterator<byte[],byte[]> iterator=stream.iterator();

        while (iterator.hasNext()){
            String message = new String(iterator.next().message());
            System.out.println("rec:"+message);
        }
    }
}
