package com.shao.spark.kafka;

/**
 * kafka java api测试类
 */
public class KafkaClientApp {
    public static void main(String[] args){
        //这里只有发送数据，所以需要在服务器上手动的开启hello_topic，和是信息消费（控制台查看）
       new KafkaProducer(KafkaProperties.TOPIC).start();
        //这里加入了接收（消费）信息可以在本地控制台看到发送和接收
        new KafkaConsumer(KafkaProperties.TOPIC).start();
    }

}
