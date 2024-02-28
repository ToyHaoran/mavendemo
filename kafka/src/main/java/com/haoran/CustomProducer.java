package com.haoran;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CustomProducer {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        // 1. 创建kafka生产者的配置对象
        Properties properties = new Properties();

        // 2. 给kafka配置对象添加配置信息：bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop100:9092");

        // key,value序列化(必须)：key.serializer，value.serializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 设置事务id(必须)，可任意起名
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction_id_0");
        // 3. 创建kafka生产者对象 ("", "hello")
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        // 初始化事务
        kafkaProducer.initTransactions();
        // 开启事务
        kafkaProducer.beginTransaction();
        try {
            // 4. 调用send方法,发送消息
            for (int i = 0; i < 10; i++) {
                kafkaProducer.send(new ProducerRecord<>("first","helloworld " + i));
            }
            int i = 1/0;  // 事务提交前失败，一条记录也没有
            // 提交事务
            kafkaProducer.commitTransaction();
        }catch (Exception e){
            // 终止事务
            kafkaProducer.abortTransaction();
        }finally {
            // 5. 关闭资源
            kafkaProducer.close();
        }
    }
}
