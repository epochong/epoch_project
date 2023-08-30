package service;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author chongwang11
 * @date 2023-03-21 13:47
 * @description
 */
public class KafkaConsumerService {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.30.41.27:9093,172.30.41.28:9093,172.30.41.29:9093");
        //props.put("bootstrap.servers", "172.30.41.123:2181,172.30.41.124:2181,172.30.41.125:2181");
        props.put("group.id", "CG_test003_upgrade_min20221011");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "min20221011");
        props.put("auto.offset.reset", "earliest");
        String username = "admin";
        String password = "admin-secret";
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username + "\"" + "password  =\"" + password + "\";");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //subscribe topic
        consumer.subscribe(Arrays.asList("datag.test_sourceinfo_jzjx_testlibrary_chongwang_copy.jzjx_testlibrary_archive_23001_chongwang.archive_absentuserarchive"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
//                System.out.println(record.key());
//                System.out.println(record.key().toLowerCase());
//                System.out.println(new String(record.key().getBytes(), StandardCharsets.UTF_8));
//                System.out.println(record.value().toString());
                System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
            }
        }
    }


}
