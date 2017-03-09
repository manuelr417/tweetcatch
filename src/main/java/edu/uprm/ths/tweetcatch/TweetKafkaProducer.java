package edu.uprm.ths.tweetcatch;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

/**
 * Created by manuel on 2/18/17.
 */
public class TweetKafkaProducer {

    private static final TweetKafkaProducer theInstance = new TweetKafkaProducer();

    public static TweetKafkaProducer getInstance(){
        return TweetKafkaProducer.theInstance;
    }

    private Producer<String, String> kafkaProducer;

    private TweetKafkaProducer(){

        Properties props = new Properties();
        props.put("bootstrap.servers", "node05.ece.uprm.edu:9092,node06.ece.uprm.edu:9092");
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.kafkaProducer = new KafkaProducer<String, String>(props);
    }

    public Producer<String, String> getProducer(){
        return this.kafkaProducer;
    }

}
