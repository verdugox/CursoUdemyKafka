package com.devs4j.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Devs4jProducer {

    public static final Logger log = LoggerFactory.getLogger(Devs4jProducer.class);
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");//Broker de kafka al que nos vamos a conectar
        props.put("acks","all");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        try (Producer<String, String> producer = new KafkaProducer<>(props);)
        {
            for(int i=0; i<10000; i++){
                producer.send(new ProducerRecord<String, String>("devs4j-topic", String.valueOf(i),"devs4j-value"))
                        .get();
            }
            producer.flush();

        }catch (InterruptedException | ExecutionException e){
            log.error("Message producer interrupted ", e);
        }




    }

}
