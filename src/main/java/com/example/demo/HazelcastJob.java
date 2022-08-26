package com.example.demo;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.*;
import org.apache.kafka.common.serialization.*;

import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class HazelcastJob {


    static final DateTimeFormatter TIME_FORMATTER =
            DateTimeFormatter.ofPattern("HH:mm:ss:SSS");

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "127.0.0.1:9092");
        props.setProperty("key.deserializer", StringDeserializer.class.getCanonicalName());
        props.setProperty("value.deserializer", StringDeserializer.class.getCanonicalName());
        props.setProperty("auto.offset.reset", "earliest");
        Pipeline p = Pipeline.create();
        p.readFrom(KafkaSources.kafka(props, "wikimedia"))
                .withoutTimestamps()
                //.writeTo(Sinks.map("changes"));
                .writeTo(Sinks.logger());


        JobConfig cfg = new JobConfig().setName("producer");
        HazelcastInstance hz = Hazelcast.bootstrappedInstance();
        hz.getJet().newJob(p, cfg);


    }


}