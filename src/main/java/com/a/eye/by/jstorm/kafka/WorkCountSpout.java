package com.a.eye.by.jstorm.kafka;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WorkCountSpout extends BaseRichSpout {

    private static final long serialVersionUID = -127820650805888567L;
    
    private final static String CONFIG_FILE = "/kafka-consumer.properties";

    private SpoutOutputCollector collector;

    private ConsumerConnector consumer;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        Properties props = new Properties();
        try {
            props.load(getClass().getResourceAsStream(CONFIG_FILE));
        } catch (IOException e) {
            e.printStackTrace();
        }
        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

    }

    @Override
    public void nextTuple() {

        Map<String, Integer> topicMap = new HashMap<String, Integer>();

        topicMap.put("test", 1);
        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<String, String>>> consumerMap =
                consumer.createMessageStreams(topicMap, keyDecoder, valueDecoder);

        KafkaStream<String, String> stream = consumerMap.get("test").get(0);

        ConsumerIterator<String, String> it = stream.iterator();

        while (it.hasNext()) {
            collector.emit(new Values(it.next().message()), it.next().message());
        }

        /*
         * collector.emit(new Values("hello")); collector.emit(new Values("world")); collector.emit(new Values("test"));
         * 
         * try { Thread.sleep(1*100); } catch (InterruptedException e) { e.printStackTrace(); }
         */
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

}
