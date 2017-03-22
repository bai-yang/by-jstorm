package com.a.eye.by.jstorm.kafka;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountBolt extends BaseRichBolt {

    private static final long serialVersionUID = -773955601879296459L;

    private OutputCollector collector;

    private Map<String, Integer> countMap = new HashMap<String, Integer>();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getString(0);
        System.out.println("----count--------==" + word);
        if (countMap.containsKey(word)) {
            Integer count = countMap.get(word).intValue() + 1;
            countMap.put(word, count);
        } else {
            countMap.put(word, 1);
        }

        collector.emit(new Values(word, countMap));

        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

    @Override
    public void cleanup() {
        /*
         * System.out.println("-----------------------------------------"); for (Map.Entry<String, Integer> entry :
         * countMap.entrySet()) { System.out.println("================================" + entry.getKey() + ": " +
         * entry.getValue()); } countMap.clear();
         */
    }

}
