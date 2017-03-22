package com.a.eye.by.jstorm.kafka;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class WordReportBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        Map<String, Integer> map = (Map<String, Integer>) input.getValue(1);
        System.out.println("word=" + word + ", count=" + map.get(word));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
