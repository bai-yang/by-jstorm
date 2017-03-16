package com.a.eye.by.jstorm.word;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class WordCount implements IRichBolt {

    OutputCollector collector;

    private Map<String, Integer> map = new HashMap<String, Integer>();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String str = input.getString(0);

        System.out.println("count==========" + str);

        if (!map.containsKey(str)) {
            map.put(str, 1);
        } else {
            Integer c = map.get(str) + 1;
            map.put(str, c);
        }
    }

    @Override
    public void cleanup() {
        System.out.println("------------------------------------");
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

}
