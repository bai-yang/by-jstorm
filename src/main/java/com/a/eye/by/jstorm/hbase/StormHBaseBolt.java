package com.a.eye.by.jstorm.hbase;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class StormHBaseBolt implements IRichBolt {

    private static final long serialVersionUID = -38585803152271656L;

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {

        Map<String, Map<String, Object>> record = new HashMap<String, Map<String, Object>>();

        Map<String, Object> personalMap = new HashMap<String, Object>();
        personalMap.put("firstName", input.getValueByField("firstName"));

        Map<String, Object> lastNameMap = new HashMap<String, Object>();
        personalMap.put("lastName", input.getValueByField("lastName"));

        record.put("cf1", personalMap);
        record.put("cf2", lastNameMap);

        HBaseOperations.insert("by-storm", UUID.randomUUID().toString(), record);

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
