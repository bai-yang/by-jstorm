package com.a.eye.by.jstorm.redis;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class StormRedisBolt implements IBasicBolt {

    private static final long serialVersionUID = -226107288355550116L;

    private RedisOperations redisOperations = null;
    private String redisIP = null;
    private int port;

    public StormRedisBolt(String redisIP, int port) {
        this.redisIP = redisIP;
        this.port = port;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        redisOperations = new RedisOperations(this.redisIP, this.port);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Map<String, Object> record = new HashMap<String, Object>();
        record.put("firstName", input.getValueByField("firstName"));
        record.put("lastName", input.getValueByField("lastName"));
        record.put("companyName", input.getValueByField("companyName"));

        redisOperations.insert(UUID.randomUUID().toString(), record);
    }

    @Override
    public void cleanup() {

    }

}
