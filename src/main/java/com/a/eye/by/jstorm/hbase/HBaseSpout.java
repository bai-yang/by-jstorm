package com.a.eye.by.jstorm.hbase;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class HBaseSpout extends BaseRichSpout {

    private static final long serialVersionUID = 8968713737050466767L;

    private SpoutOutputCollector collector;

    private static final Map<Integer, String> FIRSTNAMEMAP = new HashMap<Integer, String>();
    static {
        FIRSTNAMEMAP.put(0, "john");
        FIRSTNAMEMAP.put(1, "nick");
        FIRSTNAMEMAP.put(2, "mick");
        FIRSTNAMEMAP.put(3, "tom");
        FIRSTNAMEMAP.put(4, "jerry");
    }

    private static final Map<Integer, String> LASTNAME = new HashMap<Integer, String>();
    static {
        LASTNAME.put(0, "anderson");
        LASTNAME.put(1, "watson");
        LASTNAME.put(2, "ponting");
        LASTNAME.put(3, "dravid");
        LASTNAME.put(4, "lara");
    }

    private static final Map<Integer, String> COMPANYNAME = new HashMap<Integer, String>();
    static {
        COMPANYNAME.put(0, "abc");
        COMPANYNAME.put(1, "dfg");
        COMPANYNAME.put(2, "pqr");
        COMPANYNAME.put(3, "ecd");
        COMPANYNAME.put(4, "awe");
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        final Random rand = new Random();
        int randomNumber = rand.nextInt(5);
        collector.emit(new Values(FIRSTNAMEMAP.get(randomNumber), LASTNAME.get(randomNumber), COMPANYNAME
                .get(randomNumber)));
        
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("firstName", "lastName", "companyName"));
    }

}
