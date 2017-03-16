package com.a.eye.by.jstorm.test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TestSpout extends BaseRichSpout {

    private static final long serialVersionUID = 1L;

    private SpoutOutputCollector collector;

    static AtomicInteger atomicInteger = new AtomicInteger(0);

    public void nextTuple() {
        while (true) {
            int a = atomicInteger.incrementAndGet();
            this.collector.emit(new Values("xxxxx:" + a));
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void open(Map map, TopologyContext arg1, SpoutOutputCollector arg2) {
        this.collector = collector;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("log"));
    }

}
