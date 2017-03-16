package com.a.eye.by.jstorm.word;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordReader implements IRichSpout {

    private static final long serialVersionUID = 1L;

    SpoutOutputCollector collector;

    private boolean completed = false;

    private static List<String> words = new ArrayList<String>();

    static {
        words.add("I test my life hello");
        words.add("Love world ni hao");
        words.add("you are a good boy");
        words.add("you I a You boy");
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        
        if (completed) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {

            }
            return;
        }
        
        

        for (String word : words) {
            this.collector.emit(new Values(word), word);
        }
        completed = true;
        
  
        System.out.println("999999999999999999999999999999999999999");

        //completed = true;

    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
