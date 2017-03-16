package com.a.eye.by.jstorm.word;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class WordTopology {

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout_id", new WordReader(), 1);

        builder.setBolt("spilt_bolt_id", new WordSpilt(), 1).shuffleGrouping("spout_id");

        builder.setBolt("count_bolt_id", new WordCount(), 1).shuffleGrouping("spilt_bolt_id");

        Config conf = new Config();

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("Getting-Started-1", conf, builder.createTopology());

        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        cluster.shutdown();

    }

}
