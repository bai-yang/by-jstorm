package com.a.eye.by.jstorm.kafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCountTopology {

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new WorkCountSpout(), 1);
        builder.setBolt("add", new WordAddBolt(), 2).localOrShuffleGrouping("spout");
        builder.setBolt("count", new WordCountBolt(), 2).fieldsGrouping("add", new Fields("wordAdd"));
        builder.setBolt("total", new WordReportBolt(), 1).globalGrouping("count");

        LocalCluster cluster = new LocalCluster();

        Config conf = new Config();

        cluster.submitTopology("SplitMerge", conf, builder.createTopology());

        try {
            Thread.sleep(4000 * 6);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        cluster.killTopology("SplitMerge");

        cluster.shutdown();

    }

}
