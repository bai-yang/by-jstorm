package com.a.eye.by.jstorm.hbase;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class HBaseTopology {

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new HBaseSpout(), 1);

        builder.setBolt("bolt", new StormHBaseBolt(), 2).localOrShuffleGrouping("spout");

        Config conf = new Config();

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("stormHbaseTopology", conf, builder.createTopology());

        try {
            Thread.sleep(60000);
        } catch (Exception exception) {
            System.out.println("Thread interrupted exception : " + exception);
        }
        System.out.println("Stopped Called : ");
        cluster.killTopology("StormHBaseTopology");
        cluster.shutdown();

    }

}
