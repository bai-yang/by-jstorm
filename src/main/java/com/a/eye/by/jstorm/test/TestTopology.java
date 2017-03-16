package com.a.eye.by.jstorm.test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class TestTopology {

    private static final String SPOUT_ID = "spout_id";
    private static final String BOLT_ID = "bolt_id";
    private static final String TOPOLOGY_NAME = "topology_name";

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT_ID, new TestSpout());

        builder.setBolt(BOLT_ID, new TestBolt(), 1).localOrShuffleGrouping(SPOUT_ID);

        Config conf = new Config();

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        cluster.killTopology(TOPOLOGY_NAME);

        cluster.shutdown();
    }

}
