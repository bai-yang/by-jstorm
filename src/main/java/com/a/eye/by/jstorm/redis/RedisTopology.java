package com.a.eye.by.jstorm.redis;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class RedisTopology {

    public static void main(String[] args) {
        List<String> zks = new ArrayList<String>();
        zks.add("127.0.0.1");

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new SampleSpout(), 2);

        builder.setBolt("bolt", new StormRedisBolt("127.0.0.1", 6379), 2).shuffleGrouping("spout");

        Config conf = new Config();
        conf.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("StormRedisTopology", conf, builder.createTopology());

    }

}
