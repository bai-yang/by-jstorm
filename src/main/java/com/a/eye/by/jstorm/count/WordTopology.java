package com.a.eye.by.jstorm.count;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.a.eye.by.jstorm.utils.JStormHelper;
import com.alibaba.jstorm.utils.JStormUtils;

public class WordTopology {

    public final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "spout.parallel";
    public final static String TOPOLOGY_SPLIT_PARALLELISM_HINT = "split.parallel";
    public final static String TOPOLOGY_COUNT_PARALLELISM_HINT = "count.parallel";
    
    private static final String TOPOLOGY_NAME = "topology_name";

    static boolean isLocal = true;
    static Config conf = JStormHelper.getConfig(null);

    static {
        conf.put(Config.STORM_CLUSTER_MODE, "local");
    }

    public static void main(String[] args) {

        int spout_Parallelism_hint = JStormUtils.parseInt(conf.get(TOPOLOGY_SPOUT_PARALLELISM_HINT), 1);
        int split_Parallelism_hint = JStormUtils.parseInt(conf.get(TOPOLOGY_SPLIT_PARALLELISM_HINT), 1);
        int count_Parallelism_hint = JStormUtils.parseInt(conf.get(TOPOLOGY_COUNT_PARALLELISM_HINT), 1);

        TopologyBuilder builder = new TopologyBuilder();

        boolean isLocalShuffle = JStormUtils.parseBoolean(conf.get("is.local.first.group"), false);

        builder.setSpout("spout", new SentenceSpout(), spout_Parallelism_hint);

        if (isLocalShuffle) {
            builder.setBolt("split", new SplitSentence(), split_Parallelism_hint).localFirstGrouping("spout");
        } else {
            builder.setBolt("split", new SplitSentence(), split_Parallelism_hint).shuffleGrouping("spout");
        }

        builder.setBolt("count", new WordCount(), count_Parallelism_hint).fieldsGrouping("split", new Fields("word"));
        
        builder.setBolt("report", new ReportBolt()).globalGrouping("count");

        isLocal = JStormHelper.localMode(conf);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());

        try {
            Thread.sleep(1000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        cluster.killTopology(TOPOLOGY_NAME);

        cluster.shutdown();
        

    }

}
