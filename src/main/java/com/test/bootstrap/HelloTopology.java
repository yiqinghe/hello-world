package com.test.bootstrap;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import com.test.bolt.HelloBolt;
import com.test.spout.HelloSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class HelloTopology {


    private static final Logger log = LoggerFactory.getLogger(HelloTopology.class);


    public static void main(String[] args) throws Exception {



        // 配置KafkaBolt中的kafka.broker.properties
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 1);
        conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM,100);
        conf.put(Config.TOPOLOGY_DEBUG,true);
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS,30);

        TopologyBuilder builder = new TopologyBuilder();
        //to most performance, should be equal to the number of kafka's partition
        int hint = 1;

        builder.setSpout("spout00", new HelloSpout(), 1);

        builder.setBolt("bolt00", new HelloBolt(),1)
                .shuffleGrouping("spout00");

        if (args != null && args.length > 0) {
            conf.setNumWorkers(2);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            log.info("submitted topology "+args[0]);
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Topo", conf, builder.createTopology());
            Utils.sleep(1000000000);
            cluster.killTopology("Topo");
            cluster.shutdown();
        }
    }


}