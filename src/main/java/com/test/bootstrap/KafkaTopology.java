package com.test.bootstrap;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import com.test.bolt.HelloBolt;
import com.test.bolt.MyKafkaBolt;
import com.test.bolt.TimerBolt;
import com.test.common.MessageScheme;
import com.test.spout.HelloSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;


public class KafkaTopology {


    private static final Logger log = LoggerFactory.getLogger(KafkaTopology.class);


    public static void main(String[] args) throws Exception {
        // 动态发现kafka broker地址
        BrokerHosts brokerHosts = new ZkHosts("localhost:2181/ubasKafka");
       // SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "kafkaChannelTs2", "/ubasKafka", "ubasKafkaspout0_8");
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "kafkaChannelTs1Parttion", "/ubasKafka", "ubasKafkaspout0_8_2");
        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());

        TopologyBuilder builder = new TopologyBuilder();
        //to most performance, should be equal to the number of kafka's partition
        int hint = 1;

        builder.setSpout("spoutKafka", new KafkaSpout(spoutConfig), 4);

        builder.setBolt("bolt00kafka", new MyKafkaBolt(),1)
                .shuffleGrouping("spoutKafka");
        builder.setBolt("timerBolt", new TimerBolt(),1);

        Config conf = new Config();
        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 1);
        conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM,100);
        conf.put(Config.TOPOLOGY_DEBUG,true);
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS,30);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(2);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            log.info("submitted topology "+args[0]);
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafkaTopo", conf, builder.createTopology());
            Utils.sleep(10000000);
            cluster.killTopology("kafkaTopo");
            cluster.shutdown();
        }
    }


}