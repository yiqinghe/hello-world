package com.test.bolt;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class HelloBolt extends BaseRichBolt {
    private static final Logger log = LoggerFactory.getLogger(HelloBolt.class);

    private OutputCollector _collector;
    private int _counter;

    public HelloBolt() {
    }

    public void prepare(Map map, TopologyContext context, OutputCollector outputCollector) {
        _collector = outputCollector;

    }

    public void execute(Tuple tuple) {
        System.out.println("excute:"+tuple.getValue(0));
        log.info("emit message execute:BEGIN");

        /*try {
            Random random = new Random();
            Thread.sleep(Float.valueOf((random.nextFloat()*10000)).longValue());


        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
        // 带标记 发射
        //_collector.emit(tuple, new Values(tuple.getValue(0)));
        _collector.ack(tuple);
    }


    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("boltField"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
        return conf;
    }

    }


