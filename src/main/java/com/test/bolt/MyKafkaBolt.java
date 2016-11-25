package com.test.bolt;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.test.common.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class MyKafkaBolt extends BaseRichBolt {
    private static final Logger log = LoggerFactory.getLogger(MyKafkaBolt.class);

    private OutputCollector _collector;
    private int _counter;

    public MyKafkaBolt() {
    }

    public void prepare(Map map, TopologyContext context, OutputCollector outputCollector) {
        _collector = outputCollector;

    }

    public void execute(Tuple tuple) {
        // 为什么可能存在没有field的情况呢？
        if(tuple.contains(Constant.ubaseFiled)){
            String value = tuple.getStringByField(Constant.ubaseFiled);
            System.out.println("MyKafkaBolt excute:"+value);
            log.info("emit message execute:BEGIN"+value);
            _collector.ack(tuple);
        }
        else{
            log.info("emit message execute:BEGIN no field");

        }

    }


    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Constant.ubaseFiled));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        // 定时触发
        //conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
        return conf;
    }

    }


