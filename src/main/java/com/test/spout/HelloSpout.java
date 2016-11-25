/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.test.spout;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.server.UID;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


public class HelloSpout extends BaseRichSpout {
    public static Logger LOG = LoggerFactory.getLogger(HelloSpout.class);
    boolean _isDistributed;
    SpoutOutputCollector _collector;

    public HelloSpout() {
        this(true);
    }

    public HelloSpout(boolean isDistributed) {
        _isDistributed = isDistributed;
    }

    @Override
    public void activate() {
        LOG.debug("activate:"+"BEGIN");
        super.activate();
        LOG.debug("activate:"+"end");
    }

    @Override
    public void deactivate() {
        LOG.debug("deactivate:"+"BEGIN");
        super.deactivate();
        LOG.debug("deactivate:"+"end");
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        LOG.debug("open:"+"BEGIN");
        _collector = collector;
        LOG.debug("open:"+"end");
    }

    public void close() {
        LOG.debug("close:"+"BEGIN");
        LOG.debug("close:"+"end");
    }

    public void nextTuple() {
        LOG.info("emit message tree:"+"BEGIN");
        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 可靠发射
        _collector.emit(new Values("hello"), UUID.randomUUID().toString());
        // 不可靠发射
        // _collector.emit(new Values("hello"));
        LOG.info("emit message tree:"+"END");
    }


    @Override
    public void ack(Object msgId) {
        LOG.debug("ack:"+"BEGIN");
        LOG.debug("ack:"+"end");

    }

    @Override
    public void fail(Object msgId) {
        LOG.debug("fail:"+"BEGIN");
        LOG.debug("fail:"+"end");

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("helloFiled00"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        if (!_isDistributed) {
            Map<String, Object> ret = new HashMap<String, Object>();
            // ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 2);
            return ret;
        } else {
            return null;
        }
    }
}
