package com.storm.nb.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.storm.hbase.bolt.AbstractHBaseBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Created by DuJunchen on 2017/7/7.
 */
public abstract class AdminHbaseBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractHBaseBolt.class);
    protected OutputCollector collector;
    protected transient HBaseConnection hBaseConnection;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        Configuration hbConfig = HBaseConfiguration.create();
        this.hBaseConnection = new HBaseConnection(hbConfig);
    }

    public void cleanup() {
        try {
            this.hBaseConnection.close();
        } catch (IOException var2) {
            LOG.error("HBase Client Close Failed ", var2);
        }
    }
}
