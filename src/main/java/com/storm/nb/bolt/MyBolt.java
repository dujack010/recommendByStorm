package com.storm.nb.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by DuJunchen on 2017/3/21.
 */
public class MyBolt extends BaseBasicBolt {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    public static AtomicInteger count = new AtomicInteger(0);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String string = tuple.getString(0);
        System.out.println(string);
        String[] split = string.split("/");
        List strings = Arrays.asList(count.getAndIncrement()+"", split[1]);
        collector.emit(strings);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id","customerId"));
    }
}
