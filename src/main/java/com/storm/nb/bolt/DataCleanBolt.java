package com.storm.nb.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

/**
 * Created by DuJunchen on 2017/4/20.
 */
public class DataCleanBolt extends BaseBasicBolt{
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String string = tuple.getString(0);
        System.out.println(string);
        String[] split = string.split("\t");
        if(split.length==3){
            //collector.emit(new Values(split[0],split[1],split[2]));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashKey","key","value"));
    }
}
