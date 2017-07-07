package com.storm.nb.recommand.datainit;

import com.storm.nb.hbase.AdminHbaseBolt;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

/**
 * Created by DuJunchen on 2017/7/7.
 * 重新计算用户对某资源属性的评分
 */
public class ReScoreBolt extends AdminHbaseBolt {

    @Override
    public void execute(Tuple tuple) {
        String userId = tuple.getStringByField("userId");
        String propertyId = tuple.getStringByField("propertyId");
        Table table = this.hBaseConnection.getTable("");
        Get get = new Get(Bytes.toBytes(userId));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
