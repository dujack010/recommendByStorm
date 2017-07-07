package com.storm.nb.redis;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

import java.util.List;

/**
 * Created by DuJunchen on 2017/4/24.
 */
public class MyRedisLookupMapper implements RedisLookupMapper{
    @Override
    public List<Values> toTuple(ITuple iTuple, Object o) {
        return null;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return null;
    }

    @Override
    public String getKeyFromTuple(ITuple iTuple) {
        return null;
    }

    @Override
    public String getValueFromTuple(ITuple iTuple) {
        return null;
    }
}
