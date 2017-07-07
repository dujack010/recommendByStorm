package com.storm.nb.redis;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.ITuple;

/**
 * Created by DuJunchen on 2017/4/25.
 */
public class MyRedisStoreMapper implements RedisStoreMapper {
    private RedisDataTypeDescription description;
    private final String hashKey = "djcTest";

    public MyRedisStoreMapper(){
        description = new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.HASH);
    }

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return description;
    }

    @Override
    public String getKeyFromTuple(ITuple iTuple) {
        return iTuple.getStringByField("redisKey");
    }

    @Override
    public String getValueFromTuple(ITuple iTuple) {
        return iTuple.getStringByField("redisValue");
    }
}
