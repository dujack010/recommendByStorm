package com.storm.nb.redis;

import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;

/**
 * Created by DuJunchen on 2017/4/25.
 */
public class ResourceCreateRedisBolt extends AbstractRedisBolt {
    Logger logger = LoggerFactory.getLogger(ResourceCreateRedisBolt.class);
    private String redisPrefix_all = RedisParam.R_CUSTOMER_RECOMMEND_RESOURCE_ALL;
    private String redisPrefix_video = RedisParam.R_CUSTOMER_RECOMMEND_RESOURCE_VIDEO;
    private String redisPrefix_doc = RedisParam.R_CUSTOMER_RECOMMEND_RESOURCE_DOC;
    private String redisPrefix_topic = RedisParam.R_CUSTOMER_RECOMMEND_RESOURCE_TOPIC;
    private String redisPrefix_case = RedisParam.R_CUSTOMER_RECOMMEND_RESOURCE_CASE;

    public ResourceCreateRedisBolt(JedisPoolConfig config) {
        super(config);
    }

    @Override
    public void execute(Tuple tuple) {
        JedisCommands commands = null;
        String customerId = tuple.getStringByField("customerId");
        String resourceType = tuple.getStringByField("resourceType");
        String resourceId = tuple.getStringByField("resourceId");
        String prefix = getPrefix(resourceType);
        try {
            commands = this.getInstance();
            //同时校验混排及单项的资源列表中有没有该资源
            String key = redisPrefix_all + customerId;
            String member = resourceType + "_" + resourceId;
            Double scoreForFullList = commands.zscore(key, member);
            if (scoreForFullList == null) {
                commands.zincrby(redisPrefix_all + customerId,System.currentTimeMillis(), resourceType+"_"+resourceId);
            }
            Double scoreForSpecList = commands.zscore(prefix + customerId, resourceId);
            if (scoreForSpecList == null) {
                commands.zincrby(prefix + customerId,System.currentTimeMillis(), resourceId);
            }
            this.collector.ack(tuple);
        }catch (Exception e){
            this.collector.fail(tuple);
        }finally {
            if(commands!=null){
                this.returnInstance(commands);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private String getPrefix(String resourceType) {
        switch (Integer.parseInt(resourceType)) {
            case 1:
                return redisPrefix_video;
            case 2:
                return redisPrefix_doc;
            case 4:
                return redisPrefix_topic;
            case 7:
                return redisPrefix_case;
            default:
                return null;
        }
    }
}
