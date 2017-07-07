package com.storm.nb.recommand.datainit;

import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.Set;

/**
 * Created by DuJunchen on 2017/5/8.
 * 根据浏览的资源id及资源类型取出该资源所有的标签
 */
public class GetResourceTag extends AbstractRedisBolt {

    private static final String prefix = "recommend:customer:score:tag:";

    public GetResourceTag(JedisPoolConfig config) {
        super(config);
    }

    @Override
    public void execute(Tuple tuple) {
        Jedis commands = null;
        String redisKey = tuple.getStringByField("redisKey");
        String userId = tuple.getStringByField("userId");
        try {
            commands = (Jedis)this.getInstance();
            Set<String> propertyIdSet = commands.zrevrange(redisKey, 0, -1);
            Pipeline pipelined = commands.pipelined();
            for (String propId : propertyIdSet) {
                pipelined.zadd(prefix+userId,0.05,propId);
            }
            pipelined.sync();
            pipelined.shutdown();
        }catch (Exception e){
            e.printStackTrace();
        } finally {
            returnInstance(commands);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("value"));
    }
}
