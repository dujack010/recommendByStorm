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
public class CustomerRecommendRedisBolt extends AbstractRedisBolt {
    Logger logger = LoggerFactory.getLogger(this.getClass());

    public CustomerRecommendRedisBolt(JedisPoolConfig config) {
        super(config);
    }

    @Override
    public void execute(Tuple tuple) {
        //Jedis jedis = null;
        JedisCommands commands = null;
        String idList = tuple.getStringByField("idList");
        String[] split = idList.split(",");
        String customerId = tuple.getStringByField("customerId");
        int level = Integer.parseInt(tuple.getStringByField("level"));
        try {
            //批量方法
            /*jedis = (Jedis) this.getInstance();
            Pipeline pipelined = jedis.pipelined();
            for (String s : split) {
                pipelined.zadd(customerId,System.currentTimeMillis(),s);
            }
            pipelined.sync();*/
            //逐条方法
            commands = this.getInstance();
            for (String s : split) {
                //为防止推荐列表乱序，推荐人一旦进入表中则不能修改得分值
                Double zscore = commands.zscore(customerId, s);
                if(zscore==null){ //当列表中没有该用户时新增
                    zscore = Double.valueOf(System.currentTimeMillis());
                    switch(level){//根据推荐匹配度适当增加优先级
                        case 1: zscore += 10000;
                        case 2: zscore += 5000;
                        case 3: zscore += 1000;
                        case 0: zscore -= 1000;
                        default:
                    }
                    commands.zincrby(customerId,zscore,s);
                }
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
}
