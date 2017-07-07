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
public class TagRecommendRedisBolt extends AbstractRedisBolt {
    Logger logger = LoggerFactory.getLogger(TagRecommendRedisBolt.class);
    private String redisPrefix_tag_baseinfo = RedisParam.R_TAG_BASEINFO;// 资源属性根目录
    private String customerRecommendTagPrefix = RedisParam.R_CUSTOMER_RECOMMEND_TAG;//

    public TagRecommendRedisBolt(JedisPoolConfig config) {
        super(config);
    }

    @Override
    public void execute(Tuple tuple) {
        JedisCommands commands = null;
        String customerId = tuple.getStringByField("customerId");
        String propertyId = String.valueOf(tuple.getLongByField("propertyId"));
        String platformId = tuple.getStringByField("platformId");
        try {
            commands = this.getInstance();
            if(getIsExistResource(propertyId,commands)){
                if("0".equals(platformId)){ //为0表示推荐兄弟标签，不校验平台id
                    saveResource(customerId,propertyId,commands);
                }else{ //推送标签自身及父类标签时需推送同平台id的标签
                    if(platformId.equals(commands.get(redisPrefix_tag_baseinfo + propertyId + ":" + "platformId"))){
                        saveResource(customerId,propertyId,commands);
                    }
                }
            }
            logger.info("标签保存成功");
            this.collector.ack(tuple);
        }catch (Exception e){
            this.collector.fail(tuple);
            logger.error("标签保存失败");
        }finally {
            if(commands!=null){
                this.returnInstance(commands);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private boolean getIsExistResource(String propertyId, JedisCommands commands) {
        boolean flag = false;
        if (propertyId != null) {
            String videoNum = commands.get(redisPrefix_tag_baseinfo + propertyId + ":" + "videoNum");
            String docNum = commands.get(redisPrefix_tag_baseinfo + propertyId + ":" + "docNum");
            String topicNum = commands.get(redisPrefix_tag_baseinfo + propertyId + ":" + "topicNum");
            String caseNum = commands.get(redisPrefix_tag_baseinfo + propertyId + ":" + "caseNum");
            videoNum=videoNum != "" && videoNum != null ? videoNum :"0";
            docNum=docNum != "" && docNum != null ? docNum :"0";
            topicNum=topicNum != "" && topicNum != null ? topicNum :"0";
            caseNum=caseNum != "" && caseNum != null ? caseNum :"0";
            if ((Integer.parseInt(videoNum) +
                    Integer.parseInt(docNum) +
                    Integer.parseInt(topicNum) +
                    Integer.parseInt(caseNum))>0){
                flag = true;
            }
        } else {
            flag = false;
        }
        return flag;
    }

    private void saveResource(String customerId, String propertyId, JedisCommands commands){
        Double zscore = commands.zscore(customerRecommendTagPrefix + customerId, propertyId);
        if(zscore==null){
            commands.zincrby(customerRecommendTagPrefix+customerId,System.currentTimeMillis(),propertyId);
        }
    }
}
