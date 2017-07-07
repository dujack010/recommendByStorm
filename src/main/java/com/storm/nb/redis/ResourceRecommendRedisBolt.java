package com.storm.nb.redis;

import org.apache.commons.collections.CollectionUtils;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by DuJunchen on 2017/4/25.
 */
public class ResourceRecommendRedisBolt extends AbstractRedisBolt {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    private String redisPrefix_video_property_sort = RedisParam.R_VIDEO_PROPERTY_SORT;
    private String redisPrefix_doc_property_sort = RedisParam.R_DOC_PROPERTY_SORT;
    private String redisPrefix_case_property_sort = RedisParam.R_CASE_PROPERTY_SORT;
    private String redisPrefix_property_resource_sort = RedisParam.R_PROPERTY_RESOURCE_SORT;
    private String redisPrefix_video_dao = RedisParam.R_VIDEO_BASEINFO;
    private String redisPrefix_doc_dao = RedisParam.R_DOC_BASEINFO;
    private String redisPrefix_case_dao = RedisParam.R_CASE_BASEINFO;
    private String redisPrefix_all = RedisParam.R_CUSTOMER_RECOMMEND_RESOURCE_ALL;

    public ResourceRecommendRedisBolt(JedisPoolConfig config) {
        super(config);
    }

    @Override
    public void execute(Tuple tuple) {
        JedisCommands commands = null;
        String masterCustomerId = tuple.getStringByField("customerId");
        String resourceType = tuple.getStringByField("resourceType");
        String resourceId = tuple.getStringByField("resourceId");
        String platformId = tuple.getStringByField("platformId");
        try {
            commands = this.getInstance();
            Set<String> browseList = commands.zrevrange(masterCustomerId, 0, -1);
            // 浏览资源对应标签
            List<String[]> property_list_resource = recommendWithBrowse(resourceType, resourceId, masterCustomerId,platformId,browseList,commands);
            recommendByPropList(property_list_resource,masterCustomerId,commands);
            this.collector.ack(tuple);
        }catch (Exception e){
            this.collector.fail(tuple);
        }finally {
            if(commands!=null){
                this.returnInstance(commands);
            }
        }
    }

    private void recommendByPropList(List<String[]> property_list_resource, String masterCustomerId, JedisCommands commands) {
        int recommend_case_num = 0;
        int recommend_video_num = 0;
        int recommend_doc_num = 0;
        //int recommend_topic_num = 0;
        int all_num = 0;
        if (!CollectionUtils.isEmpty(property_list_resource)) {
            logger.info("Recommend debug recommend item  num property_list_resource=" + property_list_resource.size());
            for (String[] s : property_list_resource) {
                int resourceType = Integer.parseInt(s[0]);
                String resourceId = s[1];
                logger.info("Recommend debug recommend item  num " + masterCustomerId + " s=" + s[0]);
                switch (resourceType) {
                    case 1:
                        recommend_video_num++;
                        if (recommend_video_num > 5) {
                            continue;
                        } else {
                            logger.info("Recommend debug recommend item  num " + masterCustomerId + " recommend_video_num=" + recommend_video_num);
                        }
                        break;
                    case 2:
                        recommend_doc_num++;
                        if (recommend_doc_num > 5) {
                            continue;
                        } else {
                            logger.info("Recommend debug recommend item  " + masterCustomerId + " recommend_doc_num=" + recommend_doc_num);
                        }
                        break;
                        /*case 4:
                            recommend_topic_num++;
                            prefix = redisPrefix_topic
                            if (recommend_topic_num > 5) {
                                continue;
                            } else {
                                logger.info("Recommend debug recommend item  num " + masterCustomerId + " recommend_topic_num=" + recommend_topic_num);
                            }
                            break;*/
                    case 7:
                        recommend_case_num++;
                        if (recommend_case_num > 5) {
                            continue;
                        } else {
                            logger.info("Recommend debug recommend item  num " + masterCustomerId + " recommend_case_num=" + recommend_case_num);
                        }
                        break;
                    default:
                }
                if (all_num >= 4 * 5) {
                    break;
                } else {
                    logger.info("Recommend debug recommend item  num " + masterCustomerId + "all_num=" + all_num);
                    collector.emit("RecommendedResourceSave",new Values(masterCustomerId,resourceId,resourceType+""));
                    all_num++;
                }
            }
            logger.info("Recommend debug recommend item  all_num=" + all_num);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("resourceRecommendResult",new Fields("customerId","resourceId","resourceType"));
    }

    private List<String[]> recommendWithBrowse(String resourceType, String resourceId, String masterCustomerId,String platformId, Set<String> browseList, JedisCommands commands){
        List<String[]> list = new ArrayList();
        try {
            String resourcePlatformId="1";//默认资源为骨科资源
            StringBuffer propertyIdList = new StringBuffer("");
            // 根据resource_id,resource_type确定key
            // 从redis取property_id,标签treeLevel确定顺序
            Set<String> s = null;
            switch (Integer.parseInt(resourceType)) {
                case 1:
                    s = commands.zrevrange(redisPrefix_video_property_sort + resourceId, 0, -1);
                    break;
                case 2:
                    s = commands.zrevrange(redisPrefix_doc_property_sort + resourceId, 0, -1);
                    break;
                /*
			    * case 4: s = super.zrevrange(redisPrefix_topic_property_sort +
			    * resourceId, 0, -1); break;
			    */
                case 7:
                    s = commands.zrevrange(redisPrefix_case_property_sort + resourceId, 0, -1);
                    break;
                default:
                    break;
            }

            if (s != null) {
                logger.info("Recommend debug resourceId=" + resourceId + ";;;  resourceType=" + resourceType);
                logger.info("Recommend debug propertyId.size=" + s.size());
                int all_num = 0;
                int recommend_case_num = 0;
                int recommend_video_num = 0;
                int recommend_doc_num = 0;
                for (String propertyId : s) {
                    // 根据property_id从redis顺序获取resource_id；
                    if (all_num >= 30) {// 为了计算速度问题，先找出不重复的30个。
                        break;
                    }
                    Set<String> type_resourceList = commands.zrevrange(redisPrefix_property_resource_sort + propertyId, 0,-1);
                    if (type_resourceList != null) {
                        for (String type_resource : type_resourceList) {
                            String[] Item = type_resource.split("_");
                            if (Item != null && Item.length == 2) {
                                if (resourceType.compareTo(Item[0]) == 0 && resourceId.compareTo(Item[1]) == 0) {
                                    System.out.println("排除自身");
                                } else {
                                    String isValid = "";
                                    String tplPath = "";
                                    // 资源有效无效进行判断 去redis查询是否是有效的
                                    switch (Integer.parseInt(Item[0])) {
                                        case 1:
                                            if (recommend_video_num >= 10) {
                                                continue;
                                            }
                                            isValid = commands.get(redisPrefix_video_dao + Item[1] + ":" + "isValid");
                                            resourcePlatformId = commands.get(redisPrefix_video_dao + Item[1] + ":" + "platformId");
                                            break;
                                        case 2:
                                            if (recommend_doc_num >= 10) {
                                                continue;
                                            }
                                            isValid = commands.get(redisPrefix_doc_dao + Item[1] + ":" + "isValid");
                                            tplPath = commands.get(redisPrefix_doc_dao + Item[1] + ":" + "tplPath");
                                            resourcePlatformId = commands.get(redisPrefix_doc_dao + Item[1] + ":" + "platformId");
                                            break;
                                        /*
									    * case 4: if (recommend_topic_num>=10){
									    * continue; } isValid =
									    * topicRedisDAO.getByField( Item[1],
									    * "isValid"); break;
									    */
                                        case 7:
                                            if (recommend_case_num >= 10) {
                                                continue;
                                            }
                                            isValid = commands.get(redisPrefix_case_dao + Item[1] + ":" + "isValid");
                                            resourcePlatformId = commands.get(redisPrefix_case_dao + Item[1] + ":" + "platformId");
                                            break;
                                        default:
                                    }

                                    logger.info("=====recommend===mc====platformId"+platformId+"=====customerId:" + masterCustomerId +"==resourcePlatformId=" +resourcePlatformId+"===="+resourcePlatformId);
                                    if ("1".equals(isValid)
                                            && tplPath.compareToIgnoreCase("80")!=0  //第三方joa不推荐
                                            && resourcePlatformId!=null && resourcePlatformId.contains(platformId)) {//判断资源是否符合人所属的platform

                                        logger.info("Recommend debug recommend type_resource=" + type_resource);
                                        String[] add_s = new String[2];
                                        add_s[0] = Item[0];
                                        add_s[1] = Item[1];
                                        String key = redisPrefix_all + masterCustomerId;
                                        String member = resourceType + "_" + Item[1];
                                        Double d = commands.zscore(key, member);
                                        logger.info("=====tuijian===mc====resourceType"+resourceType+"==Item[1]"+Item[1]+"==d="+d);
                                        if (d != null && d > 0) {

                                        } else {
                                            // 判断是否浏览
                                            Boolean isBrowse = Boolean.FALSE;
                                            if (browseList != null) {
                                                for (String browse : browseList) {
                                                    String[] browseItem = browse.split("_");
                                                    if (browseItem != null && browseItem.length == 2
                                                            && browseItem[1].equalsIgnoreCase(Item[1])) {
                                                        isBrowse = Boolean.TRUE;
                                                        break;
                                                    }
                                                }
                                            }
                                            // 为了计算速度问题，先找出不重复的30个。
                                            if (all_num < 30 && !isBrowse) {
                                                list.add(add_s);
                                                all_num++;
                                                switch (Integer.parseInt(Item[0])) {
                                                    case 1:
                                                        recommend_video_num++;
                                                        break;
                                                    case 2:
                                                        recommend_doc_num++;
                                                        break;
                                                    /*
												    * case 4:
												    * recommend_topic_num++; break;
												    */
                                                    case 7:
                                                        recommend_case_num++;
                                                        break;
                                                    default:
                                                        break;
                                                }
                                            } else {
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                logger.info("Recommend debug list.size=" + list.size());
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            return list;
        }
    }
}
