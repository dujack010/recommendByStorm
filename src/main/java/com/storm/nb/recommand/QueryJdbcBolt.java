package com.storm.nb.recommand;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 Created by DuJunchen on 2017/5/9.
 */


public class QueryJdbcBolt extends BaseJdbcBolt {
    private static final Logger logger = LoggerFactory.getLogger(QueryJdbcBolt.class);

    public QueryJdbcBolt withQueryTimeoutSecs(int queryTimeoutSecs) {
        this.queryTimeoutSecs = Integer.valueOf(queryTimeoutSecs);
        return this;
    }

    public QueryJdbcBolt(ConnectionProvider connectionProvider) {
        super(connectionProvider);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        System.out.println("start");
        //String sql = "select ref_id from customer_follow_resource where is_valid = 1 and follow_type = 61 and customer_id = ?";
        String sql = "SELECT property_id FROM comm_data_property WHERE is_valid = 1";
        List<List<Column>> propList = jdbcClient.select(sql, new ArrayList<Column>());
        StringBuilder s = new StringBuilder();
        for (int i = 0,j=propList.size(); i < j; i++) {
            s.append(propList.get(i).get(0).getVal());
            s.append(",");
        }
        sql = "SELECT customer_id FROM customer_auth WHERE state = 1 or state = 2";
        List<List<Column>> authList = jdbcClient.select(sql, new ArrayList<Column>());
        for (int i = 0,j=authList.size(); i < j; i++) {
            String customerId = String.valueOf(authList.get(i).get(0).getVal());
            basicOutputCollector.emit(new Values(customerId,s.toString()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declareStream("customerRecommend",new Fields("idList","customerId","level"));
//        outputFieldsDeclarer.declareStream("topicRecommendResult",new Fields("customerId","resourceId","resourceType"));
//        outputFieldsDeclarer.declareStream("resourceRecommend",new Fields("customerId","resourceId","resourceType","platformId"));
//        outputFieldsDeclarer.declareStream("tagRecommend",new Fields("customerId","propertyId","platformId"));
        outputFieldsDeclarer.declare(new Fields("id","values"));
    }

    private void topicRecommend(String customerId, String resourceType,String platformId, BasicOutputCollector collector) {
        System.out.println(platformId);
        String sql = "select topic_id from cms_topic " +
                "where is_valid = 1 and TIMESTAMPDIFF(DAY, publish_time, CURRENT_TIMESTAMP()) < 30 and platform_id = ? " +
                "order by publish_time desc,id desc " +
                "limit 0,5";
        List<List<Column>> topic_list = this.jdbcClient.select(sql, Arrays.asList(new Column("platform", Integer.parseInt(platformId), Types.INTEGER)));
        logger.info("Recommend debug recommend item  all_num topic =" + topic_list.size());
        if (!CollectionUtils.isEmpty(topic_list)) {
            for (List<Column> columns : topic_list) {
                collector.emit("topicRecommendResult",new Values(customerId,columns.get(0).getVal().toString(),resourceType));
            }
        }
    }

    private void userRecommend(String customerId, String platformId, BasicOutputCollector collector){
        //找出关注列表中已存在的有效用户ID
        List<List<Column>> followList = jdbcClient.select("select ref_id from customer_follow_people where customer_id = ? and is_valid = 1 and relationship in (2,4)",
                Arrays.asList(new Column("customer_id", Long.parseLong(customerId), Types.BIGINT)));
        //获取id列表
        StringBuilder followIdList = new StringBuilder();
        for (int i = 0,j=followList.size(); i < j; i++) {
            followIdList.append(followList.get(i).get(0).getVal());
            if(i!=j-1) {
                followIdList.append(",");
            }
        }
        //先从cms_recommend表获取owner_column_id为113的所有会员
        //定义子查询语句获取备选推荐用户id及相关信息,并去掉已经关注过的用户id及该用户本身
        String q = "select customer_id from customer_auth where customer_id != ? and customer_id in (select ref_id from cms_recommend where owner_column_id = 113 and is_valid = 1 and ref_id not in ("+followIdList.toString()+"))";
        List<List<Column>> idList = null;
        if(platformId=="1"){//如果用户platformId为1，则推送所有platformId的用户
            idList = jdbcClient.select(q,Arrays.asList(new Column("customerId", Long.parseLong(customerId), Types.BIGINT)));
        }else{//如果用户platformId不为1，则只推荐platformId相同的用户
            q = q + " and platform_id = ?";
            idList = jdbcClient.select(q,Arrays.asList(
                    new Column("customerId", Long.parseLong(customerId), Types.BIGINT),
                    new Column("platId", Integer.parseInt(platformId), Types.INTEGER))
            );
        }
        StringBuilder defaultIdList = new StringBuilder();
        for (int i = 0,j=idList.size(); i < j; i++) {
            defaultIdList.append(idList.get(i).get(0).getVal());
            if(i!=j-1) {
                defaultIdList.append(",");
            }
        }

        //---------------------------------------------通过条件继续推荐---------------------------------------------
        String company = "";
        String medicalTitle = "";
        String areasExpertise = "";
        //查询用户的各项认证信息--医院，专业，职称
        List<List<Column>> customerAuthList = jdbcClient.select("select company,areas_expertise,medical_title from customer_auth where customer_id = ?", Arrays.asList(new Column("customer_id",Long.parseLong(customerId), Types.BIGINT)));
        if(CollectionUtils.isNotEmpty(customerAuthList)){
            List<Column> columns = customerAuthList.get(0);
            for (Column column : columns) {
                switch (column.getColumnName()){
                    case "company" : company = String.valueOf(column.getVal());
                    case "areas_expertise" : areasExpertise = String.valueOf(column.getVal());
                    case "medical_title" : medicalTitle = String.valueOf(column.getVal());
                }
            }
        }
        //找出相同医院的所有用户
        if(StringUtils.isNotEmpty(company)){
            String sql = "select customer_id,areas_expertise,medical_title from customer_auth where customer_id != ? and company = ? and state in (1,2) and customer_id not in ("+followIdList.toString()+")";
            List<List<Column>> recommendList = null;
            if(platformId=="1"){//如果用户platformId为1，则推送所有platformId的用户
                recommendList = jdbcClient.select(sql,Arrays.asList(new Column("customerId",Long.parseLong(customerId),Types.BIGINT),new Column("company", company, Types.VARCHAR)));
            }else{//如果用户platformId不为1，则只推荐platformId相同的用户
                sql = sql + " and platform_id = ?";
                recommendList = jdbcClient.select(sql,Arrays.asList(
                        new Column("customerId",Long.parseLong(customerId),Types.BIGINT),
                        new Column("company", company, Types.VARCHAR),
                        new Column("platId", Integer.parseInt(platformId), Types.INTEGER))
                );
            }
            StringBuilder firstLevelList = new StringBuilder();
            StringBuilder secondLevelList = new StringBuilder();
            StringBuilder lastLevelList = new StringBuilder();
            //找出医院相同，title类似且专业类似的用户为第一优先级
            //医院相同，title类似的用户为第二优先级
            //获取title及专业对应的正则表达式
            String titleRegex = getRegex(medicalTitle);
            String expertRegex = getRegex(areasExpertise);
            //找出医院相同且title类似的用户
            for (int i = 0; i < recommendList.size(); i++) {
                String id = String.valueOf(recommendList.get(i).get(0).getVal());
                String expert = String.valueOf(recommendList.get(i).get(1).getVal());
                String title = String.valueOf(recommendList.get(i).get(2).getVal());
                if(Pattern.matches(titleRegex, title)){//过滤出medicalTitle类似的用户
                    //进一步过滤是否专业类似
                    if(Pattern.matches(expertRegex,expert)){//如果专业类似优先级为1
                        firstLevelList.append(id);
                        firstLevelList.append(",");
                    }else{//专业不同，但医院相同且title类似，优先级为2
                        secondLevelList.append(id);
                        secondLevelList.append(",");
                    }
                }else{//只有医院相同
                    lastLevelList.append(id);
                    lastLevelList.append(",");
                }
            }
            //由于得分基于时间戳，所以高优先后发送(其实没区别)
            collector.emit("customerRecommend",new Values(firstLevelList.toString(),customerId,1));
            collector.emit("customerRecommend",new Values(secondLevelList.toString(),customerId,2));
            collector.emit("customerRecommend",new Values(lastLevelList.toString(),customerId,3));
            collector.emit("customerRecommend",new Values(defaultIdList.toString(),customerId,0));
        }
    }

    private String getRegex(String base){
        String[] splitTitle = base.split(",");
        String regex = "";
        for (String s : splitTitle) {//拼正则表达式
            regex += s + "|";
        }
        return regex;
    }

    private void recommendCustomerTag(String customerId, String resourceType, String resourceId,String platformId, BasicOutputCollector collector) {
        try {
            //从数据库取出用户已关注的标签
            String sql = "select ref_id from customer_follow_resource where is_valid = 1 and follow_type = 61 and customer_id = ?";
            List<List<Column>> followResourceList = jdbcClient.select(sql, Arrays.asList(new Column("customerId", Long.parseLong(customerId), Types.BIGINT)));
            List followedIdList = new ArrayList();
            for (int i = 0,j=followResourceList.size(); i < j; i++) {
                followedIdList.add(followResourceList.get(i).get(0).getVal());
            }
            String sql2 = "select property_id, property_parent_id from cms_resource_property where is_valid = 1 and resource_type = ? and resource_id = ? " +
                    "order by tree_level desc,create_time desc";//sortType为1时的排序方式
            List<List<Column>> propertyIdList = jdbcClient.select(sql2, Arrays.asList( //传入3个参数
                    new Column("resourceType", Integer.parseInt(resourceType), Types.INTEGER),
                    new Column("resourceId", Long.parseLong(resourceId), Types.BIGINT)
                    )
            );
            if (!CollectionUtils.isEmpty(propertyIdList)) {
                //取现有标签的兄弟及父亲
                for (List<Column> columns : propertyIdList){
                    Long propertyId = Long.parseLong(columns.get(0).getVal().toString());
                    Long propertyParentId = Long.parseLong(columns.get(1).getVal().toString());
                    if (propertyParentId!=null){
                        //推送自己（标签本身）
                        if (!getIsCustomerFollowResource(propertyId,followedIdList)){
                            logger.info("推荐标签本身");
                            collector.emit("tagRecommend",new Values(customerId,propertyId,platformId));
                        }
                        //推送兄弟
                        String sql3 = "select property_id from comm_data_property where is_valid = 1 and platform_id = ? and parent_id = ? ORDER BY (video_num + doc_num + topic_num + case_num) DESC";
                        List<List<Column>> brotherIdList = jdbcClient.select(sql3, Arrays.asList(
                                new Column("platformId", Integer.parseInt(platformId), Types.INTEGER),
                                new Column("parentId", propertyParentId, Types.BIGINT)
                                )
                        );
                        if (!CollectionUtils.isEmpty(brotherIdList)) {
                            for (int i = 0,j=brotherIdList.size(); i < j; i++) {
                                Long id = Long.valueOf(brotherIdList.get(i).get(0).getVal().toString());
                                if(id!=0 && !getIsCustomerFollowResource(id,followedIdList)){
                                    //推送兄弟时platformId为0
                                    logger.info("推荐兄弟标签");
                                    collector.emit("tagRecommend",new Values(customerId,id,"0"));
                                }
                            }
                        }
                        //推荐父标签
                        if (!getIsCustomerFollowResource(propertyId,followedIdList)){
                            logger.info("推荐父标签");
                            collector.emit("tagRecommend",new Values(customerId,propertyParentId,platformId));
                        }
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /*
    * 检验用户是否已关注该property
    * */
    private boolean getIsCustomerFollowResource(Long propertyId, List followedIdList) {
        for (int i = 0,j=followedIdList.size(); i < j; i++) {
            Object o = followedIdList.get(i);
            if(propertyId.equals(o)){
                return true;
            }
        }
        return false;
    }
}
