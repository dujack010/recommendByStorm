package com.storm.nb.topology;

import com.storm.nb.hbase.MyHbaseMapper;
import com.storm.nb.recommand.QueryJdbcBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.kafka.*;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by DuJunchen on 2017/3/17.
 */
public class MyTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        //BrokerHosts brokerHosts = new ZkHosts("nb1:2181,nb2:2181,nb3:2181");
        //设置kafkaSpout
        BrokerHosts brokerHosts = new ZkHosts("hadoop2:2181,hadoop3:2181,hadoop4:2181");
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts,"dataClean","/dataClean","kafkaSpout");
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        //当使用本地集群测试时，需要指定kafka使用的zkServer地址。生产集群下会默认使用storm的zk集群地址
        //spoutConfig.zkServers = Arrays.asList("hadoop2:2181","hadoop3:2181","hadoop4:2181");
        builder.setSpout("kafkaSpout", new KafkaSpout(spoutConfig),1);
        //设置bolt
        //builder.setSpout("kafkaSpout", new RandomStringSpout(),1);
        //定义jdbcBolt
        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://192.168.1.22:4417/allin_platform");
        hikariConfigMap.put("dataSource.user","dev");
        hikariConfigMap.put("dataSource.password","e2s0m1h6");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);
        //builder.setBolt("startBolt", new GetResourceTag(connectionProvider), 1).shuffleGrouping("kafkaSpout");
        builder.setBolt("jdbcBolt",new QueryJdbcBolt(connectionProvider),1).shuffleGrouping("kafkaSpout");

        //定义redisBolt
        //JedisPoolConfig config_customer = new JedisPoolConfig.Builder().setHost("192.168.1.126").setPort(6379).setPassword("admin").setDatabase(0).build();
        //JedisPoolConfig proc = new JedisPoolConfig.Builder().setHost("192.168.1.126").setPort(6379).setPassword("admin").setDatabase(0).build();
        //JedisPoolConfig config_resource = new JedisPoolConfig.Builder().setHost("192.168.1.126").setPort(6379).setPassword("admin").setDatabase(0).build();
        //给用户推荐用户
        //builder.setBolt("customerRecommendRedisBolt",new CustomerRecommendRedisBolt(config_customer),1).shuffleGrouping("jdbcBolt","customerRecommend");
        //给用户推荐资源
        //builder.setBolt("topicRecommendRedisBolt",new ResourceCreateRedisBolt(config_resource),1).shuffleGrouping("jdbcBolt","topicRecommendResult");
        //builder.setBolt("resourceRecommendProcBolt",new ResourceRecommendRedisBolt(proc),1).shuffleGrouping("jdbcBolt","resourceRecommend");
        //builder.setBolt("resourceRecommendSaveBolt",new ResourceCreateRedisBolt(config_resource),1).shuffleGrouping("resourceRecommendProcBolt","resourceRecommendResult");
        //给用户推荐标签
        //builder.setBolt("tagRecommendRedisBolt",new TagRecommendRedisBolt(config_resource),1).shuffleGrouping("jdbcBolt","tagRecommend");

        //HBASE Bolt
        HBaseMapper mapper = new MyHbaseMapper();
        HBaseBolt hBaseBolt = new HBaseBolt("user_profile",mapper).withConfigKey("hbase.conf");
        Map<String,Object> hbConf = new HashMap<String,Object>();
        builder.setBolt("hbaseBolt",hBaseBolt,4).shuffleGrouping("jdbcBolt").setNumTasks(1);

        //定义topology配置
        Config conf = new Config();
        conf.put("hbase.conf",hbConf);
        conf.setNumWorkers(2);
        conf.setNumAckers(0);
        LocalCluster cluster = new LocalCluster();
        try {
            //StormSubmitter.submitTopology("myTop",conf,builder.createTopology());
            cluster.submitTopology("myTop",conf,builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
