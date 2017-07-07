package com.storm.nb.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hbase.trident.windowing.HBaseWindowsStoreFactory;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.testing.CountAsAggregator;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.WindowsStoreFactory;
import org.apache.storm.trident.windowing.config.TumblingCountWindow;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * Created by DuJunchen on 2017/4/21.
 */
public class MyTridentTopology {
    private static final Logger LOG = LoggerFactory.getLogger(MyTridentTopology.class);
    public static void main(String[] args) throws Exception {
        /*//设置kafka相关的zooKeeper节点
        BrokerHosts brokerHosts = new ZkHosts("hadoop2:2181,hadoop3:2181,hadoop4:2181");
        //根据brokerList及topic创建对应的tridentKafkaConfig类
        TridentKafkaConfig tridentKafkaConfig = new TridentKafkaConfig(brokerHosts,"dataClean");
        //设置scheme为string
        tridentKafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        //
        OpaqueTridentKafkaSpout kafkaSpout = new OpaqueTridentKafkaSpout(tridentKafkaConfig);
        TridentTopology tridentTopology = new TridentTopology();*/
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        conf.put(Config.TOPOLOGY_TRIDENT_WINDOWING_INMEMORY_CACHE_LIMIT, 100);

        // window-state table should already be created with cf:tuples column
        HBaseWindowsStoreFactory windowStoreFactory = new HBaseWindowsStoreFactory(new HashMap<String, Object>(), "window-state", "cf".getBytes("UTF-8"), "tuples".getBytes("UTF-8"));

        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            String topologyName = "wordCounterWithWindowing";
            cluster.submitTopology(topologyName, conf, buildTopology(windowStoreFactory));
            Utils.sleep(120 * 1000);
            cluster.killTopology(topologyName);
            cluster.shutdown();
            System.exit(0);
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, buildTopology(windowStoreFactory));
        }
    }

    public static StormTopology buildTopology(WindowsStoreFactory windowsStore) throws Exception {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"), new Values("four score and seven years ago"),
                new Values("how many apples can you eat"), new Values("to be or not to be the person"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();

        Stream stream = topology.newStream("spout1", spout).parallelismHint(16).each(new Fields("sentence"),
                new Split(), new Fields("word"))
                .window(TumblingCountWindow.of(1000), windowsStore, new Fields("word"), new CountAsAggregator(), new Fields("count"))
                .peek(new Consumer() {
                    @Override
                    public void accept(TridentTuple input) {
                        LOG.info("Received tuple: [{}]", input);
                    }
                });

        return topology.build();
    }
}
