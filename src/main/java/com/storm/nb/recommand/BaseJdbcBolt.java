package com.storm.nb.recommand;

import org.apache.commons.lang.Validate;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IBasicBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by DuJunchen on 2017/5/9.
 */
public abstract class BaseJdbcBolt implements IBasicBolt{
    private static final Logger logger = LoggerFactory.getLogger(BaseJdbcBolt.class);
    protected transient JdbcClient jdbcClient;
    protected Integer queryTimeoutSecs;
    protected ConnectionProvider connectionProvider;

    public BaseJdbcBolt(ConnectionProvider connectionProvider) {
        Validate.notNull(connectionProvider);
        this.connectionProvider = connectionProvider;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
        this.connectionProvider.prepare();
        if(this.queryTimeoutSecs == null) {
            this.queryTimeoutSecs = Integer.valueOf(Integer.parseInt(map.get("topology.message.timeout.secs").toString()));
        }
        this.jdbcClient = new JdbcClient(this.connectionProvider, this.queryTimeoutSecs.intValue());
    }

    @Override
    public void cleanup() {
        this.connectionProvider.cleanup();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
