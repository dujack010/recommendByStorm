package com.storm.nb.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.storm.hbase.common.HBaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by DuJunchen on 2017/7/7.
 */
public class HBaseConnection implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseClient.class);
    private Connection connection;

    public HBaseConnection(final Configuration configuration) {
        try {
            this.connection = ConnectionFactory.createConnection(configuration);
        } catch (Exception e) {
            throw new RuntimeException("HBase bolt preparation failed: " + e.getMessage(), e);
        }
    }

    public Table getTable(String tableName){
        try {
            return this.connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        this.connection.close();
    }
}
