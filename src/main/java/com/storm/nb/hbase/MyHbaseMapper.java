package com.storm.nb.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.common.ColumnList;
import org.apache.storm.tuple.Tuple;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by DuJunchen on 2017/4/11.
 */
public class MyHbaseMapper implements HBaseMapper {
    private static AtomicInteger count = new AtomicInteger(0);
    @Override
    public byte[] rowKey(Tuple tuple) {
        return tuple.getStringByField("id").getBytes();
    }

    @Override
    public ColumnList columns(Tuple tuple) {
        ColumnList cols = new ColumnList();
        String values = tuple.getStringByField("values");
        String[] split = values.split(",");
        for (String s : split) {
            cols.addColumn("tag".getBytes(),s.getBytes(), Bytes.toBytes(0.0));
        }
        System.out.println("处理条数"+count.incrementAndGet());
        return cols;
    }
}
