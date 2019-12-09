package com.hadoop.keyv;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhangchun
 */
public class KVTextMapper extends Mapper<Text, Text, Text, LongWritable> {

    LongWritable v = new LongWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        System.out.println(key);
        System.out.println(value);

        context.write(key, v);

    }
}
