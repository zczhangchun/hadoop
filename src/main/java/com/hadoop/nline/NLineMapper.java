package com.hadoop.nline;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhangchun
 */
public class NLineMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    Text k = new Text();
    LongWritable v = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取第一行
        String line = value.toString();

        //切割
        String[] fileds = line.split(" ");

        for (String filed : fileds) {
            k.set(filed);
            context.write(k, v);
        }

    }
}
