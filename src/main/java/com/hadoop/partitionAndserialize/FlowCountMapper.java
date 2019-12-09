package com.hadoop.partitionAndserialize;

import com.hadoop.model.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zhangchun
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] fileds = line.split("\t");

        String phoneNum = fileds[1];

        Long downFlow = Long.parseLong(fileds[fileds.length - 2]);

        long upFlow = Long.parseLong(fileds[fileds.length - 3]);

        k.set(phoneNum);
        FlowBean v = new FlowBean(upFlow, downFlow);

        context.write(k, v);






    }
}
