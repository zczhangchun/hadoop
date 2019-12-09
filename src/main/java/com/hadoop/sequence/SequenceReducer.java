package com.hadoop.sequence;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhangchun
 */
public class SequenceReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {

        for (BytesWritable value : values) {
            System.out.println("reducer : " + key);
            System.out.println("reducer ：" + value);
            context.write(key, value);
        }

    }
}
