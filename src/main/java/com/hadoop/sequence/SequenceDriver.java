package com.hadoop.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * @author zhangchun
 * 将多个小文件合并成一个SequenceFile文件
 * （SequenceFile文件是Hadoop用来存储二进制形式的key-value对的文件格式）
 * SequenceFile里面存储着多个文件，存储的形式为文件路径+名称为key，文件内容为value。
 */
public class SequenceDriver {
    public static void main(String[] args) throws Exception{

        args = new String[2];
        args[0] = "/Users/zhangchun/workspace/input";
        args[1] = "/Users/zhangchun/workspace/output";
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);

        job.setJarByClass(SequenceDriver.class);
        job.setMapperClass(SequenceMapper.class);
        job.setReducerClass(SequenceReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        //设置自己写的inputformat
        job.setInputFormatClass(WholeInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);


    }
}
