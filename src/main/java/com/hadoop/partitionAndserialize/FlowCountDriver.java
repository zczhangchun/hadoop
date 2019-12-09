package com.hadoop.partitionAndserialize;

import com.hadoop.model.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author zhangchun
 */
public class FlowCountDriver {

    public static void main(String[] args) throws Exception{

        Configuration configuration = new Configuration();

        args = new String[2];
        args[0] = "/Users/zhangchun/workspace/3.txt";
        args[1] = "/Users/zhangchun/workspace/output2";
        Job job = Job.getInstance(configuration);

        job.setJarByClass(FlowCountDriver.class);
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //指定自定义数据分区
        job.setPartitionerClass(ProvinPartitioner.class);

        //指定ReduceTask数量
        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);


    }
}
