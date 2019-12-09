package com.hadoop.sequence;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import javax.xml.soap.Text;
import java.io.IOException;

/**
 * @author zhangchun
 */
public class WholeInputFormat extends FileInputFormat<Text, ByteWritable> {


    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        WholeRecordReader recordReader = new WholeRecordReader();
        recordReader.initialize(split, context);
        return recordReader;

    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        //不把文件内容分成一行一行
        return false;
    }
}
