package com.hadoop.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author zhangchun
 */
public class WholeRecordReader extends RecordReader<Text, BytesWritable> {

    FileSplit fileSplit = null;
    Configuration configuration = null;
    Text k = new Text();
    BytesWritable v = new BytesWritable();
    boolean flag = true;



    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        fileSplit = (FileSplit) split;
        configuration = context.getConfiguration();


    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        while (flag) {
            //封装key和value，key就是文件名称，value就是文件内容，
            //获得输入流
            //获得文件路径，key有了
            Path path = fileSplit.getPath();

            //通过文件可以获取文件系统
            FileSystem fileSystem = path.getFileSystem(configuration);
            //通过文件系统获取文件的内容
            FSDataInputStream fis = fileSystem.open(path);
            //设置key
            k.set(path.toString());

            //设置value
            byte[] bytes = new byte[(int) fileSplit.getLength()];
            IOUtils.readFully(fis, bytes, 0, bytes.length);

            v.set(bytes, 0, bytes.length);
            flag = false;
            return true;
        }

        return false;





    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
