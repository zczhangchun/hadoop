package com.hadoop.hdfs;

import org.apache.commons.io.IOCase;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * @author zhangchun
 */
public class HdfsClient {

    @Test
    public void m1()throws Exception{

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"),configuration, "root");
        fs.mkdirs(new Path("/0529/banzhang/safemode"));
        fs.close();

    }

    @Test
    public void m2()throws Exception{

        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "3");

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration, "root");
        fs.copyFromLocalFile(new Path("/Users/zhangchun/panjinlian.txt"),new Path("/"));
        fs.close();


    }

    @Test
    public void m3()throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration, "root");
        fs.copyToLocalFile(new Path("/panjinlian.txt"), new Path("/Users/zhangchun"));
        fs.close();
    }

    @Test
    public void m4()throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration, "root");
        fs.delete(new Path("/panjinlian.txt"), true);
        fs.close();
    }

    @Test
    public void m5()throws Exception{

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration, "root");
        fs.rename(new Path("/panjinlian.txt"), new Path("/zhangchun.txt"));
        fs.close();


    }

    @Test
    public void m6()throws Exception{

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration, "root");
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {

            LocatedFileStatus next = listFiles.next();
            BlockLocation[] blockLocations = next.getBlockLocations();
            System.out.println(next.getBlockSize());
            System.out.println(next.getGroup());
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }

        }
        fs.close();

    }

    @Test
    public void m7()throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration, "root");
        FileStatus[] statuses = fs.listStatus(new Path("/"));
        for (FileStatus status : statuses) {
            if (status.isFile()) {

                System.out.println("file:" + status.getPath().getName());
            }else {
                System.out.println("directory:" + status.getPath().getName());
            }
        }
    }

    @Test
    public void m8()throws Exception{

        Configuration configuration = new Configuration();
        FileInputStream fis = new FileInputStream(new File("/Users/zhangchun/zhangchun.txt"));

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration, "root");
        FSDataOutputStream fsdos = fs.create(new Path("/zc.txt"));

        IOUtils.copyBytes(fis, fsdos, configuration);

        fs.close();

    }

    @Test
    public void m9()throws Exception{

        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration, "root");

        FSDataInputStream fsdis = fileSystem.open(new Path("/zc.txt"));

        //必须是存在的文件
        File file = new File("/Users/zhangchun/zc.txt");
        file.createNewFile();
        FileOutputStream fos = new FileOutputStream(file);

        IOUtils.copyBytes(fsdis, fos, configuration);
        fileSystem.close();

    }

    @Test
    public void m10()throws Exception{

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration, "root");
        FSDataInputStream fsdis = fs.open(new Path("/jdk-8u231-linux-x64.tar.gz"));

        File file = new File("/Users/zhangchun/jdk-8u231-linux-x64.tar.gz.part1");
        FileOutputStream fos = new FileOutputStream(file);
        byte[] bytes = new byte[1024];

        for (int i = 0; i < 1024 * 128; i++) {
            fsdis.read(bytes);
            fos.write(bytes);
        }

        fsdis.close();
        fos.close();
        fs.close();


    }

    @Test
    public void m11()throws Exception{

        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), configuration, "root");

        FSDataInputStream fsdis = fs.open(new Path("/jdk-8u231-linux-x64.tar.gz"));
        fsdis.seek(1024 * 1024 * 128);

        File file = new File("/Users/zhangchun/jdk-8u231-linux-x64.tar.gz.part2");
        file.createNewFile();
        FileOutputStream fos = new FileOutputStream(file);

        IOUtils.copyBytes(fsdis, fos, configuration);

        IOUtils.closeStream(fos);
        IOUtils.closeStream(fsdis);
        IOUtils.closeStream(fs);




    }

}
