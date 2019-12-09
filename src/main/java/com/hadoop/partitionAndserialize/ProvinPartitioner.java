package com.hadoop.partitionAndserialize;

import com.hadoop.model.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author zhangchun
 */
public class ProvinPartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {

        //根据手机号返回分区
        String prefix = text.toString().substring(0, 3);
        int partition = 4;

        if ("136".equals(prefix)){
            partition = 0;
        }else if ("137".equals(prefix)){
            partition = 1;
        }else if ("138".equals(prefix)){
            partition = 2;
        }else if ("139".equals(prefix)){
            partition = 3;
        }

        return partition;

    }
}
