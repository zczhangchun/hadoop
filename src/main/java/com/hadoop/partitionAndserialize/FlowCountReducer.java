package com.hadoop.partitionAndserialize;

import com.hadoop.model.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zhangchun
 */
public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {


    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long upSum = 0;
        long downSum = 0;

        for (FlowBean value : values) {
            upSum += value.getUpFlow();
            downSum += value.getUpFlow();
        }

        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(upSum);
        flowBean.setDownFlow(downSum);
        long sum = upSum + downSum;
        flowBean.setSumFlow(sum);

        context.write(key, flowBean);

    }
}
