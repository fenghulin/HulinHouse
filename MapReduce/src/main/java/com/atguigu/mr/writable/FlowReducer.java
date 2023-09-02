package com.atguigu.mr.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private Text outkey = new Text();
    private FlowBean outv = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {

        int totalUpFlow=0;
        int totalDownFlow=0;
        int totalSunFlow=0;
        //遍历当前相同一组相同key的values
        for (FlowBean value : values) {
            totalUpFlow+=value.getUpFlow();
            totalDownFlow+=value.getDownFlow();
            totalSunFlow+=value.getSumFlow();
        }


        //封装输出数据的key

        //封装输出数据的value
        outv.setUpFlow(totalUpFlow);
        outv.setDownFlow(totalDownFlow);
        outv.setSumFlow(totalSunFlow);
        context.write(key,outv);
    }
}
