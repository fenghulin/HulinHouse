package com.atguigu.mr.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text outkey = new Text();
    private FlowBean outv = new FlowBean();
    /**
     * 核心业务逻辑处理
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        //获取当前行数据
        String line = value.toString();
        //切割数据
        String[] phoneDatas = line.split("\t");
        //获取输出的数据key（手机号）
        outkey.set(phoneDatas[1]);
        //获取数据value
        outv.setUpFlow(Integer.parseInt(phoneDatas[phoneDatas.length-3]));
        outv.setDownFlow(Integer.parseInt(phoneDatas[phoneDatas.length-2]));
        outv.setSumFlow();

        //将数据输出
        context.write(outkey,outv);

    }
}
