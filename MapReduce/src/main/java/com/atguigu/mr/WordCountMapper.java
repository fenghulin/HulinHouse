package com.atguigu.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 自定义的Mapper类 需要集成Hadoop提供的Mapper 并且根据具体业务制定输入数据和输出数据的数据类型
 *
 * 输入数据类型
 * KEYIN, 读取文件偏移量 数字（LongWritable)
 * VALUEIN,读取文件一行数据文本(Text)
 * 输出数据类型
 * KEYOUT, 输出数据key的类型就是一个单词(Text)
 * VALUEOUT 输出数据value的类型 给单词的一个标记 1 数字（IntWritable)
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    /**
     * Map阶段核心业务处理方法，每输入一行数据会调用一次map方法
     * @param key  输入的数据key
     * @param value 输入的数据value
     * @param context 上下文对象
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        //获取当前输入的数据
        String line = value.toString();
        //切割数据
        String[] datas = line.split(" ");
        //遍历集合 封装 输出数据的key和value
        for (String data : datas) {
            context.write(new Text(data),new IntWritable(1));
        }

    }
}
