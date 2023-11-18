package com.atguigu.mr.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 自定义的Mapper类 需要集成Hadoop提供的Reduce 并且根据具体业务制定输入数据和输出数据的数据类型
 *
 * 输入数据类型
 * KEYIN, Map端输出key的数据类型 数字（LongWritable)
 * VALUEIN,VALUEIN
 * 输出数据类型
 * KEYOUT, 输出数据key的类型就是一个单词(Text)
 * VALUEOUT 输出数据value的类型 给单词的一个标记 1 数字（IntWritable)
 */
public class WordCountReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
    private Text outk = new Text();
    private IntWritable outv = new IntWritable();
    /**
     * Reduce阶段的核心业务处理方法，一组相同的key的values会调用一次reduce
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
       int total=0;
        // 遍历values
        for (IntWritable value : values) {
            //对每一次value累加输出结果
            total+=value.get();
        }
        outk.set(key);
        outv.set(total);
        context.write(outk,outv);
    }
}
