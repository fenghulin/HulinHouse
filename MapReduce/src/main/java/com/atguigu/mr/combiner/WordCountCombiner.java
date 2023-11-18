package com.atguigu.mr.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.nio.CharBuffer;

public class WordCountCombiner extends Reducer<Text, IntWritable,Text, IntWritable> {
    private Text outk = new Text();
    private IntWritable outv = new IntWritable();
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
