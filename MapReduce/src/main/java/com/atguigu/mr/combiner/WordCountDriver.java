package com.atguigu.mr.combiner;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Scanner;

/**
 * MR程序驱动类用于提交MR任务
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        //声明配置对象
        Configuration conf = new Configuration();
        //声明job象
        Job job = Job.getInstance(conf);
        //指定当前job的驱动类
        job.setJarByClass(WordCountDriver.class);
        //指定当前job的mapper和reduce
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);
        //指定map段输出的key的类型和输出数据value的值
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //指定最终输出结果key的类型和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass (IntWritable.class);

        //指定自定义的combiner
        job.setCombinerClass(WordCountCombiner.class);
        //指定CombinTextInputFormat中切片的最大值
//        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);
        //指定输入数据的路径和输出输的路径
//        job.setInputFormatClass(CombineTextInputFormat.class);

        FileInputFormat.setInputPaths(job,new Path("G:\\2021尚硅谷大数据\\03.Hadoop\\02.资料\\07_测试数据\\combine"));
        FileOutputFormat.setOutputPath(job,new Path("G:\\2021尚硅谷大数据\\03.Hadoop\\02.资料\\07_测试数据\\combine_out2"));
//        FileInputFormat.setInputPaths(job,new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交job
        job.waitForCompletion(true);
    }
}
