package com.atguigu.mr.wordcunt1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MR程序驱动类用于提交MR任务
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //声明配置对象
        Configuration conf = new Configuration();
        //设置HDFS NameNode的地址
        conf.set("fs.defaultFS", "hdfs://hadoop102:9820");
        // 指定MapReduce运行在Yarn上
        conf.set("mapreduce.framework.name","yarn");
        // 指定mapreduce可以在远程集群运行
        conf.set("mapreduce.app-submission.cross-platform","true");
        //指定Yarn resourcemanager的位置
        conf.set("yarn.resourcemanager.hostname","hadoop103");

        //声明job象
        Job job = Job.getInstance(conf);
        //指定当前job的驱动类
//        job.setJarByClass(WordCountDriver.class);
        job.setJar("G:\\IdeaProject\\HdfsClient\\MapReduce\\target\\MapReduce-1.0-SNAPSHOT.jar");
        //指定当前job的mapper和reduce
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);
        //指定map段输出的key的类型和输出数据value的值
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //指定最终输出结果key的类型和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //指定输入数据的路径和输出输的路径
//        FileInputFormat.setInputPaths(job,new Path("G:\\count\\in"));
//        FileOutputFormat.setOutputPath(job,new Path("G:\\count\\out"));
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交job
        job.waitForCompletion(true);
    }
}
