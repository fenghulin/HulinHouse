package com.atguigu.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class CopyFromLocalFile {

    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {

        Configuration configuration = new Configuration() ;
        configuration.set("dfs.replication","2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9820"),configuration,"hadoop");
        fs.copyFromLocalFile(new Path("G:/data/hello.txt"),new Path("/hello.txt"));

        // 3 关闭资源
        fs.close();

        System.out.println("over");

    }
}
