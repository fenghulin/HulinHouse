package com.hulin.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * 1获取ZK客户端连接对象
 * 2.调用相关API
 * 3.关闭资源
 */
public class ZookeeperTest {

    private ZooKeeper zooKeeper;

    private String path="/sanguo";
    /**
     * 递归删除node
     */
    @Test
    public void testDeleteAllNode(String path, ZooKeeper zk) throws InterruptedException, KeeperException {
        Stat exists = zooKeeper.exists(path, false);
        if (exists == null){
            System.out.println("不存在");
        }else{
            // 获取子节点
            List<String> children = zooKeeper.getChildren(path, false);
            if (children.isEmpty()){

                //没有子节点 直接删除
            }else{
                //递归调用自己先把所有自己点删除
                for (String child : children) {
                    testDeleteAllNode(path+"/"+child,zooKeeper);
                }
                //删除完所有子节点以后，记得删除传入的节点
                zooKeeper.delete(path,exists.getVersion());
            }
        }
    }

    /**
     * 删除node
     */
    @Test
    public void testDeleteNode() throws InterruptedException, KeeperException {
        Stat exists = zooKeeper.exists("/sanguo", false);
        if (exists == null){
            System.out.println("不存在");
        }else{
            zooKeeper.delete("/sanguo",exists.getVersion());
        }
    }
    /**
     * 给node设置值
     */
    @Test
    public void testsetDate() throws InterruptedException, KeeperException {
        Stat exists = zooKeeper.exists("/sanguo", false);
        if (exists == null){
            System.out.println("不存在");
        }else{
            zooKeeper.setData("/sanguo","guaneryedexifu".getBytes(),exists.getVersion());
        }
    }
    /**
     * 从zknode取值
     */
    @Test
    public void testGetDate() throws InterruptedException, KeeperException {
        Stat exists = zooKeeper.exists("/sanguo", false);
        if (exists == null){
            System.out.println("不存在");
        }else{
            byte[] data = zooKeeper.getData("/sanguo", false, exists);
            System.out.println(new String(data));
        }
    }
    /**
     * 判断Znode是否存在
     */
    @Test
    public void exit() throws InterruptedException, KeeperException {
        Stat exists = zooKeeper.exists("/sanguo", false);
        if (exists == null){
            System.out.println("不存在");
        }else{
            System.out.println("存在");
        }
    }

    /**
     * 创建节点
     *  1.path : 之地昂节点路径
     *  2.data : 之地昂要创建的节点内容
     *  3.acl : 对操作用户的权限控制
     *  4.createMode : 指定当前节点类型（持久化的 临时的）
     * @throws IOException
     */
    @Test
    public void createNode() throws InterruptedException, KeeperException {
        zooKeeper.create("/xiyou",
                "guanerye".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
    }

    /**
     *不监控
     * @throws IOException
     */
    @Test
    public void testGetChild() throws InterruptedException, KeeperException {
//        List<String> childrenList = zooKeeper.getChildren("/", false);
        List<String> childrenList = zooKeeper.getChildren("/", new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println("发现根目录下的节点有变化");
            }
        });
        for (String child : childrenList) {
            System.out.println(child);
        }
        Thread.sleep(Long.MAX_VALUE);
    }


    @Before
    public void  init() throws IOException {
        String connerStr = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        zooKeeper = new ZooKeeper(connerStr, 30000, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println("根据具体业务进行下一步操作。。。");
            }
        });
    }

    @After
    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    /**
     * connectString :连接zk服务器地址
     *
     */
    @Test
    public void testCreateZookeeperClien() {

        System.out.println("**********"+zooKeeper);

    }
}
