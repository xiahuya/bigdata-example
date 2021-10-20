package cn.xhjava.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * @author Xiahu
 * @create 2020/12/10
 */
public class ZkWatch {
    private static final String CONNECT_STRING = "node2";
    private static final int SESSION_TIMEOUT = 5000;

    public static void main(String[] args) throws Exception {

        // 获取连接
        // 当前的这个匿名内部类不是已经添加好的监听， 以后只要是当前这个zk对象添加了任何的监听器响应了之后，
        // 都会调用这个process方法
        ZooKeeper zk = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, new Watcher() {

            public void process(WatchedEvent event) {

                System.out.println("1111111111111111111111");
                Event.KeeperState state = event.getState();
                String path = event.getPath();
                Event.EventType type = event.getType();

                System.out.println(state + "\t" + path + "\t" + type);//SyncConnected	null	None
            }
        });

        System.out.println("2222222222222222222222");

        /**
         * 注册监听
         * 第二个参数有三种传法：
         *
         * 1、false, 表示不使用监听器
         *
         * 2、watcher对象， 表示当前的这次监听如果响应不了的话，就会回调当前这个watcher的process方法
         *
         * 3、true,  表示如果当前的会话/zk 所注册或者添加的所有的监听器的响应，都会会调用 获取连接时  初始化的 监听器对象中 的 process 方法
         */
        zk.getData("/a/c", true, null);

        System.out.println("3333333333333333333333333333");
        Thread.sleep(5000);

        zk.setData("/a/c", "hehe666".getBytes(), -1);

        System.out.println("4444444444444444444444444444");

        zk.close();
    }
}