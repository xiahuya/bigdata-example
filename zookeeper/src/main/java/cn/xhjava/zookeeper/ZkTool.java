package cn.xhjava.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author Xiahu
 * @create 2020/12/10
 */
public class ZkTool {
    private static final String CONNECT_STRING = "node2:2181";

    //设置超时时间
    private static final int SESSION_TIMEOUT = 5000;

    //获取zookeeper的连接
    static ZooKeeper zk;

    static {
        try {
            zk = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //创建一个节点

    /**
     * 四个参数path, data, acl, createMode
     * path:创建节点的绝对路径
     * data：节点存储的数据
     * acl:权限控制
     * createMode：节点的类型----永久、临时    有编号的、没有编号的
     */
    public  void createZnode(String path, byte[] data) {
        try {
            zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //查看该节点是否存在
    public void exist(String path) throws Exception {
        Stat exists = zk.exists(path, null);
        if (exists == null) {
            System.out.println("节点不存在");
        } else {
            System.out.println("节点存在");
        }
    }

    //查看节点数据
    public String getData(String path) throws Exception {
        return new String(zk.getData(path, false, null));
    }

    //修改节点数据
    public void setData(String path, byte[] data) throws Exception {
        Stat setData = zk.setData(path, data, -1);
        if (setData == null) {
            System.out.println("节点不存在 --- 修改不成功");
        } else {
            System.out.println("节点存在 --- 修改成功");
        }
    }

    //删除节点
    public void deleteNode(String path) {
        try {
            zk.delete(path, -1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    //关闭连接
    public void close() {
        try {
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 级联查看某节点下所有节点及节点值
     *
     * @throws Exception
     * @throws KeeperException
     */
    public static Map<String, String> getChildNodeAndValue(String path, ZooKeeper zk, Map<String, String> map) throws Exception {
        //看看传入的节点是否存在
        if (zk.exists(path, false) != null) {
            //存在的话将该节点的数据存放到map中，key是绝对路径，value是存放的数据
            map.put(path, new String(zk.getData(path, false, null)));
            //查看该节点下是否还有子节点
            List<String> list = zk.getChildren(path, false);
            if (list.size() != 0) {
                //遍历子节点,递归调用自身的方法
                for (String child : list) {
                    getChildNodeAndValue(path + "/" + child, zk, map);
                }
            }
        }

        return map;
    }

    /**
     * 删除一个节点，不管有有没有任何子节点
     */
    public static boolean rmr(String path, ZooKeeper zk) throws Exception {
        //看看传入的节点是否存在
        if ((zk.exists(path, false)) != null) {
            //查看该节点下是否还有子节点
            List<String> children = zk.getChildren(path, false);
            //如果没有子节点，直接删除当前节点
            if (children.size() == 0) {
                zk.delete(path, -1);
            } else {
                //如果有子节点，则先遍历删除子节点
                for (String child : children) {
                    rmr(path + "/" + child, zk);
                }
                //删除子节点之后再删除之前子节点的父节点
                rmr(path, zk);
            }
            return true;
        } else {
            //如果传入的路径不存在直接返回不存在
            System.out.println(path + " not exist");
            return false;
        }


    }

    /**
     * 级联创建任意节点
     * create znodePath data
     * create /a/b/c/xx 'xx'
     *
     * @throws Exception
     * @throws KeeperException
     */
    public static boolean createZNode(String znodePath, String data, ZooKeeper zk) throws Exception {

        //看看要创建的节点是否存在
        if ((zk.exists(znodePath, false)) != null) {
            return false;
        } else {
            //获取父路径
            String parentPath = znodePath.substring(0, znodePath.lastIndexOf("/"));
            //如果父路径的长度大于0，则先创建父路径，再创建子路径
            if (parentPath.length() > 0) {
                createZNode(parentPath, data, zk);
                zk.create(znodePath, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                //如果父路径的长度=0，则直接创建子路径
                zk.create(znodePath, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            return true;
        }
    }

    /**
     * 清空子节点
     */
    public static boolean clearChildNode(String znodePath, ZooKeeper zk) throws Exception {

        List<String> children = zk.getChildren(znodePath, false);

        for (String child : children) {
            String childNode = znodePath + "/" + child;
            if (zk.getChildren(childNode, null).size() != 0) {
                clearChildNode(childNode, zk);
            }
            zk.delete(childNode, -1);
        }

        return true;
    }
}
