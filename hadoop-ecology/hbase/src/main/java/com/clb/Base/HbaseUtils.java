package com.clb.Base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;


public class HbaseUtils {

    /**
     * 配置ss
     */
    static Configuration config = null;
    private Connection connection = null;
    private Table table;

    @Before
    public void init() throws Exception {
        config = HBaseConfiguration.create();// 配置
        config.set("hbase.zookeeper.quorum", "192.168.0.115");// zookeeper地址
        connection = ConnectionFactory.createConnection(config);
        table = connection.getTable(TableName.valueOf("student"));
    }

    /**
     * 创建一个表
     *
     * @throws Exception
     */
    @Test
    public void createTable() throws Exception {
        // 创建表管理类
        Admin admin = connection.getAdmin();// hbase表管理
        // 创建表描述类
        TableName tableName = TableName.valueOf("word"); // 表名称
        HTableDescriptor desc = new HTableDescriptor(tableName);
        // 创建列族的描述类
        HColumnDescriptor family = new HColumnDescriptor("content"); // 列族
        // 将列族添加到表中
        desc.addFamily(family);
//        HColumnDescriptor family2 = new HColumnDescriptor("info"); // 列族
        // 将列族添加到表中
        //desc.addFamily(family2);
        // 创建表
        admin.createTable(desc); // 创建表
    }

    @Test
    @SuppressWarnings("deprecation")
    public void deleteTable() throws Exception {
//        HBaseAdmin admin = new HBaseAdmin(config);
        Admin admin = connection.getAdmin();
        admin.disableTable(TableName.valueOf("test3"));
        admin.deleteTable(TableName.valueOf("test3"));
        admin.close();
    }

    /**
     * 向hbase中增加数据
     *
     * @throws Exception
     */
    @SuppressWarnings({"deprecation", "resource"})
    @Test
    public void insertData() throws Exception {
//        table.setWriteBufferSize(534534534);
//        ArrayList<Put> arrayList = new ArrayList<Put>();
//        for (int i = 0; i < 50; i++) {
//            Put put = new Put(Bytes.toBytes("00"+i));
//            put.add(Bytes.toBytes("content"), Bytes.toBytes("info"), Bytes.toBytes(""+'i'));
//            arrayList.add(put);
//        }
//
//        //插入数据
//        table.put(arrayList);

        Table table = connection.getTable(TableName.valueOf("word"));
        ArrayList<Put> lists = new ArrayList<Put>();
        Put put = new Put(Bytes.toBytes("001"));
        Put put2 = new Put(Bytes.toBytes("002"));
        Put put3 = new Put(Bytes.toBytes("003"));
        put.addColumn(Bytes.toBytes("content"), Bytes.toBytes("info"), Bytes.toBytes("The key to working on projects at Apache (and any open source for that matter) is to have a personal reason for being involved. You might be trying to solve a day job issue, you might be looking to learn a new technology or you might simply want to do something fun in your free time. We don't care what your motivation is we just care about you wanting to get involved.\n" +
                "\n" +
                "If you don't have a specific technical issue to solve you might be willing to work on any project. Our projects page provides a useful index which allows you to view projects alphabetically, by category or by language. When you view a projects detail page in this list you will find details of their mailing lists, issue tracker and other resources.\n" +
                "\n" +
                "As well as our main projects you might also like to view our Incubating projects. These projects work in exactly the same way as our \"top level projects\" but are still developing their initial community.\n" +
                "\n" +
                "Once you've found some interesting looking projects join their mailing lists and start \"lurking\". Read the mails that come through the list. Initially you will not understand what people are talking about, but over time you will start to pick up the language, objectives, strategies and working patterns of the community."));
        put2.addColumn(Bytes.toBytes("content"), Bytes.toBytes("info"), Bytes.toBytes("If you are trying to satisfy a specific technical problem then you already know what you want to work on, but if you are looking for something useful to do in order to participate in an ASF project then the projects issue/bug tracker is your friend (this will be linked from the projects home page or from its entry on the projects page linked above).\n" +
                "\n" +
                "In the projects issue tracker you will find details of bugs and feature requests the project would like to work with, this should give you some inspiration about how you might be able to help the project community. If you are looking for a beginner level issue try searching JIRA for issues with the label \"GSoC\" or \"mentor\", these are issues the community feel are manageable for someone new to the ASF and their project. The community has also indicated that they are willing to help someone work on those issues through our mentoring program."));

        put3.addColumn(Bytes.toBytes("content"), Bytes.toBytes("info"), Bytes.toBytes("once you have identified an issue you would like to tackle its time to join the projects mailing list (if you haven't already) and get started.\n" +
                "\n" +
                "Remember, community members usually happy to help, but they have to get something in return. The community needs to believe that you intend to contribute positively to their work. There is a limit to how much \"hand holding\" you will get so be ready to do some work if you expect to continue to be helped in your first foray into open source.\n" +
                "\n" +
                "At this point you might want to request that someone in the community offers to mentor you. See our mentoring programme for guidance on how to do this.\n" +
                "\n" +
                "Alternatively you can dive straight in and work with the community. Since you've been lurking on the lists for a while by the time you have got to this point you should have a feel for how to get involved, so go for it."));
        lists.add(put);
        lists.add(put2);
        lists.add(put3);

        table.put(lists);
        table.close();

    }

    /**
     * 修改数据
     *
     * @throws Exception
     */
    @Test
    public void uodateData() throws Exception {
        Put put = new Put(Bytes.toBytes("1234"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("namessss"), Bytes.toBytes("lisi1234"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"), Bytes.toBytes(1234));
        //插入数据
        table.put(put);
    }

    /**
     * 删除数据
     *
     * @throws Exception
     */
    @Test
    public void deleteDate() throws Exception {
        Delete delete = new Delete(Bytes.toBytes("1234"));
        table.delete(delete);
    }


    /**
     * 单条查询
     *
     * @throws Exception
     */
    @Test
    public void queryData() throws Exception {
        Get get = new Get(Bytes.toBytes("1234"));
        Result result = table.get(get);
        System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("password"))));
        System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("namessss"))));
        System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("sex"))));
    }

    /**
     * 全表扫描
     *
     * @throws Exception
     */
    @Test
    public void scanData() throws Exception {
        Scan scan = new Scan();
        //scan.addFamily(Bytes.toBytes("info"));
        //scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"));
        scan.setStartRow(Bytes.toBytes("1234"));
        scan.setStopRow(Bytes.toBytes("123499"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("password"))));
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
            //System.out.println(Bytes.toInt(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("password"))));
            //System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("info2"), Bytes.toBytes("name"))));
        }
    }


    @After
    public void close() throws Exception {
        table.close();
        connection.close();
    }


}