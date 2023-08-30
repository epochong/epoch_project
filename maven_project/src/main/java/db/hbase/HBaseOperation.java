package db.hbase;

import db.ftp.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chongwang11
 * @date 2023-05-09 15:33
 * @description
 */
@Slf4j
public class HBaseOperation {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    // 建立链接
    public static void init() {
        configuration = HBaseConfiguration.create();
        //configuration.set("hbase.rootdir", "hdfs://10.1.86.77:60010/hbase");
        configuration.set("hbase.zookeeper.quorum", "10.1.86.75,10.1.86.76,10.1.86.77");
        //configuration.set("hbase.zookeeper.property.clientPort", "60010");
        //configuration.set("zookeeper.znode.parent", "/hbase");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 关闭连接
    public static void close() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (connection != null) {
                connection.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 建立表
    public static void createTable(String myTableName, String[] colFamily) throws IOException {
        init();
        TableName tableName = TableName.valueOf(myTableName);
        if (admin.tableExists(tableName)) {
            System.out.println("table is exist");
        } else {
            List<ColumnFamilyDescriptor> colFamilyList = new ArrayList<>();
            TableDescriptorBuilder tableDesBuilder = TableDescriptorBuilder.newBuilder(tableName);
            for (String str : colFamily) {
                ColumnFamilyDescriptor colFamilyDes = ColumnFamilyDescriptorBuilder.newBuilder(str.getBytes()).build();
                colFamilyList.add(colFamilyDes);
            }
            TableDescriptor tableDes = tableDesBuilder.setColumnFamilies(colFamilyList).build();
            admin.createTable(tableDes);
        }
        close();
    }

    // 删除表
    public static void deleteTable(String myTableName) throws IOException {
        init();
        TableName tableName = TableName.valueOf(myTableName);
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("delete " + myTableName + " successful!");
        }
        close();
    }

    // 列出数据库中所有的表
    public static void listTables() throws IOException {
        init();
        for (TableName table : admin.listTableNames()) {
            System.out.println(table);
        }
        close();
    }

    // 向表中添加一个列族
    public static void addColumnFamily(String myTableName, String colFamily) throws IOException {
        init();
        TableName tableName = TableName.valueOf(myTableName);
        if (admin.tableExists(tableName)) {
            TableDescriptor tableDes = TableDescriptorBuilder.newBuilder(tableName).build();
            ColumnFamilyDescriptor colFamilyDes = ColumnFamilyDescriptorBuilder.newBuilder(colFamily.getBytes()).build();
            admin.addColumnFamily(tableName, colFamilyDes);
            System.out.println("add " + colFamily + " successful!");
        }
        close();
    }

    // 从表中移除一个列族
    public static void removeColumnFamily(String myTableName, String colFamily) throws IOException {
        init();
        TableName tableName = TableName.valueOf(myTableName);
        if (admin.tableExists(tableName)) {
            TableDescriptor tableDes = TableDescriptorBuilder.newBuilder(tableName).build();
            admin.deleteColumnFamily(tableName, colFamily.getBytes());
            System.out.println("remove " + colFamily + " successful!");
        }
        close();
    }

    // 描述表的详细信息
    public static void describeTable(String myTableName) throws IOException {
        init();
        TableName tableName = TableName.valueOf(myTableName);
        if (admin.tableExists(tableName)) {
            ColumnFamilyDescriptor[] colFamilies = admin.getDescriptor(tableName).getColumnFamilies();
            System.out.println("==============describe  " + myTableName + " ================");
            for (ColumnFamilyDescriptor colFamily : colFamilies) {
                System.out.println(colFamily.getNameAsString());
                System.out.println(colFamily.getBlocksize());
                System.out.println(colFamily.getConfigurationValue(myTableName));
                System.out.println(colFamily.getMaxVersions());
                System.out.println(colFamily.getEncryptionType());
                System.out.println(colFamily.getTimeToLive());
                System.out.println(colFamily.getDFSReplication());
                System.out.println();
            }
        }
        close();
    }

    //添加数据
    public void insert() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.1.86.75,10.1.86.76,10.1.86.77");
        //通过配置获取一个链接
        Connection connection = null;
        Table table = null;
        try {
            //通过配置获取一个链接
            connection = ConnectionFactory.createConnection(conf);
            //通过链接获取一个table
            table = connection.getTable(TableName.valueOf("mystudent"));
            //然后在构建一个put对象，每行数据都是一个Put对象,创建行健的时候是通过行健创建的
            Put put = new Put(Bytes.toBytes("s001"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("Tom"));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes("man"));
            put.addColumn(Bytes.toBytes("grade"), Bytes.toBytes("yuwen"), Bytes.toBytes("89"));
            table.put(put);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
                connection.close();
            } catch (Exception e2) {
                e2.printStackTrace();
            }

        }

    }

    //获取数据
    public static void testGet() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.1.86.75,10.1.86.76,10.1.86.77");
        //通过配置获取一个链接
        Connection connection = null;
        Table table = null;
        try {
            //通过配置获取一个链接
            connection = ConnectionFactory.createConnection(conf);
            //通过链接获取一个table
            table = connection.getTable(TableName.valueOf("quiver-2023-05-10-23"));
            //构造一个get对象

            List<String> rowKeyList = new ArrayList<String>() {{
                add("b625d4e4hsc0001bd6a@cs188063717b0f011902");
                add("b8b8c92ahsc0001bd93@cs18806405060f011902");
                add("bfd247e5hsc0001bde7@cs188065018a4f01190");
                add("bfd247e5hsc0001bde7@cs188065018a4f011902");
                add("bfd247e5hsc0001bde7@cs188065018a4f01190");
            }};
            List<Get> getList = new ArrayList();
            for (String rowkey : rowKeyList) {//把rowkey加到get里，再把get装到list中
                Get get = new Get(Bytes.toBytes(rowkey));
                getList.add(get);
            }
            Result[] results = table.get(getList);//重点在这，直接查getList
            System.out.println(results.length);
            for (Result result : results) {//对返回的结果集进行操作
                log.info(result.rawCells().length + "bbbb");
                if (result.rawCells().length == 0) {
                    log.error("is empty nnnnn");
                }
                for (Cell kv : result.rawCells()) {
                    String key = Bytes.toString(CellUtil.cloneRow(kv));
                    System.out.println(key);
                    System.out.println("===================");
                    String value = Bytes.toString(CellUtil.cloneValue(kv));
                    System.out.println(value);
                    System.out.println("---------------");
                }
                System.out.println("*********");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
                connection.close();
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }

    public static Connection getConnection() {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "10.1.86.75,10.1.86.76,10.1.86.77");
            return ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void testScan() {
        //通过配置获取一个链接
        Connection connection = getConnection();
        Table table = null;
        try {
            //通过链接获取一个table
            table = connection.getTable(TableName.valueOf("quiver-2023-05-10-23"));
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("r"), Bytes.toBytes("a"));
            ResultScanner scanner = table.getScanner(scan);
            //指定返回列，如果不指定返回列那么就是返回正行的数据
            while (true) {
                Result next = scanner.next();
//                System.out.println(next.toString());
                for (Cell kv : next.rawCells()) {
                    String key = Bytes.toString(CellUtil.cloneRow(kv));
                    System.out.println(StringUtil.rowKey2Sid(key));
                    String value = Bytes.toString(CellUtil.cloneValue(kv));
                    //System.out.println(value);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
                connection.close();
            } catch (Exception e2) {
                e2.printStackTrace();
            }

        }
    }
}
