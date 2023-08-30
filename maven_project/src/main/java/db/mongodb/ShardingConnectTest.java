package db.mongodb;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * @author chongwang11
 * @date 2023-03-03 14:42
 * @description
 */
public class ShardingConnectTest {
    public static ServerAddress seed1 = new ServerAddress("shard341/172.30.41.2:34110,172.30.41.3:34110,172.30.41.4:34110", 34310);
    public static ServerAddress seed2 = new ServerAddress("172.30.41.3", 34310);
    public static String username = "admin";
    public static String password = "TrJenMy4bP4Jnjve";
    //    public static String ReplSetName = "mgset-**********";
    public static String DEFAULT_DB = "admin";
    public static String DEMO_DB = "test";
    public static String DEMO_COLL = "testColl";


    public static MongoClient createMongoDBClient() {
        // 构建Seed列表
        List<ServerAddress> seedList = new ArrayList<ServerAddress>();
        seedList.add(seed1);
        //seedList.add(seed2);
        // 构建鉴权信息
        List<MongoCredential> credentials = new ArrayList<MongoCredential>();
        credentials.add(MongoCredential.createScramSha1Credential(username, DEFAULT_DB,
                password.toCharArray()));
        // 构建操作选项，requiredReplicaSetName属性外的选项根据自己的实际需求配置，默认参数满足大多数场景
        MongoClientOptions options = MongoClientOptions.builder().socketTimeout(2000).connectionsPerHost(1).build();
        return new MongoClient(seedList, credentials, options);
    }

    public static void main(String[] args) {
        MongoClient mongo = createMongoDBClient();
        MongoDatabase db = mongo.getDatabase("local");
        MongoCollection<Document> collection = db.getCollection("oplog.rs");
        System.out.println("集合选择成功 oplog.rs");
        // 获取 iterable 对象
        FindIterable<Document> iterDoc = collection.find();
        //FindIterable<Document> iterDoc = collection.findone();
        int i = 1;
        // 获取迭代器
        Iterator it = iterDoc.iterator();
        while (it.hasNext()) {
            Document next = (Document) it.next();
            String s = next.toJson();
            System.out.println(next);
            BsonTimestamp ts = next.get("ts", BsonTimestamp.class);
            System.out.println(ts.getTime());
            break;
        }
    }

    public static MongoClient createMongoDBClientWithURI() {
        // 另一种通过URI初始化
        // mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]
        MongoClientURI connectionString = new MongoClientURI("mongodb://" + username + ":" + password + "@"
                + seed1 + "," + seed2 + "/" + DEFAULT_DB);
        return new MongoClient(connectionString);
    }
//    public static void main(String args[]) {
//        MongoClient client = createMongoDBClientWithURI();
//        // or
//        // MongoClient client = createMongoDBClientWithURI();
//        try {
//            // 取得Collection句柄
//            MongoDatabase database = client.getDatabase(DEMO_DB);
//            MongoCollection<Document> collection = database.getCollection(DEMO_COLL);
//            // 插入数据
//            Document doc = new Document();
//            String demoname = "JAVA:" + UUID.randomUUID();
//            doc.append("DEMO", demoname);
//            doc.append("MESG", "Hello AliCoudDB For MongoDB");
//            collection.insertOne(doc);
//            System.out.println("insert document: " + doc);
//            // 读取数据
//            BsonDocument filter = new BsonDocument();
//            filter.append("DEMO", new BsonString(demoname));
//            MongoCursor<Document> cursor = collection.find(filter).iterator();
//            while (cursor.hasNext()) {
//                System.out.println("find document: " + cursor.next());
//            }
//        } finally {
//            // 关闭Client，释放资源
//            client.close();
//        }
//        return;
//    }
}