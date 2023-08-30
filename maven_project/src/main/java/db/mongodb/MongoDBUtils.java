package db.mongodb;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chongwang11
 * @date 2023-06-20 09:55
 * @description
 */
@Slf4j
public class MongoDBUtils {
    static MongoClientOptions.Builder builder = new MongoClientOptions.Builder().serverSelectionTimeout(5000);

    public static MongoClient get30042() {
        String uri = "mongodb://" + "u_app" + ":" + "bmvMC4XC7rc5JR8d" + "@" + "172.30.41.2:30042,172.30.41.3:30042,172.30.41.4:30042" + "/" + "testdataex" + "?authSource=" + "admin";
        MongoClientURI clientURI = new MongoClientURI(uri, builder);
        return new MongoClient(clientURI);
    }

    public static MongoClient createMongoDBClient(ServerAddress seed, String db, String userName, String password) {
        // 构建Seed列表
        List<ServerAddress> seedList = new ArrayList<>();
        seedList.add(seed);
        //seedList.add(seed2);构建鉴权信息
        List<MongoCredential> credentials = new ArrayList<>();
        credentials.add(MongoCredential.createScramSha1Credential(userName, db,
                password.toCharArray()));
        // 构建操作选项，requiredReplicaSetName属性外的选项根据自己的实际需求配置，默认参数满足大多数场景
        MongoClientOptions options = MongoClientOptions.builder().socketTimeout(2000).connectionsPerHost(1).build();
        return new MongoClient(seedList, credentials, options);
    }

    public static Document getDocument() {
        Document document = new Document();
        document.put("id", new ArrayList<String>() {{
            add(" ");
        }});
        //添加一条数据
        return document;
    }

    public static void delete() {
        //获取链接
        MongoClient mc = new MongoClient("localhost", 27017);
        //获取库对象
        MongoDatabase db = mc.getDatabase("student");
        //获取集合对象
        MongoCollection<Document> collection = db.getCollection("student");
//		Bson exists=Filters.exists("age",false);
        Bson gt = Filters.gt("age", 100);
//		Bson age=Filters.exists("age");
        DeleteResult deleteMany = collection.deleteMany(gt);
        System.out.println(deleteMany);
        mc.close();
    }

    public static void modify() {
        //获取链接
        MongoClient mc = new MongoClient("localhost", 27017);
        //获取库对象
        MongoDatabase db = mc.getDatabase("student");
        //获取集合对象
        MongoCollection<Document> collection = db.getCollection("student");
        //			Bson eq = Filters.eq("name","吕布");
        //修改一条数据
//				UpdateResult updateOne = collection.updateOne(eq, new Document("$set",new Document("age",99)),new UpdateOptions().upsert(true));
//				System.out.println(updateOne);

        //修改多条数据
        Bson and = Filters.and(Filters.gt("age", 20), Filters.lte("age", 100));
//				UpdateResult updateMany = collection.updateMany(eq, new Document("$set",new Document("age",3)));
        UpdateResult updateMany = collection.updateMany(and, new Document("$inc", new Document("age", 100)));
        System.out.println(updateMany);
        mc.close();

    }

    public static void search() {
//        Bson b = Filters.regex("name", "张");//查出所有姓张的数据
//        //跳过第0条数据，一次看三条数据
//        FindIterable<Document> b = collection.find().skip(0).limit(3);

        //获取链接
        MongoClient mc = new MongoClient("localhost", 27017);
        //获取库对象
        MongoDatabase db = mc.getDatabase("student");
        //获取集合对象
        MongoCollection<Document> collection = db.getCollection("student");

        // 添加条件
        Bson eq = Filters.regex("name", "张");
        Document document = new Document("birthday", -1);
        // .limit(2).skip(2)
        FindIterable<Document> find = collection.find(eq).sort(document);

        MongoCursor<Document> iterator = find.iterator();


        while (iterator.hasNext()) {

            Document next = iterator.next();
            next.getString("name");
            next.getString("sex");
            next.getInteger("sid");
        }

        mc.close();
    }

    public static void insert() {
        //获取链接
        MongoClient mc = get30042();
        //获取库对象
        MongoDatabase db = mc.getDatabase("testdataex");
        //获取集合对象
        MongoCollection<Document> collection = db.getCollection("test_bigdata_16");
        //新增
        collection.insertOne(getDocument());

        //添加多条数据
//        Document document1= new Document();
//        document1.put("name", "钟无艳");
//        document1.put("sex", "女");
//        document1.put("age", 18);
//        document1.put("birthday",new Date());
//
//        Document document2= new Document();
//        document2.put("name", "貂蝉");
//        document2.put("sex", "女");
//        document2.put("age", 1);
//        document2.put("birthday",new Date());
//
//        ArrayList<Document> list = new ArrayList<Document>();
//        list.add(document1);
//        list.add(document2);
//        collection.insertMany(list);
        mc.close();
        log.info("insert success!");

    }

    public static void main(String[] args) {
        insert();
    }
}
