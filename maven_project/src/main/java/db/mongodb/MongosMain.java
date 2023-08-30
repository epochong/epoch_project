package db.mongodb;

import com.mongodb.AggregationOptions;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author chongwang11
 * @date 2023-02-27 09:32
 * @description
 */
public class MongosMain {


    public static void main(String[] args) {
//        getReadingsBetween();
//        printOplogExpireTime();
        countDocuments();
    }


    public static void countDocuments() {
        MongoClient mongo = getMongoClient();
        MongoDatabase db = mongo.getDatabase("inventory");
        MongoCollection<Document> collection = db.getCollection("customers");
        collection.countDocuments();
        List<Document> pipeline = Arrays.asList(
                new Document("$group", new Document("_id", null).append("count", new Document("$sum", 1)))
        );
        long count = collection.aggregate(pipeline).first().getInteger("count");
        System.out.println(count);

    }

    public static void printOplogExpireTime() {
        MongoClient mongo = getMongoClient();
        MongoDatabase adminDatabase = mongo.getDatabase("local");
        Document oplogTtlDoc = adminDatabase.runCommand(new Document("collStats", "oplog.rs").append("scale", 1));
        int oplogTtlSeconds = oplogTtlDoc.getInteger("ttl");
        System.out.println("Oplog expire time (in seconds): " + oplogTtlSeconds);
    }

    public static String getReadingsBetween() {
        MongoClient mongo = getMongoClient();
        MongoDatabase db = mongo.getDatabase("config");
        MongoCollection<Document> collection = db.getCollection("shards");
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
        }
        return null;
    }

//    public static void printOplogStash() {
//        MongoClient mongo = getMongoClient();
//        MongoDatabase database = mongo.getDatabase("rs");
//
//        MongoCollection<Document> collection = database.getCollection("oplog");
//
//        BsonDocument oplog = collection.find().maxTime(TimeUnit.SECONDS).first();
//
//        long retentionDuration = (oplog.getTimestamp() - System.currentTimeMillis()) / 1000;
//
//        System.out.println("Oplog retention duration: " + retentionDuration + " seconds");
//
//    }

    public static MongoClient getMongoClient() {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder().serverSelectionTimeout(5000);
//        String uri = "mongodb://" + "admin" + ":" + "TrJenMy4bP4Jnjve" + "@" + "172.30.41.2:34310,172.30.41.3:34310,172.30.41.4:34310" + "/"
////                + "local" + "?authSource=" + "admin";
        String uri = "mongodb://" + "admin" + ":" + "TrJenMy4bP4Jnjve" + "@" + "172.30.41.2:34310,172.30.41.3:34310,172.30.41.4:34310" + "/"
                + "local" + "?authSource=" + "admin";
        MongoClientURI clientURI = new MongoClientURI(uri, builder);
        return new MongoClient(clientURI);
    }
}
