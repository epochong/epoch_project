package db.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonTimestamp;
import org.bson.Document;

import java.sql.Timestamp;
import java.util.Iterator;

/**
 * @author chongwang11
 * @date 2023-02-27 09:32
 * @description
 */
public class ShardMain {


    public static void main(String[] args) {
        getReadingsBetween("", new Timestamp(1645931422), new Timestamp(1677467422));
    }

    public static String getReadingsBetween(String type, Timestamp startTime, Timestamp endTime) {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder().serverSelectionTimeout(5000);
//        String uri = "mongodb://" + "admin" + ":" + "TrJenMy4bP4Jnjve" + "@" + "172.30.41.2:34310,172.30.41.3:34310,172.30.41.4:34310" + "/"
//              + "local" + "?authSource=" + "admin";
        String uri = "mongodb://" + "admin" + ":" + "TrJenMy4bP4Jnjve" + "@" + "172.30.41.2:34110,172.30.41.3:34110,172.30.41.4:34110" + "/"
                + "local" + "?authSource=" + "admin";
        MongoClientURI clientURI = new MongoClientURI(uri, builder);
        MongoClient mongo = new MongoClient(clientURI);
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
        return null;
    }
}
