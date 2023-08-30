//package service;
//
///**
// * @author chongwang11
// * @date 2023-04-19 15:43
// * @description
// */
//import java.io.IOException;
//import java.io.FileInputStream;
//import java.io.BufferedInputStream;
//import java.sql.Timestamp;
//import java.util.Properties;
//import java.util.concurrent.TimeUnit;
//import com.mysql.jdbc.Driver;
//import com.mysql.jdbc.log.Log;
//import com.mysql.jdbc.log.LogFactory;
//import com.mysql.jdbc.log.NullLogger;
//import com.mysql.jdbc.BinlogEventListener;
//import com.mysql.jdbc.BinlogEventV4;
//import com.mysql.jdbc.MysqlIO;
//import com.mysql.jdbc.MysqlIOConstants;
//import com.mysql.jdbc.ReplicationBasedBinlogParser;
//import com.mysql.jdbc.ReplicationConnection;
//import com.mysql.jdbc.ReplicationDriver;
//import com.mysql.jdbc.ReplicationEvent;
//import com.mysql.jdbc.ReplicationEvent.Metadata;
//
//public class BinlogTimestampQuery {
//
//    private static final String JDBC_URL = "jdbc:mysql://<hostname>:<port>/<database>";
//    private static final String USERNAME = "<username>";
//    private static final String PASSWORD = "<password>";
//
//    public static void main(String[] args) throws Exception {
//
//        Properties props = new Properties();
//        props.setProperty("user", USERNAME);
//        props.setProperty("password", PASSWORD);
//
//        ReplicationDriver driver = new ReplicationDriver();
//        ReplicationConnection conn = (ReplicationConnection)driver.connect(JDBC_URL, props);
//
//        // Parse binlog file
//        String binlogFileName = "<binlog-file-name>";
//        long binlogPosition = 4L; // Start reading from the beginning of the file
//        ReplicationBasedBinlogParser parser = new ReplicationBasedBinlogParser();
//        parser.parse(new BufferedInputStream(new FileInputStream(binlogFileName)), binlogPosition);
//
//        // Start processing binlog events
//        while (true) {
//            ReplicationEvent event = parser.getNextEvent(MysqlIOConstants.UNLIMITED_BYTE_ARRAY_LENGTH);
//            if (event == null) {
//                break; // End of binlog file
//            }
//            Metadata metadata = event.getMetadata();
//            long timestamp = metadata.getTimestamp();
//            Timestamp ts = new Timestamp(timestamp * 1000);
//            System.out.println("Timestamp: " + ts);
//        }
//    }
//}
