package db.mysql;

import util.MysqlUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author chongwang11
 * @date 2023-02-27 16:45
 * @description
 */
public class Main {
    public static void main(String[] args) throws SQLException {
        printBinlogExpireLogsDays();
    }

    public static void printBinlogExpireLogsDays() throws SQLException {
        String jdbc = "jdbc:mysql://172.30.41.4:3308/test_dataex?useSSL=false";
        String userName = "manager";
        String pass = "manager!@#";
        MysqlUtil mysqlUtil = new MysqlUtil(jdbc, userName, pass);
        Connection connection = mysqlUtil.getConnection();

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("SHOW GLOBAL VARIABLES LIKE 'expire_logs_days'");
        if (rs.next()) {
            int expireLogsDays = rs.getInt(2);
            System.out.println("Binlog expire time (in days): " + expireLogsDays);
        }
    }

    public static void printBinlogList() {
        String jdbc = "jdbc:mysql://172.30.41.4:3308/test_dataex?useSSL=false";
        String userName = "manager";
        String pass = "manager!@#";
        List<String> binlogNames = new ArrayList<>();
        MysqlUtil mysqlUtil = new MysqlUtil(jdbc, userName, pass);
        try {
            Connection connection = mysqlUtil.getConnection();
            Statement statement = connection.createStatement();
            String sql = "show binary logs";
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                binlogNames.add(resultSet.getString("Log_name"));
            }
            binlogNames.sort(Comparator.reverseOrder());
            binlogNames.forEach(System.out::println);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
        }
    }
}
