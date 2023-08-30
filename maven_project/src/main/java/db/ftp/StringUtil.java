package db.ftp;

import org.apache.hadoop.io.MD5Hash;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author chongwang11
 * @date 2023-05-10 14:07
 * @description
 */
public class StringUtil {

    public static void main(String[] args) {
        System.out.println(rowKey2Sid("hsc0001bd6a@cs188063717b0f011902"));
        System.out.println(sid2RowKey("hsc0001bd6a@cs188063717b0f011902"));
        System.out.println(sid2TableName("hsc0001bd6a@cs118063717b0f011902"));
    }

    public static String rowKey2Sid(String rowKey) {
        return rowKey.substring(8);
    }

    public static String sid2RowKey(String sid) {
        return MD5Hash.digest(sid).toString().substring(0, 8) + sid;
    }
    public static String sid2TableName(String sid) {
        long ts = null != sid && sid.length() == 32 ? Long.parseLong(sid.substring(14, 25), 16) : -1L;
        System.out.println(sid.substring(14, 25));
        System.out.println(Long.parseLong(sid.substring(14, 25), 16));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
        Date date = new Date(ts);
        return sdf.format(date);
    }
}
