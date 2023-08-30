package com.epochong.service;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.util.*;

/**
 * @Author: chongwang11
 * @Date: 2023/8/24 21:55
 */
public class Main {
    public static Map<String, String> sid2Rsid = new LinkedHashMap<>();


    public static void main(String[] args) {
        String filePath = "/Users/epochong/Downloads/iflytek/数据/edu/test.parquet";

        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(filePath)).build()) {

            Group group;
            /*while ((group = reader.read()) != null) {
                // 处理每一行数据
                sid2Rsid.put(group.getString("school_id", 0), group.getString("retain_school_id", 0));
            }*/
            sid2Rsid.put("1500000100110387321","1500000100110378301");
            sid2Rsid.put("1500000100110378301","1500000100110378302");
            sid2Rsid.put("1500000100110378302","1500000100110387321");

            List<List<String>> lists = dealDatas2();
            for (List<String> list : lists) {
                for (String s : list) {
                    System.out.print(s + " ");
                }
                System.out.println();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 方法一：返回的结构为二维的list，每一行数据的最后一个数据代表终结点，最后一个数据的前面所有数据都是途径节点
     * 返回示例：
     * A B C D A
     * 1 2 3 4
     *
     * 代表一共有两个学生，第一个学生的经历是 A->A,B->A,C->A,D->A
     * 第二个学生的经历是 1->4,2->4,3->4
     * @return
     */
    public static List<List<String>> dealDatas() {

        List<List<String>> res = new ArrayList<>();
        for (Map.Entry<String, String> entry : sid2Rsid.entrySet()) {
            if (entry.getValue() != null) {
                res.add(dealOneData(entry.getKey()));
            }
        }
        return res;
    }


    /**
     * 方法二：返回二维list，每一行数据第一个位置代表源节点，第二个位置代表终结点
     * 去除了来源节点和目标节点相同的情况  如 A -> A
     * 样例如下：
     * A B C B D B
     * 1 4 3 4
     * 第一个学生为 A->B,C->B,D->B
     * 第二个学生为1->4,3->4
     * @return
     */
    public static List<List<String>> dealDatas2() {

        List<List<String>> res = new ArrayList<>();
        for (Map.Entry<String, String> entry : sid2Rsid.entrySet()) {
            if (!entry.getValue().equals("")) {
                res.add(dealOneData2(entry.getKey()));
            }
        }
        return res;
    }



    /**
     * res 最后一个位置就是终结点
     * @param schoolId
     * @return
     */
    public static List<String> dealOneData(String schoolId) {
        List<String> res = new ArrayList<>();
        String retainId = sid2Rsid.get(schoolId);
        res.add(schoolId);
        while (retainId != null && !"".equals(retainId)) {;
            sid2Rsid.put(schoolId, "");
            retainId = sid2Rsid.get(retainId);
        }

        return res;
    }

    public static List<String> dealOneData2(String schoolId) {
        List<String> res = dealOneData(schoolId);

        List<String> newRes = new ArrayList<>();
        for (int i = 0; i < res.size() - 1; i++) {
            if (res.get(i).equals(res.get(res.size() - 1))) {
                continue;
            }
            newRes.add(res.get(i));
            newRes.add(res.get(res.size() - 1));
        }

        return newRes;
    }
}
