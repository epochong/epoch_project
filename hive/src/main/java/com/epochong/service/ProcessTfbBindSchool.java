package com.epochong.service;


import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.util.*;

public class ProcessTfbBindSchool {
    public static Map<String, String> sid2Rsid = new LinkedHashMap<String, String>();

    public static void main(String[] args) {
//        String filePath = "path/to/your/parquet/file.parquet";


//        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(filePath)).build()) {
//
//            Group group;
//            while ((group = reader.read()) != null) {
//                // 处理每一行数据
//                System.out.println(group);
//                sid2Rsid.put(group.getString("school_id", 1), group.getString("retain_school_id", 2));
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        sid2Rsid.put("1500000100110378301","1500000100110387321");
        sid2Rsid.put("1500000100110387321","1500000100110378301");
        if (sid2Rsid.containsKey("1500000100110378301")) {
            sid2Rsid.remove("1500000100110378301");
            sid2Rsid.put("1500000100110378301","1500000100110387321");
        }

        dealDatas();

    }

    /**
     * 方法一：返回的结构为二维的list，每一行数据的最后一个数据代表终结点，最后一个数据的前面所有数据都是途径节点
     * 返回示例：
     * A B C D A
     * 1 2 3 4
     *
     * 代表一共有两所学校，第一个学校的经历是 A->A,B->A,C->A,D->A
     * 第二个学校的经历是 1->4,2->4,3->4
     * @return
     */
    public static List<List<String>> dealDatas() {

        List<List<String>> res = new ArrayList<List<String>>();
        for (Map.Entry<String, String> entry : sid2Rsid.entrySet()) {
            if (entry.getValue() != null) {
                List<String> list = dealOneData(entry.getKey());
                if (list.size() > 0) {

                    res.add(list);
                }
            }
        }
        System.out.println("tg" + res);
        return res;
    }


    /**
     * 方法二：返回二维list，每一行数据第一个位置代表源节点，第二个位置代表终结点
     * 去除了来源节点和目标节点相同的情况  如 A -> A
     * 样例如下：
     * A B C B D B
     * 1 4 3 4
     * 第一个学校为 A->B,C->B,D->B
     * 第二个学校为1->4,3->4
     * @return
     */
    public static List<List<String>> dealDatas2() {

        List<List<String>> res = new ArrayList<List<String>>();
        for (Map.Entry<String, String> entry : sid2Rsid.entrySet()) {
            if (entry.getValue() != null) {
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
        String firstId = schoolId;

        List<String> res = new ArrayList<String>();
        res.add(schoolId);
        while (sid2Rsid.get(schoolId) != null) {

            schoolId = sid2Rsid.get(schoolId);
            sid2Rsid.put(firstId, null);
//            sid2Rsid.put(oldSchoolId, null);
        }
        if (firstId.equals(schoolId)) {
            return new ArrayList<>();
        }
        res.add(schoolId);

        return res;
    }



    public static List<String> dealOneData2(String schoolId) {
        List<String> res = new ArrayList<String>();
        res.add(schoolId);
        while (sid2Rsid.get(schoolId) != null) {
            res.add(sid2Rsid.get(schoolId));
            schoolId = sid2Rsid.get(schoolId);
            sid2Rsid.put(schoolId, null);
        }

        List<String> newRes = new ArrayList<String>();
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
