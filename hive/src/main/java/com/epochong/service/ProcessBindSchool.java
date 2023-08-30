package com.epochong.service;


import java.util.*;

/**
 * @Author: ddjia
 * @Date: 2023/8/25 10:40
 */

public class ProcessBindSchool {

    public static class Node {
        String sid;
        String updateTime;

        public Node(String sid, String updateTime) {
            this.sid = sid;
            this.updateTime = updateTime;
        }

        @Override
        public int hashCode() {
            return (this.sid + this.updateTime).hashCode();
        }

        @Override
        public boolean equals(Object obj) {

            if (this == obj) {
                return true;
            }

            Node node = (Node) obj;
            return node.sid.equals(this.sid) && node.updateTime.equals(this.updateTime);
        }
    }

    public static Map<Node, String> sidNodeMap = new LinkedHashMap<>();
    public static Map<String, String> sid2Time = new LinkedHashMap<>();

    public static Map<String, String> sid2Rsid = new LinkedHashMap<>();

    public static void setValue(String sid, String rsid) {
        if (sid2Rsid.containsKey(sid)) {
            sid2Rsid.remove(sid);
            sid2Rsid.put(sid, rsid);
        } else {
            sid2Rsid.put(sid, rsid);
        }
    }

    /**
     * 设置sid和对应updateTime时间的map
     */
    public static void setTime() {
        for (Map.Entry<Node, String> entry : sidNodeMap.entrySet()) {
            sid2Time.put(entry.getKey().sid, entry.getKey().updateTime);
        }
    }

    public static void setValue(String sid, String rsid, String updateTime) {

        if (sidNodeMap.containsKey(new Node(sid, sid2Time.get(sid)))) {
            sidNodeMap.remove(new Node(sid, sid2Time.get(sid)));

            sidNodeMap.put(new Node(sid, updateTime), rsid);
        } else {
            sidNodeMap.put(new Node(sid, updateTime), rsid);
        }
    }


    /**
     * 方法一：返回的结构为二维的list，每一行数据的最后一个数据代表终结点，最后一个数据的前面所有数据都是途径节点
     * 返回示例：
     * A B C D A
     * 1 2 3 4
     * <p>
     * 代表一共有两所学校，第一个学校的经历是 A->A,B->A,C->A,D->A
     * 第二个学校的经历是 1->4,2->4,3->4
     *
     * @return
     */
    public static List<List<String>> dealDatas() {
        List<List<String>> res = new ArrayList<>();
        for (Map.Entry<String, String> entry : sid2Rsid.entrySet()) {
            if (entry.getValue() != null) {
                List<String> oneData = dealOneData(entry.getKey());
                if (oneData.size() > 0) {
                    res.add(oneData);

                }
            }
        }
        return res;
    }

    public static List<List<String>> dealDatasWithNode() {
        List<List<String>> res = new ArrayList<>();
        for (Map.Entry<Node, String> entry : sidNodeMap.entrySet()) {
            if (entry.getValue() != null) {
                List<List<String>> cycleList = dealOneDataWithNode(entry.getKey());
                if (cycleList.size() > 0) {
                    res.addAll(cycleList);
                }
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
     * 第一个学校为 A->B,C->B,D->B
     * 第二个学校为1->4,3->4
     * @return
     */
//    public static List<List<String>> dealDatas2() {
//
//        List<List<String>> res = new ArrayList<List<String>>();
//        for (Map.Entry<String, String> entry : sid2Rsid.entrySet()) {
//            if (entry.getValue() != null) {
//                res.add(dealOneData2(entry.getKey()));
//            }
//        }
//        return res;
//    }


    /**
     * res 最后一个位置就是终结点
     *
     * @param schoolId
     * @return
     */
    public static List<String> dealOneData(String schoolId) {
        String firstId = schoolId;
        List<String> res = new ArrayList<>();
        res.add(schoolId);
        while (sid2Rsid.get(schoolId) != null) {
            schoolId = sid2Rsid.get(schoolId);
            sid2Rsid.put(firstId, null);
        }
        if (firstId.equals(schoolId)) {
            return new ArrayList<>();
        }
        res.add(schoolId);

        return res;
    }


    /**
     * a c a_b_updateTime
     * b c b_c_updateTime
     *
     * @param node
     * @return
     */
    public static List<List<String>> dealOneDataWithNode(Node node) {
        List<List<String>> finalList = new ArrayList<>();
        List<String> res = new ArrayList<>();
        while (sidNodeMap.get(node) != null) {
            String schoolId = sidNodeMap.get(node);
            res.add(node.sid);
            sidNodeMap.put(node, null);
            node = new Node(schoolId, sid2Time.get(schoolId));
        }
        res.add(node.sid);
        for (int i = 0; i < res.size() - 1; i++) {
            if (res.get(i).equals(res.get(res.size() - 1))) {
                continue;
            }
            List<String> oneDataList = new ArrayList<>();
            oneDataList.add(res.get(i));
            oneDataList.add(res.get(res.size() - 1));
            oneDataList.add(sid2Time.get(res.get(i)));
            finalList.add(oneDataList);
        }
        return finalList;
    }

//    public static List<String> dealOneData2(String schoolId) {
//        List<String> res = new ArrayList<String>();
//        res.add(schoolId);
//        while (sid2Rsid.get(schoolId) != null) {
//            res.add(sid2Rsid.get(schoolId));
//            schoolId = sid2Rsid.get(schoolId);
//            sid2Rsid.put(schoolId, null);
//        }
//
//        List<String> newRes = new ArrayList<String>();
//        for (int i = 0; i < res.size() - 1; i++) {
//            if (res.get(i).equals(res.get(res.size() - 1))) {
//                continue;
//            }
//            newRes.add(res.get(i));
//            newRes.add(res.get(res.size() - 1));
//        }
//
//        return newRes;
//    }

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

        setValue("a", "b", "1");
        setValue("b", "c", "2");
        setValue("c", "b", "3");

        setValue("m", "n", "4");
        setValue("n", "m", "5");

        setValue("d", "e", "6");
        setValue("e", "f", "7");
        setValue("f", "g", "8");

        setValue("d", "r", "9");
        setValue("d", "l", "10");

        setTime();
        //a b 1
        //c b 3
        List<List<String>> lists = dealDatasWithNode();
        System.out.println(lists);

    }
}
