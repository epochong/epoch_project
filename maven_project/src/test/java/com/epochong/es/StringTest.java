package com.epochong.es;

import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chongwang11
 * @date 2023-03-17 15:30
 * @description
 */
public class StringTest {


    @Test
    public void testToArray() {
        List<String> list = new ArrayList<>();
        String str = "{\"color\":\"red\",\"www\":{\"path\":\"www.baidu.com\",\"pv\":2143547587},\"we\":\"\"}";
        list.add(str);
        String[] strings = list.toArray(new String[0]);
        for (String s : strings) {
            System.out.println(s);
        }

    }
    @Test
    public void testContains() {
        String[] fields = new String[0];
        System.out.println(fields.length);
        String json = "{\"user_id\": \"2300000017001612991\",\"project_id\": \"1539078601625153531\",\"_id\": \"64ae14fd2bd5cf14f925c611\"}";
        JSONObject jsonObject = JSONObject.parseObject(json);
        System.out.println(jsonObject.size());
        System.out.println("".contains(""));
    }
    @Test
    public void testReplaceAll() {
        String str = "'|'";
        System.out.println(str.replaceAll("\\|", "or"));
    }

    @Test
    public void testDude() {
        System.out.println(Integer.parseInt("-10") + 2);
    }

    @Test
    public void testSubstring() {
        String path = "/user/wangchong/name";
        System.out.println(path.substring(path.lastIndexOf(File.separator) + 1).split("\\.")[0]);
    }
    @Test
    public void testNullString() {
        Object s = null;
        System.out.println(String.valueOf(s));
        System.out.println(Long.parseLong(String.valueOf(s)));
    }


}
