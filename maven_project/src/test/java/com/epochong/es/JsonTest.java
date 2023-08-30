package com.epochong.es;

import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import java.util.HashMap;

/**
 * @author chongwang11
 * @date 2023-04-21 16:31
 * @description
 */
public class JsonTest {
    @Test
    public void jsonTest() {
        HashMap hashMap = JSONObject.parseObject("{\"cluster\":\"odeon-dev-es6\",\"attribution\":\"合肥B3\",\"attributionType\":\"跨IDC/专线\"}", HashMap.class);
        System.out.println(hashMap);
    }
}
