package com.epochong.es;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import okhttp3.*;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;

/**
 * @author chongwang11
 * @date 2023-03-20 17:00
 * @description
 */
public class HttpTest {

    @Test
    public void test() throws IOException {
        OkHttpClient client = new OkHttpClient().newBuilder()
                .build();
        RequestBody body = new MultipartBody.Builder().setType(MultipartBody.FORM)
                .addFormDataPart("token","1XDAxs11fj2")
                .addFormDataPart("ip_address","10.254.12.1645")
                .build();
        Request request = new Request.Builder()
                .url("https://netops.iflytek.com/v1/api/get_ip_idc")
                .method("POST", body)
                .build();
        try {
            Response response = client.newCall(request).execute();
            //if ()
            JSONObject resp = JSONObject.parseObject(Objects.requireNonNull(response.body()).string());
            System.out.println(resp.toJSONString());
            if ("200".equals(resp.get("code").toString())) {
                JSONArray result = resp.getJSONArray("result");
                JSONObject jsonObject = JSONObject.parseObject(result.get(0).toString());
                System.out.println(jsonObject.get("idc_name"));
            }

        } catch (IOException e) {

        }
    }

    @Test
    public void testIp() throws UnknownHostException {
        //获取本机IP地址
        System.out.println(InetAddress.getLocalHost().getHostAddress());
        //获取www.baidu.com的地址
        System.out.println(InetAddress.getByName("mysql.mysql-bj02-hl6pxl.svc.bjb.ipaas.cn"));
        //获取www.baidu.com的真实IP地址
        System.out.println(InetAddress.getByName("mysql.mysql-bj02-hl6pxl.svc.bjb.ipaas.cn").getHostAddress());
        System.out.println(InetAddress.getByName("10.105.46.46").getHostAddress());
    }
}
