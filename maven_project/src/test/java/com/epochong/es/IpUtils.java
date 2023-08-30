package com.epochong.es;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author chongwang11
 * @date 2023-04-18 14:29
 * @description
 */
public class IpUtils {

    public static HashMap<String, HashSet<String>> IP_ATTRIBUTION = new HashMap<String, HashSet<String>>() {{
        put("北京鲁谷", new HashSet<String>() {{
            add("bj");
        }});
        put("北京大族", new HashSet<String>() {{
            add("dx");
        }});
        // 合肥a2/合肥b3
        put("合肥A2", new HashSet<String>() {{
            add("hb");
            add("hf");
            add("ud");
            add("upgrade");
            add("test");
        }});
        put("合肥B3", new HashSet<String>() {{
            add("hb");
            add("hf");
            add("ud");
            add("upgrade");
            add("test");
        }});
        put("北京酒仙桥", new HashSet<String>() {{
            add("jxq");
        }});
        put("上海嘉定", new HashSet<String>() {{
            add("sha");
        }});
    }};

    public static void main(String[] args) throws Exception {
        String hostAttribution = getHostAttribution("192.168.133.36");
        System.out.println(hostAttribution);
    }

    public static String getHostAttribution(String host) throws Exception {
        OkHttpClient client = new OkHttpClient().newBuilder()
                .build();
        RequestBody body = new MultipartBody.Builder().setType(MultipartBody.FORM)
                .addFormDataPart("token", "1XDAxs11fj2")
                .addFormDataPart("ip_address", InetAddress.getByName(host).getHostAddress())
                .build();
        Request request = new Request.Builder()
                .url("https://netops.iflytek.com/v1/api/get_ip_idc")
                .method("POST", body)
                .build();
        try {
            Response response = client.newCall(request).execute();
            JSONObject resp = JSONObject.parseObject(Objects.requireNonNull(response.body()).string());
            if ("200".equals(resp.get("code").toString())) {
                JSONArray result = resp.getJSONArray("result");
                JSONObject jsonObject = JSONObject.parseObject(result.get(0).toString());
                return jsonObject.get("idc_name").toString();
            }
            return null;
        } catch (IOException e) {
            throw new Exception(e);
        }
    }
    public static List<String> getIpAddressList(String host) {
        List<String> result = new ArrayList<>();
        if (StringUtils.isNotEmpty(host)) {
            String ipAddressPattern = "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
            Pattern pattern = Pattern.compile(ipAddressPattern);
            Matcher matcher = pattern.matcher(host);
            while (matcher.find()) {
                result.add(matcher.group());
            }
        }
        return result;
    }

    /**
     * @param ip
     * @param attributionType ip对应的归属地
     * @param cluster         当前集群
     * @return IDC内 跨IDC/专线 不明归属地 公网
     */
    public static String getAttributionType(String ip, String attributionType, String cluster) {
        Boolean isPrivate = false;
        if (ip.startsWith("10.") || ip.startsWith("192.168.")) {
            isPrivate = true;
        }
        String[] split = ip.split("\\.");
        if ("172".equals(split[0]) && Integer.parseInt(split[1]) >= 16 && Integer.parseInt(split[1]) <= 31) {
            isPrivate = true;
        }
        if (isPrivate) {
            if (StringUtils.isEmpty(attributionType)) {
                return "不明归属地";
            }
            if (IP_ATTRIBUTION.get(attributionType).contains(cluster)) {
                return "IDC内";
            } else {
                return "跨IDC/专线";
            }

        } else {
            return "公网";
        }
    }
}
