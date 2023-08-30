package com.epochong.es;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author chongwang11
 * @date 2023-04-18 14:11
 * @description
 */
public class MatchTest {

    @Test
    public void testIp() {
        String ipString = "mysql.mysql-hf04-cjxwsn.svc.hfb.ipaas.cn:8066";
        List<String> result = new ArrayList<>();
        if (StringUtils.isNotEmpty(ipString)) {
            String ipAddressPattern = "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";
            Pattern pattern = Pattern.compile(ipAddressPattern);
            Matcher matcher = pattern.matcher(ipString);
            while (matcher.find()) {
                result.add(matcher.group());
            }
        }
        System.out.println(String.join(",", result));
    }

    @Test
    public void getHostIp() throws Exception {
        String host = "mysql.mysql-hf04-cjxwsn.svc.hfb.ipaas.cn";
        host = host.split(":")[0];
        List<String> ipAddressList = IpUtils.getIpAddressList(host);
        if (CollectionUtils.isEmpty(ipAddressList)) {
            System.out.println(InetAddress.getByName(host).getHostAddress());
        } else {
            System.out.println(InetAddress.getByName(ipAddressList.get(0)).getHostAddress());
        }
    }

    @Test
    public void testTables() {
        ArrayList<String> tableNames = new ArrayList<>();
        tableNames.add("chongwang");
        tableNames.add("chongwang11");
        tableNames.add("xxx.*");

        ArrayList<String> allTableNames = new ArrayList<>();
        allTableNames.add("chongwang");
        allTableNames.add("chongwang11");
        allTableNames.add("chongwang22");
        allTableNames.add("xxx");
        allTableNames.add("xxx11");
        allTableNames.add("xxx22");
        allTableNames.add("xxx");
        ArrayList<String> all = new ArrayList<>(allTableNames);

        allTableNames.retainAll(tableNames);
        System.out.println(allTableNames);
        String ipaddressPattern = "chongwang.*";
        System.out.println(allTableNames.size());
        tableNames.removeAll(allTableNames);
        System.out.println(tableNames);
        System.out.println(all);

        Pattern pattern = Pattern.compile(ipaddressPattern);
        boolean contains = tableNames.contains(ipaddressPattern);
        System.out.println(contains);
        tableNames.forEach(t -> {
            Matcher matcher = pattern.matcher(t);
            while (matcher.find()) {
                System.out.println(matcher.group());
            }
        });


    }
}
