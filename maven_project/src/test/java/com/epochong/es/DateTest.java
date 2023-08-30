package com.epochong.es;

import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author chongwang11
 * @date 2023-02-17 18:12
 * @description
 */
public class DateTest {

    public int seven = 7 * 24 * 60 * 60 * 1000;
    @Test
    public void testDate() {
        System.out.println(new Timestamp(1676628874000l).toString());
    }

    @Test
    public void testDateTs() {
        System.out.println(new Date().getTime() - seven);
    }

    @Test
    public void string() {
        ArrayList<Integer> arrayList = new ArrayList<>();
        arrayList.add(1);
        arrayList.add(2);
        arrayList.add(3);
        arrayList.add(4);
        arrayList.add(5);
        arrayList.add(6);
        arrayList.add(7);

        List<Integer> list = arrayList.subList(0, 7);

        for (Integer integer : list) {
            System.out.println(" "+integer);
        }


    }
}
