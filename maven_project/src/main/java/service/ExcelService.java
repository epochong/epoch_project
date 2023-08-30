package service;

import util.ExcelReader;
import util.ExcelUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author chongwang11
 * @date 2023-08-17 15:11
 * @description
 */
public class ExcelService {
    public static void main(String[] args) {
        List<List<Object>> data = ExcelReader.readExcel("/Users/epochong/Downloads/iflytek/数据/作业信息.xlsx", "data");
        List<List<Object>> res = new ArrayList<>();
        for (List<Object> datum : data) {
            String[] split = datum.get(3).toString().split(",");
            String[] target = datum.get(5).toString().split(",");
            for (int i = 0; i < split.length; i++) {
                List<Object> newRow = new ArrayList<>(datum);
                newRow.set(3, split[i]);
                newRow.set(5, target[i]);
                res.add(newRow);
            }
        }
        ExcelUtils.writeListDataToSheet("/Users/epochong/Downloads/iflytek/数据/作业信息_after.xlsx", res);
    }
}
