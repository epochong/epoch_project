package util;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.net.URLDecoder;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author chongwang11
 * @date 2023-03-07 15:40
 * @description
 */
public class ExcelReader {

    private static final String XLS = "xls";
    private static final String XLSX = "xlsx";
    private static final String ENCODING_TYPE_UTF = "UTF-8";
    private static final int ROW_TYPE_FLAG = 0;
    private static final int LIST_TYPE_FLAG = 1;

    public static void main(String[] args) {
        List<List<Object>> kettle = readExcel("/Users/epochong/Downloads/iflytek/Kettle迁移输入信息.xlsx", "kettle迁移");
    }

    /**
     * 合并单元格处理,获取合并行
     *
     * @param sheet
     * @return List<CellRangeAddress>
     */
    public static List<CellRangeAddress> getCombineCell(Sheet sheet) {
        List<CellRangeAddress> list = new ArrayList<>();
        //获得一个 sheet 中合并单元格的数量
        int sheetMergerCount = sheet.getNumMergedRegions();
        //遍历所有的合并单元格
        for (int i = 0; i < sheetMergerCount; i++) {
            //获得合并单元格保存进list中
            CellRangeAddress ca = sheet.getMergedRegion(i);
            list.add(ca);
        }
        return list;
    }

    /**
     * 读取Excel文件
     *
     * @param fileName  读取Excel文件的名称
     * @param sheetName 读取Excel文件的SheetName
     * @return
     */
    public static  List<List<Object>> readExcel(String fileName, String sheetName) {
        FileInputStream inputStream = null;
        Workbook workbook = null;
        try {
            String fileType = fileName.substring(fileName.lastIndexOf(".") + 1);
            File excelFile = new File(fileName);
            if (!excelFile.exists()) {
                System.out.println("the excel file does not exist!");
                return null;
            }
            inputStream = new FileInputStream(excelFile);
            workbook = getWorkbook(inputStream, fileType);
            return parseExcel(workbook, sheetName).get(sheetName);
        } catch (Exception e) {
            System.out.println(String.format("read excel file throws error :{}", e.getMessage()));
            e.printStackTrace();
        } finally {
            try {
                if (workbook != null) workbook.close();
                if (inputStream != null) inputStream.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * 获取一个Excel文件对象
     *
     * @param inputStream
     * @param fileType
     * @return
     * @throws Exception
     */
    private static Workbook getWorkbook(InputStream inputStream, String fileType) throws Exception {
        Workbook workbook = null;
        if (fileType.equalsIgnoreCase(XLS)) {
            workbook = new HSSFWorkbook(inputStream);
        } else if (fileType.equalsIgnoreCase(XLSX)) {
            workbook = new XSSFWorkbook(inputStream);
        }
        return workbook;
    }

    /**
     * 解析Excel文件对象
     *
     * @param workbook
     * @param sheetName
     * @return
     */
    private static Map<String, List<List<Object>>> parseExcel(Workbook workbook, String sheetName) {
        Map<String, List<List<Object>>> result = new HashMap<>();
        for (int sheetNum = 0; sheetNum < workbook.getNumberOfSheets(); sheetNum++) {
            String name = workbook.getSheetName(sheetNum);
            if (sheetName == null || sheetName.equals(name)) {
                Sheet sheet = workbook.getSheet(name);
                List<CellRangeAddress> combineCell = getCombineCell(sheet);
                combineCell = combineCell.stream().filter(cellAddresses -> cellAddresses.formatAsString().startsWith("A")).collect(Collectors.toList());
                List<List<Object>> rows = convertSheet(sheet);
                result.put(name,  getMergedRows(combineCell, rows));
            }
        }
        return result;
    }

    public static List<List<Object>> getMergedRows(List<CellRangeAddress> cellRangeAddresses, List<List<Object>> rows) {
        for (CellRangeAddress cellRangeAddress : cellRangeAddresses) {
            List<String> columns = new ArrayList<>();
            List<String> types = new ArrayList<>();
            for (int i = cellRangeAddress.getFirstRow(); i <= cellRangeAddress.getLastRow(); i++) {
                columns.add(rows.get(i).get(8).toString());
                types.add(rows.get(i).get(9).toString());
            }
            rows.get(cellRangeAddress.getFirstRow()).set(8, columns);
            rows.get(cellRangeAddress.getFirstRow()).set(9, types);
        }
        return rows.stream().filter(row -> Objects.nonNull(row.get(0)) && StringUtils.isNotEmpty(row.get(0).toString())).collect(Collectors.toList());
    }
    /**
     * 转换Sheet表为List<Object>集合
     *
     * @param sheet
     * @return
     */
    private static List<List<Object>> convertSheet(Sheet sheet) {
        List<List<Object>> sheetDataList = new ArrayList<>();
        for (int rowNum = 0; rowNum < sheet.getPhysicalNumberOfRows(); rowNum++) {
            Row row = sheet.getRow(rowNum);
            List<Object> rowDataList = convertRow(row);
            sheetDataList.add(rowDataList);
        }
        return sheetDataList;
    }

    /**
     * 转换行为List<Object>集合
     *
     * @param row
     * @return
     */
    private static List<Object> convertRow(Row row) {
        List<Object> rowDataList = new ArrayList<>();
        // kettle迁移定制化表格,只有14列
        for (int cellNum = 0; cellNum < 14; cellNum++) {
            Cell cell = row.getCell(cellNum);
            Object value = convertCell(cell);
            rowDataList.add(value);
        }
        return rowDataList;
    }

    /**
     * 转换每个cell单元格为Object对象
     *
     * @param cell
     * @return
     */
    private static Object convertCell(Cell cell) {
        if (cell == null) {
            return "";
        }
        CellType cellType = cell.getCellType();
        Object value = null;
        switch (cellType) {
            case _NONE:
            case BLANK:
            case ERROR:
                break;
            case STRING:
                value = cell.getStringCellValue().trim();
                break;
            case BOOLEAN:
                value = cell.getBooleanCellValue();
                break;
            case NUMERIC:
                value = cell.getDateCellValue();
                break;
            case FORMULA:
                value = cell.getCellFormula();
                break;
        }
        return value;
    }

    /**
     * 读取项目资源目录下 Excel文件
     *
     * @param fileName  文件名称
     * @param sheetName Excel中的sheet名称
     * @return 读取的Map集合
     */
    public static List<List<Object>> readExcelFile(String fileName, String sheetName) {
        URL resource = ExcelReader.class.getClassLoader().getResource(fileName);
        List<List<Object>> result = null;
        try {
            result = readExcel(URLDecoder.decode(resource.getPath(), ENCODING_TYPE_UTF), sheetName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 获取Excel文件中的具体值
     *
     * @param excelMap Excel读取出来的Map结果集
     * @param row      需要获取值得行数
     * @param list     需要获取值得列数
     * @return
     */
    public static String getExcelValue(Map<String, Object> excelMap, int row, int list) {
        // 行数组集合
        ArrayList<Object> rowValues = (ArrayList<Object>) excelMap.values();
        // 列数组集合
        ArrayList<Object> listValues = (ArrayList<Object>) rowValues.get(row);
        return String.valueOf(listValues.get(list));
    }

    /**
     * 获取指定列的List集合
     *
     * @param rowList 行的集合
     * @param index   列的索引
     * @return
     */
    private static List<Object> getLineList(List<Object> rowList, int index) {
        List<Object> resultList = new ArrayList<>();
        Object cell;
        for (Object obj : rowList) {
            if (obj instanceof ArrayList) {
                cell = ((ArrayList) obj).get(index);
                resultList.add(cell);
            }
        }
        return resultList;
    }

    /**
     * 获取行的集合
     *
     * @param excelMap
     * @return
     */
    private static List<Object> getRowList(Map<String, Object> excelMap, int index) {
        // 行数组集合
        List<Object> resultList = new ArrayList<>();
        Iterator<Object> iterator = excelMap.values().iterator();
        while (iterator.hasNext()) {
            Object next = iterator.next();
            if (next instanceof ArrayList) {
                for (Object value : (List<?>) next) {
                    resultList.add(value);
                }
            }
        }
        if (index < 0) {
            return resultList;
        }
        Object result = resultList.get(index);
        return result instanceof ArrayList ? (List<Object>) result : new ArrayList<>();
    }

    /**
     * 获取Excel文件中指定的一行数据或者是一列数据
     *
     * @param excelMap 需要进行获取的Excel文件
     * @param index    需要获取的一行或者是一列
     * @param type     行或者列的类型 0 表示获取行 1 表示获取列
     * @return 获取的结果集
     */
    public static List<String> getList(Map<String, Object> excelMap, int index, int type) {
        switch (type) {
            case ROW_TYPE_FLAG:
                return handleRow(excelMap, index);
            case LIST_TYPE_FLAG:
                return handleList(excelMap, index);
            default:
                return Collections.EMPTY_LIST;
        }
    }

    /**
     * 获取Excel中指定列的信息
     *
     * @param excelMap
     * @param index
     * @return
     */
    private static List<String> handleList(Map<String, Object> excelMap, int index) {
        List<Object> rowList = getRowList(excelMap, -1);
        List<Object> lineList = getLineList(rowList, index);
        List<String> resultList = new ArrayList<>();
        for (Object obj : lineList) {
            resultList.add(obj.toString());
        }
        return resultList;
    }

    /**
     * 获取Excel中指定行的信息
     *
     * @param excelMap
     * @param index
     * @return
     */
    private static List<String> handleRow(Map<String, Object> excelMap, int index) {
        List<Object> rowList = getRowList(excelMap, index);
        List<String> resultList = new ArrayList<>();
        for (Object obj : rowList) {
            resultList.add(obj.toString());
        }
        return resultList;
    }
}