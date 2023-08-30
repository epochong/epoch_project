package util;

/**
 * @author chongwang11
 * @date 2023-08-17 15:04
 * @description
 */
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

public class ExcelUtils {

    public static void readExcel(String filePath) {
        try (FileInputStream fileInputStream = new FileInputStream(new File(filePath));
             Workbook workbook = WorkbookFactory.create(fileInputStream)) {

            Sheet sheet = workbook.getSheetAt(0); // 获取第一个工作表

            for (Row row : sheet) {
                for (Cell cell : row) {
                    switch (cell.getCellType()) {
                        case STRING:
                            System.out.print(cell.getStringCellValue() + "\t");
                            break;
                        case NUMERIC:
                            System.out.print(cell.getNumericCellValue() + "\t");
                            break;
                        case BOOLEAN:
                            System.out.print(cell.getBooleanCellValue() + "\t");
                            break;
                        default:
                            System.out.print("\t");
                    }
                }
                System.out.println(); // 换行
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createSheetWithContent(String filePath) {
        try (Workbook workbook = new XSSFWorkbook()) { // 使用XSSFWorkbook创建一个新的Excel工作簿
            Sheet sheet = workbook.createSheet("NewSheet"); // 创建一个名为"NewSheet"的工作表

            // 创建标题行
            Row headerRow = sheet.createRow(0);
            Cell headerCell = headerRow.createCell(0);
            headerCell.setCellValue("Header 1");

            // 创建数据行
            Row dataRow = sheet.createRow(1);
            Cell dataCell = dataRow.createCell(0);
            dataCell.setCellValue("Data 1");

            // 将工作簿保存到文件
            try (FileOutputStream fileOut = new FileOutputStream(filePath)) {
                workbook.write(fileOut);
            }

            System.out.println("Excel sheet created and saved successfully.");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeListDataToSheet(String filePath, List<List<Object>> data) {
        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("DataSheet");

            for (int rowIdx = 0; rowIdx < data.size(); rowIdx++) {
                Row row = sheet.createRow(rowIdx);
                List<Object> rowData = data.get(rowIdx);
                for (int colIdx = 0; colIdx < rowData.size(); colIdx++) {
                    Cell cell = row.createCell(colIdx);
                    Object cellValue = rowData.get(colIdx);
                    setCellValue(cell, cellValue);
                }
            }

            try (FileOutputStream fileOut = new FileOutputStream(filePath)) {
                workbook.write(fileOut);
            }

            System.out.println("Data written to Excel sheet successfully.");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void setCellValue(Cell cell, Object value) {
        if (value instanceof String) {
            cell.setCellValue((String) value);
        } else if (value instanceof Double) {
            cell.setCellValue((Double) value);
        } else if (value instanceof Integer) {
            cell.setCellValue((Integer) value);
        } else if (value instanceof Boolean) {
            cell.setCellValue((Boolean) value);
        } else {
            cell.setCellValue(value.toString());
        }
    }

    public static void writeDataToSheet(String filePath) {
        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("NewSheet");

            String[] headers = {"Header 1", "Header 2", "Header 3"};
            Row headerRow = sheet.createRow(0);

            for (int i = 0; i < headers.length; i++) {
                Cell headerCell = headerRow.createCell(i);
                headerCell.setCellValue(headers[i]);
            }

            String[][] data = {
                    {"Data 1", "Data 2", "Data 3"},
                    {"Data A", "Data B", "Data C"},
                    {"Value X", "Value Y", "Value Z"}
            };

            for (int rowIdx = 0; rowIdx < data.length; rowIdx++) {
                Row dataRow = sheet.createRow(rowIdx + 1); // Start from row 1
                for (int colIdx = 0; colIdx < data[rowIdx].length; colIdx++) {
                    Cell dataCell = dataRow.createCell(colIdx);
                    dataCell.setCellValue(data[rowIdx][colIdx]);
                }
            }

            try (FileOutputStream fileOut = new FileOutputStream(filePath)) {
                workbook.write(fileOut);
            }

            System.out.println("Data written to Excel sheet successfully.");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String filePath = "path/to/your/excel/file.xlsx";
        readExcel(filePath);
    }
}
