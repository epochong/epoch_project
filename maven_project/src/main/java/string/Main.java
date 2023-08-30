package string;

import lombok.extern.slf4j.Slf4j;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author chongwang11
 * @date 2023-07-17 17:37
 * @description
 */
@Slf4j
public class Main {

    public static void main(String[] args) {
        System.out.println(parseExpression("chongwang_${yyyy-MM-dd,-1,day}"));
    }
    /**
     * 占位符表达式替换
     *
     * @param text
     * @return java.lang.String
     * @author zywang13 2023/7/3
     */
    public static String parseExpression(String text) {

        // 使用正则表达式匹配占位符
        Pattern pattern = Pattern.compile("\\$\\{(.+?)\\}");
        Matcher matcher = pattern.matcher(text);

        // 使用数据源替换占位符
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            String placeholder = matcher.group(1);
            String replacement = resolvePlaceholder(placeholder);
            matcher.appendReplacement(buffer, replacement);
        }
        matcher.appendTail(buffer);
        // 打印替换后的字符串
        return buffer.toString();
    }

    /**
     * 按照逗号分隔，含义依次为 时间格式，时间间隔，时间单位，时间类型
     *
     * @param placeholder
     * @return java.lang.String
     * @author zywang13 2023/7/14
     */
    private static String resolvePlaceholder(String placeholder) {
        String[] options = placeholder.split(",");

        //时间间隔，默认0
        int interval = 0;
        if (options.length >= 2) {
            interval = Integer.parseInt(options[1]);
        }
        //时间单位
        int unit = Calendar.DATE;
        if (options.length >= 3) {
            switch (options[2]) {
                case "month":
                    unit = Calendar.MONTH;
                    break;
                case "year":
                    unit = Calendar.YEAR;
                    break;
                default:
                    break;
            }
        }

        try {
            SimpleDateFormat sdf = new SimpleDateFormat(options[0]);
            // 根据选项生成相应的值
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(sdf.parse(sdf.format(new Date())));
            calendar.add(unit, interval);
            return sdf.format(calendar.getTime());
        } catch (ParseException e) {
            log.warn("时间格式转化异常");
        }
        return "";
    }
}
