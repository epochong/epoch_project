package db.ftp;


import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chongwang11
 * @date 2023-05-10 09:39
 * @description
 */
public class FileReader {
    static FtpWriter ftpWriter = new FtpWriter();

    static String mediaPath = "/Users/epochong/Downloads/iflytek/音频/media/";

    public static void main(String[] args) throws Exception {
         readFileWrite2Ftp();

        //InputStream ftpStream = ftpWriter.getInputStream();

        //System.out.println(equals(getLocalInputStream(), ftpStream));
//        processFtpHandledMediaFile("/Users/epochong/data/iflytek/output_0");
//        fileEquals("/Users/epochong/Downloads/iflytek/音频/media/iat000d6cda@hu1869b4528ef035f822.media",
//                "/Users/epochong/IdeaProjects/maven_project/output/iat000d6cda@hu1869b4528ef035f822.media");
    }


    /**
     * @param path 本地处理过后的写入到ftp的文件,绝对路径
     * @return 写入成功的文件列表路径
     * @throws IOException
     */
    public static List<String> processFtpHandledMediaFile(String path) throws IOException {
        InputStream inputStream = Files.newInputStream(Paths.get(path));
        System.out.println(inputStream.available());
        byte[] b = new byte[32];
        inputStream.read(b);
        String sid = new String(b);
        File file = new File("." + File.separator + "output" + File.separator + sid + ".media"); //指定文件路径
        OutputStream output = new FileOutputStream(file, false);
        if (!file.getParentFile().exists()) { //如果文件不存在
            file.getParentFile().mkdirs(); //创建父目录
        }
        System.out.println(sid);
        char temp;
        StringBuilder format = new StringBuilder();
        // 先read一个,这个是逗号
        inputStream.read();
        while (true) {
            temp = (char) inputStream.read();
            if (temp == 10)
                break;
            format.append(temp);
        }
        output.write(format.toString().getBytes());
        output.write("\n".getBytes());
        System.out.println(format);
        b = new byte[3];

        while ((inputStream.read(b)) != -1) {
            if (new String(b).equals("\n\n\n")) {
                break;
            }

            try {
                output.write(b);   //将字符串变为字节数组
                output.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        output.close();
        System.out.println();
        return new ArrayList<>();
    }

    public static List<InputStream> getLocalInputStream() throws IOException {
        List<InputStream> inputStreamList = new ArrayList<>();
        List<String> fileNames = getFileNames(mediaPath);
        assert fileNames != null;
        for (String fileName : fileNames) {
            InputStream inputStream = Files.newInputStream(Paths.get(mediaPath + fileName));
            inputStreamList.add(inputStream);
        }
        return inputStreamList;
    }

//    public static List<InputStream> getFtpInputStream() throws IOException {
//        List<InputStream> inputStreamList = new ArrayList<>();
//        InputStream ftpStream = ftpWriter.getInputStream();
//        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(ftpStream));
//        String str = null;
//        while ((str = bufferedReader.readLine()) != null) {
//            String[] split = str.split(",");
//            System.out.println(str);
//        }
//        List<String> fileNames = getFileNames(mediaPath);
//        assert fileNames != null;
//        for (String fileName : fileNames) {
//            InputStream inputStream = Files.newInputStream(Paths.get(mediaPath + fileName));
//            inputStreamList.add(inputStream);
//        }
//        return inputStreamList;
//    }


    public static void readFileWrite2Ftp() throws Exception {
        List<String> fileNames = getFileNames(mediaPath);
        List<InputStream> localInputStream = getLocalInputStream();
//        SequenceInputStream sis = new SequenceInputStream(localInputStream.get(0), localInputStream.get(1));
//        for (int i = 2; i < localInputStream.size(); i++) {
//            sis = new SequenceInputStream(sis, localInputStream.get(i));
//        }
        List<String> sidList = new ArrayList<>();
        assert fileNames != null;
        for (String fileName : fileNames) {
            sidList.add(fileName.substring(0, fileName.lastIndexOf(".")));
        }
        List<String> writerFileNameList = new ArrayList<>();
        for (int i = 1; i <= 99; i++) {
            writerFileNameList.add("output_" + (int) (i * 1.0 / 20) + ".zip");
        }
        //对于二进制文件的读取，没有太大的意义，因为显示的二进制数字的十进制表示，所以一般二进制流用于文件的复制
        ftpWriter.loginFtpServer("172.30.41.123", "ftpuser", "GvkGmejnD4", 10021, 60000);
        ftpWriter.writeByZipToFtp(sidList, writerFileNameList, localInputStream);
        System.out.println("写入完成");
    }

    public static List<String> getFileNames(String path) {
        File file = new File(path);
        if (!file.exists()) {
            return null;
        }
        List<String> fileNames = new ArrayList<>();
        return getFileNames(file, fileNames);
    }

    private static List<String> getFileNames(File file, List<String> fileNames) {
        File[] files = file.listFiles();
        for (File f : files) {
            if (f.isDirectory()) {
                getFileNames(f, fileNames);
            } else {
                fileNames.add(f.getName());
            }
        }
        return fileNames;
    }

    public static boolean fileEquals(List<InputStream> source, List<InputStream> target) throws Exception {
        for (int i = 0; i < source.size(); i++) {
            //返回中的字节数
            //用file.length（）方法也可以
            int len1 = source.get(i).available();
            int len2 = target.get(i).available();
            System.out.println(len1);
            System.out.println(len2);
            if (len1 == len2) {
                //长度比较，长度相同就比较具体内容
                //建立两个字节缓冲区
                //字节数组是为了存入数据
                byte[] b1 = new byte[len1];
                byte[] b2 = new byte[len2];
                //将两个文件读入缓冲区
                source.get(i).read(b1);
                target.get(i).read(b2);
                //依次比较文件中的每个字节
                for (int j = 0; j < len1; j++) {
                    if (b1[j] != b2[j]) {
                        System.out.println("文件内容不一样");
                        return false;
                    }
                }
                System.out.println("文件一样");
            } else {
                System.out.println("文件长度不一样，文件不一样");
                return false;
            }
            source.get(i).close();
            target.get(i).close();
        }
        return true;
    }

    public static boolean fileEquals(String sourcePath, String targetPath) throws Exception {
        InputStream source = Files.newInputStream(Paths.get(sourcePath));
        InputStream target = Files.newInputStream(Paths.get(targetPath));
        //返回中的字节数
        //用file.length（）方法也可以
        int len1 = source.available();
        int len2 = target.available();
        System.out.println(len1);
        System.out.println(len2);
        if (len1 == len2) {
            //长度比较，长度相同就比较具体内容
            //建立两个字节缓冲区
            //字节数组是为了存入数据
            byte[] b1 = new byte[len1];
            byte[] b2 = new byte[len2];
            //将两个文件读入缓冲区
            source.read(b1);
            target.read(b2);
            //依次比较文件中的每个字节
            for (int j = 0; j < len1; j++) {
                if (b1[j] != b2[j]) {
                    System.out.println("文件内容不一样");
                    return false;
                }
            }
            System.out.println("文件一样");
        } else {
            System.out.println("文件长度不一样，文件不一样");
            return false;
        }
        source.close();
        target.close();
        return true;
    }
}
