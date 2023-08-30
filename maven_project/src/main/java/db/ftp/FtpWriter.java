package db.ftp;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import java.io.*;
import java.net.UnknownHostException;
import java.util.*;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * @author chongwang11
 * @date 2023-05-09 16:40
 * @description
 */
@Slf4j
public class FtpWriter {

    static String separator = ",";
    static String separatorMedia = "\ndataex_separator\n";
    FTPClient ftpClient = null;

    private String host;
    private String username;
    private String password;
    private int port;
    private int timeout;

    //String fileFinalName = "media_output_5n";

    //String absolutePathFileName = "/data1/home/ftpuser/chongwang/binary/test/" + fileFinalName;
    String parentPath = "/data1/home/ftpuser/chongwang/binary/test";


    public void loginFtpServer(String host, String username, String password, int port, int timeout) {
        this.host = host;
        this.username = username;
        this.password = password;
        this.port = port;
        this.timeout = timeout;
        this.connect();
    }

    private void connect() {
        this.ftpClient = new FTPClient();
        try {
            // 不需要写死ftp server的OS TYPE,FTPClient getSystemType()方法会自动识别
            // this.ftpClient.configure(new FTPClientConfig(FTPClientConfig.SYST_UNIX));
//            this.ftpClient.setDefaultTimeout(timeout);
            this.ftpClient.setConnectTimeout(timeout);
//            this.ftpClient.setDataTimeout(timeout);

            // 连接登录
            this.ftpClient.connect(host, port);

            ftpClient.setControlEncoding("GBK");
            ftpClient.setAutodetectUTF8(true);

            this.ftpClient.login(username, password);

            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
            this.ftpClient.enterRemotePassiveMode();
            this.ftpClient.enterLocalPassiveMode();
            this.ftpClient.setControlKeepAliveTimeout(20);
            int reply = this.ftpClient.getReplyCode();
            String replyString = this.ftpClient.getReplyString();
            if (!FTPReply.isPositiveCompletion(reply)) {
                this.ftpClient.disconnect();
                String message = String
                        .format("与ftp服务器建立连接失败,host:%s, port:%s, username:%s, replyCode:%s,replyString:%s",
                                host, port, username, reply, replyString);
                System.out.println("FtpWriter.connect" + message);
            }
            System.out.println("FtpWriter.connect success");
        } catch (UnknownHostException e) {
            String message = String.format(
                    "请确认ftp服务器地址是否正确，无法连接到地址为: [%s] 的ftp服务器, errorMessage:%s",
                    host, e.getMessage());
            System.out.println("FtpWriter.connect + message" + message);
        } catch (IllegalArgumentException e) {
            String message = String.format(
                    "请确认连接ftp服务器端口是否正确，错误的端口: [%s], errorMessage:%s", port,
                    e.getMessage());
            System.out.println(message);
        } catch (Exception e) {
            String message = String
                    .format("与ftp服务器建立连接失败,host:%s, port:%s, username:%s, errorMessage:%s",
                            host, port, username, e.getMessage());
            System.out.println(message);
        }

    }

    public boolean isFileExist(String pathName) {
        try {
            String parentDir = pathName.substring(0,
                    StringUtils.lastIndexOf(pathName, IOUtils.DIR_SEPARATOR));
            this.ftpClient.changeWorkingDirectory(parentDir);
            FTPFile[] files = this.ftpClient.listFiles(pathName);
            if (files.length == 0) {
                return false;
            } else {
                return true;
            }
        } catch (IOException e) {
            String message = String
                    .format("检查文件%s是否存在时发生I/O异常,请确认与ftp服务器的连接正常,拥有目录ls权限, errorMessage:%s",
                            pathName, e.getMessage());
            log.error(message);
        }
        return false;
    }

    public void deleteFiles(Set<String> filesToDelete) {
        String eachFile = null;
        boolean deleteOk = false;
        try {
            this.printWorkingDirectory();
            for (String each : filesToDelete) {
                log.info(String.format("delete file [%s].", each));
                eachFile = each;
                deleteOk = this.ftpClient.deleteFile(new String(each.getBytes("GBK"), FTP.DEFAULT_CONTROL_ENCODING));
                if (!deleteOk) {
                    String message = String.format(
                            "删除文件:[%s] 时失败,请确认指定文件有删除权限", eachFile);
                }
            }
        } catch (IOException e) {
            String message = String.format(
                    "删除文件:[%s] 时发生异常,请确认指定文件有删除权限,以及网络交互正常, errorMessage:%s",
                    eachFile, e.getMessage());
            log.error(message);
        }
    }

    public void writeBinaryToStream(List<String> sidList, List<String> fileNameList, List<InputStream> inputStream) {
        OutputStream fo = null;
        String finalName = null;
        try {
            Long totalBytes = 0L;
            for (int i = 0; i < sidList.size(); i++) {
                String fileName = fileNameList.get(i);
                if (finalName == null || !finalName.equals(fileName)) {
                    if (fo != null) {
                        IOUtils.closeQuietly(fo);
                        this.completePendingCommand();
                    }
                    finalName = fileName;
                    String absolutePathFileName = parentPath + File.separator + finalName;
                    System.out.println(absolutePathFileName);
                    if (isFileExist(absolutePathFileName)) {
                        // 覆盖模式
                        HashSet<String> deletePath = new HashSet<>();
                        deletePath.add(absolutePathFileName);
                        deleteFiles(deletePath);
                    }
                    mkDirRecursive(parentPath);
                    fo = getOutputStream(absolutePathFileName);

                }
                // 先写sid
                fo.write(sidList.get(i).getBytes());
                fo.write(separator.getBytes());
                fo.flush();
                int num;
                while ((num = inputStream.get(i).read()) != -1) {
                    fo.write(num);
                    fo.flush();
                }
                fo.write(separatorMedia.getBytes());
                fo.flush();
            }

            log.info(String.format("file %s 's total size:%s", finalName, totalBytes));
        } catch (Exception e) {
            System.err.println(ExceptionUtils.getMessage(e));
        } finally {
            IOUtils.closeQuietly(fo);
            this.completePendingCommand();
        }
    }

    @Data
    static class Record {
        String fileName;
        byte[] data;

        public Record(String fileName, byte[] data) {
            this.fileName = fileName;
            this.data = data;
        }
    }

    /**
     * 同一个线程使用 会在awaitSpace 死锁
     *
     * @param sidList
     * @param fileNameList
     * @param inputStreams
     * @throws IOException
     */

    public void writeZipToFtp(List<String> sidList, List<String> fileNameList, List<InputStream> inputStreams) throws IOException {
        /* 创建 outputstream 和 inputstream 之间的转换*/
        PipedOutputStream pos = new PipedOutputStream();
        PipedInputStream pis = new PipedInputStream();
        pos.connect(pis);
        /* 使用pos初始化zos，相当于zos和pis连接起来 */
        ZipOutputStream zos = new ZipOutputStream(pos);

        List<Record> recordList = new ArrayList<>();
        OutputStream fo = null;
        String finalName = null;
        try {
            for (int i = 0; i < sidList.size(); i++) {
                // 先写sid
                zos.putNextEntry(new ZipEntry(sidList.get(i) + ".media"));
                System.out.println(inputStreams.get(i).available());
                int num;
                while ((num = inputStreams.get(i).read()) != -1) {
                    zos.write(num);
                }
                zos.closeEntry();
                zos.flush();

                //inputStreams.get(i).close();
                if (i == 3) {
                    zos.close();
                    byte[] bytes = new byte[pis.available()];
                    pis.read(bytes);
                    recordList.add(new Record("x", bytes));
                    break;
                }
                byte[] bytes = new byte[pis.available()];
                pis.read(bytes);
                recordList.add(new Record("x", bytes));
            }
            String fileName = fileNameList.get(0);
            if (finalName == null || !finalName.equals(fileName)) {
                finalName = fileName;
                String absolutePathFileName = parentPath + File.separator + finalName;
                System.out.println(absolutePathFileName);
                if (isFileExist(absolutePathFileName)) {
                    // 覆盖模式
                    HashSet<String> deletePath = new HashSet<>();
                    deletePath.add(absolutePathFileName);
                    deleteFiles(deletePath);
                }
                mkDirRecursive(parentPath);
                fo = getOutputStream(absolutePathFileName);

            }
            for (Record record : recordList) {
                try {
                    fo.write(record.data);
                    fo.flush();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (Exception e) {
            System.err.println(ExceptionUtils.getMessage(e));
        } finally {
            IOUtils.closeQuietly(fo);
            this.completePendingCommand();
        }
    }


    public void writeByZipToFtp(List<String> sidList, List<String> fileNameList, List<InputStream> inputStreams) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ZipOutputStream zos = new ZipOutputStream(byteArrayOutputStream);
        zos.setLevel(Deflater.NO_COMPRESSION);
        List<Record> recordList = new ArrayList<>();
        String finalName = null;
        String absoluteFinalName = null;
        try {
            for (int i = 0; i < sidList.size(); i++) {
                String fileName = fileNameList.get(i);
                if (finalName == null || !finalName.equals(fileName)) {
                    if (finalName != null) {
                        zos.close();
                        recordList.add(new Record(absoluteFinalName, byteArrayOutputStream.toByteArray()));
                        byteArrayOutputStream.reset();
                        zos = new ZipOutputStream(byteArrayOutputStream);
                    }
                    finalName = fileName;
                    absoluteFinalName = parentPath + File.separator + finalName;
                }
                // 压缩包里面的文件, 以sid命名
                zos.putNextEntry(new ZipEntry(sidList.get(i) + ".media"));
                int num;
                while ((num = inputStreams.get(i).read()) != -1) {
                    zos.write(num);
                }
                zos.closeEntry();
                zos.flush();
                recordList.add(new Record(absoluteFinalName, byteArrayOutputStream.toByteArray()));
                byteArrayOutputStream.reset();
            }
            zos.close();
            recordList.add(new Record(absoluteFinalName, byteArrayOutputStream.toByteArray()));
            byteArrayOutputStream.reset();
            String curPathName = null;
            OutputStream fo = null;
            for (Record record : recordList) {
                if (curPathName == null || !curPathName.equals(record.getFileName())) {
                    curPathName = record.getFileName();
                    log.info("write current ftp file" + curPathName);
                    if (fo != null) {
                        IOUtils.closeQuietly(fo);
                        this.completePendingCommand();
                    }
                    log.info(record.getFileName());
                    if (isFileExist(record.getFileName())) {
                        // 覆盖模式
                        HashSet<String> deletePath = new HashSet<>();
                        deletePath.add(record.getFileName());
                        deleteFiles(deletePath);
                    }
                    mkDirRecursive(parentPath);
                    fo = getOutputStream(record.getFileName());
                }
                try {
                    fo.write(record.getData());
                    fo.flush();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            IOUtils.closeQuietly(fo);
            this.completePendingCommand();
        } catch (Exception e) {
            log.error(ExceptionUtils.getMessage(e));
        }
    }


//    public InputStream getInputStream() {
//        try {
//            String gbkPath = new String(absolutePathFileName.getBytes("GBK"), FTP.DEFAULT_CONTROL_ENCODING);
//            return ftpClient.retrieveFileStream(gbkPath);
//        } catch (IOException e) {
//            String message = String.format("读取文件 : [%s] 时出错,请确认文件：[%s]存在且配置的用户有权限读取", parentPath, parentPath);
//            LOG.error(message);
//        }
//        return null;
//    }

    public void completePendingCommand() {
        try {
            boolean isOk = this.ftpClient.completePendingCommand();
            if (!isOk) {
            }
        } catch (IOException e) {
            String message = String.format(
                    "完成ftp completePendingCommand操作发生异常, errorMessage:%s",
                    e.getMessage());
            log.error(message);
        }
    }


    private void printWorkingDirectory() {
        try {
            log.info(String.format("current working directory:%s", this.ftpClient.printWorkingDirectory()));
        } catch (Exception e) {
            log.warn(String.format("printWorkingDirectory error:%s",
                    e.getMessage()));
        }
    }

    public OutputStream getOutputStream(String filePath) throws Exception {
        try {
            this.printWorkingDirectory();
            String parentDir = filePath.substring(0, StringUtils.lastIndexOf(filePath, IOUtils.DIR_SEPARATOR));
            this.ftpClient.changeWorkingDirectory(new String(parentDir.getBytes("GBK"), FTP.DEFAULT_CONTROL_ENCODING));
            this.printWorkingDirectory();
            OutputStream writeOutputStream = this.ftpClient.storeFileStream(new String(filePath.getBytes("GBK"), FTP.DEFAULT_CONTROL_ENCODING));
            String message = String.format("打开FTP文件[%s]获取写出流时出错,请确认文件%s有权限创建，有权限写出等", filePath, filePath);
            if (null == writeOutputStream) {
                log.error("log" + message);
            }

            return writeOutputStream;
        } catch (IOException e) {
            String message = String.format(
                    "写出文件 : [%s] 时出错,请确认文件:[%s]存在且配置的用户有权限写, errorMessage:%s",
                    filePath, filePath, e.getMessage());
            System.out.println(message);
            throw new Exception(message);
        }
    }


    public void mkDirRecursive(String directoryPath) {
        StringBuilder dirPath = new StringBuilder();
        dirPath.append(IOUtils.DIR_SEPARATOR_UNIX);
        String[] dirSplit = StringUtils.split(directoryPath, IOUtils.DIR_SEPARATOR_UNIX);
        String message = String.format("创建目录:%s时发生异常,请确认与ftp服务器的连接正常,拥有目录创建权限", directoryPath);
        try {
            // ftp server不支持递归创建目录,只能一级一级创建
            for (String dirName : dirSplit) {
                dirPath.append(dirName);
                boolean mkdirSuccess = mkDirSingleHierarchy(dirPath.toString());
                dirPath.append(IOUtils.DIR_SEPARATOR_UNIX);
                if (!mkdirSuccess) {
                    System.out.println(mkdirSuccess);
                }
            }
        } catch (IOException e) {
            message = String.format("%s, errorMessage:%s", message, e.getMessage());
            System.err.println(message);
        }
    }

    public boolean mkDirSingleHierarchy(String directoryPath) throws IOException {
        return mkDirSingleHierarchyStatic(this.ftpClient, directoryPath);
    }

    private synchronized static boolean mkDirSingleHierarchyStatic(FTPClient fc, String directoryPath) throws IOException {
        boolean isDirExist = fc.changeWorkingDirectory(directoryPath);
        // 如果directoryPath目录不存在,则创建
        if (!isDirExist) {
            log.info("dir not exists,start to create:{}", directoryPath);
            int replayCode = fc.mkd(directoryPath);
            log.info("create dir {} replay code:{}", directoryPath, replayCode);
            if (replayCode != FTPReply.COMMAND_OK
                    && replayCode != FTPReply.PATHNAME_CREATED) {
                return false;
            }
        }
        return true;
    }
}
