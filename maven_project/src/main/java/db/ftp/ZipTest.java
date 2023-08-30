package db.ftp;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipTest {
    static class Record {
        String fileName;
        byte[] data;

        public Record(String fileName, byte[] data) {
            this.fileName = fileName;
            this.data = data;
        }
    }


    public static void main(String[] args) throws IOException {
        /* 创建 outputstream 和 inputstream 之间的转换*/
        PipedOutputStream pos = new PipedOutputStream();
        PipedInputStream pis = new PipedInputStream();
        pos.connect(pis);

        /* 使用pos初始化zos，相当于zos和pis连接起来 */
        ZipOutputStream zos = new ZipOutputStream(pos);


        /* 相当于在dataex中传输records */
        List<Record> recordList = new ArrayList<>();

        for (int i = 0; i <= 10; i++) {
            String sid = Integer.toString(i);
            byte[] media = "format=xxxx,lenth=yyyyyy".getBytes();
            zos.putNextEntry(new ZipEntry(sid));
            zos.write(media);
            zos.closeEntry();
            zos.flush();
            if (i == 10) {
                zos.close();
            }
            byte[] bytes = new byte[pis.available()];
            pis.read(bytes);
            recordList.add(new Record("D:\\test\\1.zip", bytes));
        }

        /* 在ftp端恢复 */
        FileOutputStream fos = new FileOutputStream("/Users/epochong/IdeaProjects/maven_project/output/1.zip");
        recordList.forEach(x -> {
            try {
                fos.write(x.data);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
