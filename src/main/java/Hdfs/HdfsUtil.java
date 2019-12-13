package Hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author WuYe
 * @vesion 1.0 2019/12/13
 * /
 * /**
 * @program: hadoopdemo
 * @description:
 * @author: WuYe
 * @create: 2019-12-13 19:26
 **/
public class HdfsUtil {
    private static String uri="hdfs://192.168.37.207:9000/";
    private static String owner="wy";
    public static void main(String[] args) {
        HdfsUtil.upload("D:\\bigdata\\wordcount\\input\\test1.txt", "/input/wordcount/test1.txt");
    }
    static public boolean upload(String src,String dst){
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI(uri),new Configuration(),owner);
            Path path = new Path(dst);
            if (fileSystem.exists(path)){
               fileSystem.delete(path,false);
            }
            fileSystem.copyFromLocalFile(new Path(src),new Path(dst));
            fileSystem.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        } catch (URISyntaxException e) {
            e.printStackTrace();
            return false;
        }
    }
}
