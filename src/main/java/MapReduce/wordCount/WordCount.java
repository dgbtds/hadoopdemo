package MapReduce.wordCount;

import Hdfs.HdfsUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

/**
 * @author WuYe
 * @vesion 1.0 2019/12/13
 * /
 * /**
 * @program: hadoopdemo
 * @description:
 * @author: WuYe
 * @create: 2019-12-13 20:33
 **/

public class WordCount {
    private static String uri="hdfs://192.168.37.207:9000/";
    private static String log4jPropertiesPath="/log4j.properties";
    private static String owner="wy";
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        //配置了fs.defaultFS选项，则下面的setInputPaths等配置不需要再配置前缀
        conf.set("fs.defaultFS", "hdfs://192.168.37.207:9000/");
        //MapReduce运行环境为yarn
        conf.set("mapreduce.framework.name", "yarn");
        //yarn的recourseManager的主机名
        conf.set("yarn.resourcemanager.hostname", "192.168.37.207");
        //表示MapReduce是跨平台运行的
        conf.set("mapreduce.app-submission.cross-platform", "true");

        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI(uri),conf,owner);
            InputStream is = fileSystem.open(new Path(log4jPropertiesPath));
            Properties properties = new Properties();
            properties.load(is);
            PropertyConfigurator.configure(properties);
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }


        Job job = Job.getInstance(conf);

        job.setJar("build\\libs\\hadoopdemo-1.0-SNAPSHOT.jar");
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, "/input/wordcount/test1.txt");
        FileOutputFormat.setOutputPath(job, new Path("/output/wordcount/result_of_test1.txt"));

        job.waitForCompletion(true);
    }

}

