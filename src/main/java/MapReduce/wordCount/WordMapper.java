package MapReduce.wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author WuYe
 * @vesion 1.0 2019/12/13
 * /
 * /**
 * @program: hadoopdemo
 * @description:
 * @author: WuYe
 * @create: 2019-12-13 20:31
 **/

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {

        //���յ�split�е���������Ϊmap������
        String line = value.toString();
        String[] words = line.split(" ");

        //�ŵ�list�У�ÿ��textͳ�Ƽ�1����Ϊmap�����
        //д��context�����Ļ����У��������һ��ִ�й���shuffle
        for(String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }

}


