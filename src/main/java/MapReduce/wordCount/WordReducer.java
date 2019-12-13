package MapReduce.wordCount;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Reducer<Text, IntWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        long count = 0;

        //��ʱ��������shuffle�����
        //�����key���ַ����������value��shuffle�ɷ���������һ����list
        for(IntWritable v : values) {
            count += v.get();
        }
        context.write(key, new LongWritable(count));
    }

}

