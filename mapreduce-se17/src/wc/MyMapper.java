package wc;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private Text k=new Text();
	private IntWritable v=new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] words=value.toString().split(" ");
		for(String word:words) {
			if(word.startsWith("h")) {
				k.set(word);
				context.write(k, v);
			}
		}
		
	}
}
