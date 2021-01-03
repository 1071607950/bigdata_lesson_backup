package uvstat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UvMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text k=new Text();
	private IntWritable v=new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] strs=value.toString().split("\t");
		k.set(strs[0]+":"+strs[1]);
		context.write(k, v);
	}
}
