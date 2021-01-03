package uvstat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UvReducer extends Reducer<Text, IntWritable, Text, Text> {
	private Text k=new Text();
	private Text v=new Text();
	protected void reduce(Text key, Iterable<IntWritable> value,Context context)
			throws IOException, InterruptedException {
		String[] strs=key.toString().split(":");
		k.set(strs[1]);
		v.set(strs[0]);
		context.write(k, v);
	}

}
