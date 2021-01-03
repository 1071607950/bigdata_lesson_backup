package uvstat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UvReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,Context context)
			throws IOException, InterruptedException {
		int s=0;
		for(IntWritable v:value)
			s=s+v.get();
		context.write(key, new IntWritable(s));
	}

}
