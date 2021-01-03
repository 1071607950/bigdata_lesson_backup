package flow;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowValue> {
	private Text k=new Text();
	private FlowValue v=new FlowValue();
	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] strs=value.toString().split("\t");
		k.set(strs[0]);
		v.setUp(Integer.parseInt(strs[3]));
		v.setDown(Integer.parseInt(strs[4]));
		v.setTotal(v.getUp()+v.getDown());
		v.setSum(1);
		context.write(k, v);
	}
}
