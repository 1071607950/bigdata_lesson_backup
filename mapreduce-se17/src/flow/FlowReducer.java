package flow;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReducer  extends Reducer<Text, FlowValue, Text, Text> {
	private Text v=new Text();
	protected void reduce(Text key, Iterable<FlowValue> value,Context context)
			throws IOException, InterruptedException {
		int up=0;
		int down=0;
		int total=0;
		int sum=0;
		for(FlowValue v:value) {
			up=up+v.getUp();
			down=down+v.getDown();
			total=total+v.getTotal();
			sum+=v.getSum();
		}
		v.set(up+"\t"+down+"\t"+total+"\t"+sum);
		context.write(key, v);
	}

}
