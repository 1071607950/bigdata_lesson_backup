package flow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowStat {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.setProperty("HADOOP_USER_NAME", "root");
		//配置
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://node1:9820");
		conf.set("mapreduce.framework.name", "local");
		//job
		Job job=Job.getInstance(conf);
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowValue.class);
		job.setNumReduceTasks(1);
		//数据读取路径和写出路径
		Path outpath=new Path("/logs/result6");
		if(outpath.getFileSystem(conf).exists(outpath)) {
			outpath.getFileSystem(conf).delete(outpath,true);
		}

		FileInputFormat.addInputPath(job, new Path("/logs/test.log"));
		FileOutputFormat.setOutputPath(job, outpath);
		//执行
		System.exit(job.waitForCompletion(true)?0:-1);
	}

}
