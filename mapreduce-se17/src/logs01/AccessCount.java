package logs01;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AccessCount {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.setProperty("HADOOP_USER_NAME", "root");
		//设参数
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://node1:9820");
		conf.set("mapreduce.framework.name", "local");
		//设置job
		Job job=Job.getInstance(conf);
		job.setMapperClass(AccessMapper.class);
		job.setReducerClass(AccessReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
		//数据读取路径和写出路径
		Path outpath=new Path("/logs/result1");
		if(outpath.getFileSystem(conf).exists(outpath)) {
			outpath.getFileSystem(conf).delete(outpath,true);
		}

		FileInputFormat.addInputPath(job, new Path("/logs/test.log"));
		FileOutputFormat.setOutputPath(job, outpath);
		
		
		
		System.exit(job.waitForCompletion(true)?0:-1);
	}

}
