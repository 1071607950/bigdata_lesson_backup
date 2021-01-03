package uvstat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UvStat {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.setProperty("HADOOP_USER_NAME", "root");
		//配置
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://node1:9820");
		conf.set("mapreduce.framework.name", "local");
		//job
		Job job=Job.getInstance(conf);
		job.setMapperClass(UvMapper.class);
		job.setReducerClass(UvReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
		//数据读取路径和写出路径
		FileInputFormat.addInputPath(job, new Path("/logs/test.log"));
		Path outpath=new Path("/logs/result4");
		if(outpath.getFileSystem(conf).exists(outpath)) {
			outpath.getFileSystem(conf).delete(outpath,true);
		}
		FileOutputFormat.setOutputPath(job, outpath);
		//执行
		boolean res1=job.waitForCompletion(true);
		boolean res2=false;
		if(res1) {
			//job
			Job job2=Job.getInstance(conf);
			job2.setMapperClass(UvMapper2.class);
			job2.setReducerClass(UvReducer2.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(IntWritable.class);
			job2.setNumReduceTasks(1);
			//数据读取路径和写出路径
			FileInputFormat.addInputPath(job2, new Path("/logs/result4/part-r-00000"));
			Path outpath2=new Path("/logs/result5");
			if(outpath2.getFileSystem(conf).exists(outpath2)) {
				outpath2.getFileSystem(conf).delete(outpath2,true);
			}
			FileOutputFormat.setOutputPath(job2, outpath2);
			//执行
			res2=job2.waitForCompletion(true);
		}
		if(res1&&res2)
			System.exit(0);
		System.exit(-1);
		
		
	}

}
