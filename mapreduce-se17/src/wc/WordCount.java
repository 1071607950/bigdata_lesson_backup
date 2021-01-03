package wc;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCount {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.setProperty("HADOOP_USER_NAME", "root");
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://node1:9820");
		conf.set("mapreduce.framework.name", "local");

		Job job=Job.getInstance(conf);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);
		
		Path outpath=new Path("/wc/result");
		if(outpath.getFileSystem(conf).exists(outpath)) {
			outpath.getFileSystem(conf).delete(outpath,true);
		}

		FileInputFormat.addInputPath(job, new Path("/wc/wc.txt"));
		FileOutputFormat.setOutputPath(job, outpath);
		
		
		
		System.exit(job.waitForCompletion(true)?0:-1);
	}

}
