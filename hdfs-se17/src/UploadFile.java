import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class UploadFile {

	public static void main(String[] args) throws IOException {
		System.setProperty("HADOOP_USER_NAME", "root");
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://node1:9820");
		conf.set("dfs.replication","1");
		conf.set("dfs.blocksize", "1m");
		
		
		FileSystem fs=FileSystem.get(conf);
		fs.copyFromLocalFile(new Path("D:/test.log"), new Path("/test"));
		fs.close();
	}

}
