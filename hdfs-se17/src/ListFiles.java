import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class ListFiles {

	public static void main(String[] args) throws IOException {
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://node1:9820");
		FileSystem fs=FileSystem.get(conf);
		RemoteIterator<LocatedFileStatus> listFiles=fs.listFiles(new Path("/test"), false);
		while(listFiles.hasNext()) {
			LocatedFileStatus file=listFiles.next();
			System.out.println("name:"+file.getPath());
			System.out.println("blocksize:"+file.getBlockSize());
			System.out.println("location:"+Arrays.toString(file.getBlockLocations()));
		}
	}

}
