package collect;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimerTask;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CollectTask extends TimerTask {
	private String source="D:/workspace/logs/source";
	private String tohdfs="D:/workspace/logs/tohdfs";
	private String backup="D:/workspace/logs/backup/";
	private String hdfslog="/logs/";
	
	@Override
	public void run() {
		/**
		 * 
		 */
		SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd-HH");
		String d=sdf.format(new Date());
		File sFile=new File(source);
		File[] files=sFile.listFiles(new MyFilter());
		
		for(File f:files) {
			f.renameTo(new File(tohdfs+"/"+f.getName()));
		}
		
		File toFile=new File(tohdfs);
		
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://node1:9820");
		conf.set("dfs.replication","1");
		conf.set("dfs.blocksize", "1m");
		FileSystem fs=null;
		try {
			fs=FileSystem.get(conf);
			if(!fs.exists(new Path(hdfslog+d)))
				fs.mkdirs(new Path(hdfslog+d));
			File[] uploadFiles=toFile.listFiles();
			for(File f:uploadFiles) {
				String fname=f.getName()+"."+UUID.randomUUID();
				fs.copyFromLocalFile(new Path(f.getAbsolutePath()), new Path(hdfslog+d+"/"+fname));
				//备份
				File bFile=new File(backup+d);
				if(!bFile.exists())
					bFile.mkdir();
				f.renameTo(new File(backup+d+"/"+fname));	
			}
			fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
