package collect;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimerTask;

import org.apache.hadoop.fs.FileUtil;


public class CleanTask extends TimerTask {
	private String backup="D:/workspace/logs/backup/";
	@Override
	public void run() {
		SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd-HH");
		Calendar calendar=Calendar.getInstance();
		calendar.add(Calendar.DAY_OF_MONTH, -3);
		String d=sdf.format(calendar.getTime());
		
		File bFile=new File(backup);
		File[] files=bFile.listFiles();
		for(File f:files) {
			if(f.getName().substring(0, 8).compareTo(d)<=0)
				FileUtil.fullyDelete(f);
			
		}
	}

}
