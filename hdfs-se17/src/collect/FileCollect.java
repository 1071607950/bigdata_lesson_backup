package collect;

import java.util.Timer;

public class FileCollect {

	public static void main(String[] args) {
		Timer timer=new Timer();
		timer.schedule(new CollectTask(),0,10000);
		//timer.schedule(new CleanTask(),0, 24*60*60*1000);
		
	}

}
