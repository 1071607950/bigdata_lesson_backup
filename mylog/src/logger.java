import java.util.ArrayList;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
public class logger {
	public static void main(String[] args) throws InterruptedException {
		/**
		 * 数据
		 * ip 网址  时间  上行流量 下行流量   
		 */
		//PropertyConfigurator.configure("log4j.properties");
		Logger log=Logger.getLogger(logger.class.getName());
		ArrayList<String> ips=new ArrayList<String>();
		ips.add("192.168.1.101");
		ips.add("192.168.2.101");
		ips.add("192.168.3.101");
		ips.add("192.168.4.101");
		ips.add("192.168.5.101");
		ips.add("192.168.6.101");
		ips.add("192.168.7.101");
		ips.add("192.168.8.101");
		ips.add("192.168.9.101");
		ips.add("192.168.10.101");
		ips.add("210.204.108.161");
		ips.add("210.204.108.162");
		ips.add("210.204.108.163");
		ips.add("210.204.108.164");
		ips.add("210.204.108.165");
		ips.add("210.204.108.166");
		ips.add("210.204.108.167");
		ips.add("210.204.108.168");
		ips.add("210.204.108.169");

		ArrayList<String> pages=new ArrayList<String>();
		pages.add("www.baidu.com");
		pages.add("www.sina.com");
		pages.add("hadoop.apache.org");
		pages.add("www.oracle.com");
		pages.add("city.dlut.edu.cn");
		pages.add("www.yahoo.com");
		pages.add("www.google.com");
		pages.add("hbase.apache.org");
		pages.add("www.mysql.com");
		pages.add("www.csdn.net");
		
		ArrayList<String> years=new ArrayList<String>();
		years.add("2019");
		years.add("2018");
		ArrayList<String> months=new ArrayList<String>();



		while(true) {
			String month=""+(Math.round(Math.random()*1000)%12+1);
			month=month.length()==2 ? month : "0"+month;
			String day=""+(Math.round(Math.random()*1000)%30+1);
			day=day.length()==2 ? day : "0"+day;
			String hour=""+Math.round(Math.random()*1000)%24;
			hour=hour.length()==2 ? hour : "0"+hour;
			String minute=""+Math.round(Math.random()*1000)%60;
			minute=minute.length()==2 ? minute : "0"+minute;
			String second=""+Math.round(Math.random()*1000)%60;
			second=second.length()==2 ? second : "0"+second;
			
			String msg=ips.get((int)Math.round(Math.random()*100)%ips.size());
			msg=msg+"	"+pages.get((int)Math.round(Math.random()*123)%pages.size());
			msg=msg+"	"+years.get((int)Math.round(Math.random()*1000)%years.size());
			msg=msg+"-"+month+"-"+day+" "+hour+":"+minute+":"+second;
			msg=msg+"	"+(Math.round(Math.random()*1000));
			msg=msg+"	"+(Math.round(Math.random()*1000));
			log.info(msg);
			Thread.sleep(50);
		}
	}
}
