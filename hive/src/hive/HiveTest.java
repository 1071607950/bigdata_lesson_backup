package hive;

import java.sql.*;

public class HiveTest {
	private static String driverName="org.apache.hive.jdbc.HiveDriver";
	private static String url="jdbc:hive2://192.168.2.2:10000/default";
	private static String user="root";
	private static String password="";
	public static void main(String[] args) {
		try {
			Class.forName(driverName);
			Connection conn=DriverManager.getConnection(url,user,password);
			Statement stmt=conn.createStatement();
			String sql="select count(*) from log";
			ResultSet res=stmt.executeQuery(sql);
			if(res.next()) {
				System.out.println(res.getString(1));
			}
			conn.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
