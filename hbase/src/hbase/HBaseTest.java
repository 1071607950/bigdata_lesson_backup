package hbase;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTest {
	public static Connection conn=null;
	public static void main(String[] args) throws IOException {
		getConn();
		//createTable();
		putData();
		//getData()
		scanData();
		//scanData1();
		//delData();
		closeConn();
	}
	public static void createTable() throws IOException {
		Admin admin=conn.getAdmin();
		if(admin.tableExists(TableName.valueOf("student"))) {
			admin.disableTable(TableName.valueOf("student"));
			admin.deleteTable(TableName.valueOf("student"));
		}
		TableDescriptorBuilder tb=
		TableDescriptorBuilder.newBuilder(TableName.valueOf("student"));
		ColumnFamilyDescriptorBuilder cf1=
		ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("baseinfo"));
		ColumnFamilyDescriptorBuilder cf2=
		ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("other"));
		ArrayList<ColumnFamilyDescriptor> list=new ArrayList<ColumnFamilyDescriptor>();
		list.add(cf1.build());
		list.add(cf2.build());
		tb.setColumnFamilies(list);
		admin.createTable(tb.build());
	}
	public static void putData() throws IOException {
		Table table=conn.getTable(TableName.valueOf("student"));
		Put row1=new Put(Bytes.toBytes("1001"));
		row1.addColumn(Bytes.toBytes("baseinfo"), Bytes.toBytes("name"), Bytes.toBytes("zs"));
		row1.addColumn(Bytes.toBytes("baseinfo"), Bytes.toBytes("age"), Bytes.toBytes("20"));
		row1.addColumn(Bytes.toBytes("other"), Bytes.toBytes("phone"), Bytes.toBytes("110"));
		table.put(row1);
		Put row2=new Put(Bytes.toBytes("1002"));
		row2.addColumn(Bytes.toBytes("baseinfo"), Bytes.toBytes("name"), Bytes.toBytes("xh"));
		row2.addColumn(Bytes.toBytes("baseinfo"), Bytes.toBytes("age"), Bytes.toBytes("25"));
		row2.addColumn(Bytes.toBytes("other"), Bytes.toBytes("phone"), Bytes.toBytes("119"));
		table.put(row2);
	}
	public static void getData() throws IOException {
		Table table=conn.getTable(TableName.valueOf("student"));
		Get row=new Get(Bytes.toBytes("1001"));
		row.addColumn(Bytes.toBytes("baseinfo"), Bytes.toBytes("name"));
		row.addColumn(Bytes.toBytes("baseinfo"), Bytes.toBytes("age"));
		Result result=table.get(row);
		System.out.println(
		Bytes.toString(
		result.getValue(Bytes.toBytes("baseinfo"), Bytes.toBytes("name"))));
		System.out.println(
		Bytes.toString(
		result.getValue(Bytes.toBytes("baseinfo"), Bytes.toBytes("age"))));
	}
	public static void delData() throws IOException {
		Table table=conn.getTable(TableName.valueOf("student"));
		Delete row=new Delete(Bytes.toBytes("1002"));
		table.delete(row);
	}
	public static void scanData() throws IOException {
		Table table=conn.getTable(TableName.valueOf("student"));
		Scan scan=new Scan();
		ResultScanner rs=table.getScanner(scan);
		for(Result r:rs) {
			 System.out.println(
			 Bytes.toString(
			 r.getValue(Bytes.toBytes("baseinfo"),Bytes.toBytes("name"))));
			 System.out.println(
			 Bytes.toString(
			 r.getValue(Bytes.toBytes("baseinfo"),Bytes.toBytes("age"))));
			 System.out.println(
			 Bytes.toString(
			 r.getValue(Bytes.toBytes("other"),Bytes.toBytes("phone"))));
		} 
	}
	public static void scanData1() throws IOException {
		Table table=conn.getTable(TableName.valueOf("student"));
		Scan scan=new Scan();
		SingleColumnValueFilter filter1=
		new SingleColumnValueFilter(
		Bytes.toBytes("baseinfo"),
		Bytes.toBytes("name"), 
		CompareOperator.EQUAL, 
		Bytes.toBytes("xh"));
		SingleColumnValueFilter filter2=new SingleColumnValueFilter(
		 Bytes.toBytes("baseinfo"),
		 Bytes.toBytes("age"), 
		 CompareOperator.LESS_OR_EQUAL, 
		 Bytes.toBytes("20"));
		FilterList filterList=
		 new FilterList(FilterList.Operator.MUST_PASS_ALL); 
		filterList.addFilter(filter1);
		filterList.addFilter(filter2);
		scan.setFilter(filterList);
		ResultScanner rs=table.getScanner(scan);
		for(Result r:rs) {
			 System.out.println(
			 Bytes.toString(
			 r.getValue(Bytes.toBytes("baseinfo"),Bytes.toBytes("name"))));
			 System.out.println(
			 Bytes.toString(
			 r.getValue(Bytes.toBytes("baseinfo"),Bytes.toBytes("age"))));
			 System.out.println(
			 Bytes.toString(
			 r.getValue(Bytes.toBytes("other"),Bytes.toBytes("phone"))));
		} 
	}
	public static void getConn() throws IOException {
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "node1");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conn=ConnectionFactory.createConnection(conf);
	}
	public static void closeConn() throws IOException {
		conn.close();
	} 
}