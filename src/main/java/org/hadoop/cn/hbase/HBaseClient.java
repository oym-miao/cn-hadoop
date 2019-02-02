package org.hadoop.cn.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseClient {

	public static HTable getTable(String name) throws Exception {
		//获取配置，使用HBaseConfiguration
		Configuration conf = HBaseConfiguration.create();
		//操作的表
		HTable table = new HTable(conf, name);

		return table;
	}


	public static void getData(HTable table) throws Exception {

		//实例化一个get，指定一个rowkey
		Get get = new Get(Bytes.toBytes("2018_1001"));
		//get某列的值
		get.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age"));
		//get一个列簇
		//get.addFamily(Bytes.toBytes("f1"));

		//定义一个result
		Result rs = table.get(get);
//		//打印数据
//		for (Cell cell : rs.rawCells()) {
//			System.out.println(
//					Bytes.toString(CellUtil.cloneFamily(cell)) + " *** " + Bytes.toString(CellUtil.cloneQualifier(cell))
//							+ " *** " + Bytes.toString(CellUtil.cloneValue(cell)) + " *** " + cell.getTimestamp());
//			System.out.println("---------------------------------------------");
//		}
		
		// get加载到表中
		table.get(get);
	}

	public static void putData(HTable table) throws Exception {

		Put put = new Put(Bytes.toBytes("20180218_1325"));
		put.add(Bytes.toBytes("f1"), Bytes.toBytes("sex"), Bytes.toBytes("male"));

		table.put(put);
		
		getData(table);
	}

	public static void deleteData(HTable table) throws Exception {
		Delete del = new Delete(Bytes.toBytes("20180218_1325"));
		del.deleteColumns(Bytes.toBytes("f1"), Bytes.toBytes("sex"));
		
		table.delete(del);
		
		getData(table);
	}

	public static void scanData(HTable table) throws Exception {

		Scan scan = new Scan();

		ResultScanner rsscan = table.getScanner(scan);

		for (Result rs : rsscan) {
			System.out.println(Bytes.toString(rs.getRow()));
			for (Cell cell : rs.rawCells()) {
				System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)) + " *** "
						+ Bytes.toString(CellUtil.cloneQualifier(cell)) + " *** "
						+ Bytes.toString(CellUtil.cloneValue(cell)) + " *** " + cell.getTimestamp());
			}
			System.out.println("---------------------------------------------");
		}
	}

	public static void rangeData(HTable table) throws Exception {

		Scan scan = new Scan();

		// conf the scan
		//scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("name"));
		scan.setStartRow(Bytes.toBytes("20180218_1325"));
		scan.setStopRow(Bytes.toBytes("20180218_1328"));

		ResultScanner rsscan = table.getScanner(scan);
		for (Result rs : rsscan) {
			System.out.println(Bytes.toString(rs.getRow()));
			for (Cell cell : rs.rawCells()) {
				System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)) + " *** "
						+ Bytes.toString(CellUtil.cloneQualifier(cell)) + " *** "
						+ Bytes.toString(CellUtil.cloneValue(cell)) + " *** " + cell.getTimestamp());
			}
			System.out.println("---------------------------------------------");
		}
	}

	public static void main(String[] args) throws Exception {

		HTable table = getTable("t2");
		//get数据
		getData(table);
		
		//添加数据
		//putData(table);
		
		//删除数据
		//deleteData(table);
		
		//scan数据
		//scanData(table);
		
		//scan范围查看
		//rangeData(table);
	}
}
