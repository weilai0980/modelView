package basetool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class rawQuery {

	public HBaseOp tabop = new HBaseOp();
	public String prekey = "abcdefghijklmnopqrstuvwxyz";
	
	public rawMR rmr;// = new rawMR(int mo, String taint sign) 

	public rawQuery() {

	}

	public String rowParser(String str) {
		int len = str.length();
		int i = 0;
		for (i = 0; i < len; ++i) {
			if (str.charAt(i) == ':') {
				break;
			}
		}
		return str.substring(i + 1, len);
	}

	public String rowParserTabVal(String str) {
		int len = str.length();
		int i = 0;
		int st = 0;
		for (i = 0; i < len; ++i) {
			if (str.charAt(i) == ':') {

				if (st != 0) {
					break;
				}
				st = i + 1;
			}
		}
		return str.substring(st, i);
	}

	public double pointQryTemp(double val, String tabname) {

		String[] qual = new String[3];
		qual[0] = "val";

		//String rkey = "temp:" + Double.toString(val);
		String rkey = "temp:" + Long.toString((long)val)+"";

		if (tabop.get(tabname, rkey, "attri", "val") == true) {
			tabop.getQual(tabname, rkey, "attri", "val");
		} else {

		}

		// while ((tmpRes = tabop.scanR()) != "NO") {
		// num++;
		// tp=Double.parseDouble(rowParser(tmpRes));
		// if (Math.abs(tp-val)<=1e-1) {
		// tabop.scanGet("attri","val");
		//
		// } else if (tp>val) {
		// break;
		// }
		// tabop.scanMNext();
		// }
		return 1;
	}

	public double rangeQryTemp(double l, double r, String tabname) throws Exception {
		String tmpRes = "";
		double num = 0;

		long tl=(long)l, tr=(long)r;
		tabop.scanIni(tabname, "temp:" + Long.toString(tl),
				"temp:" + Long.toString(tr+1));

		
		//...........test.........................//
//		String str1="temp:" + Long.toString(tl);
//		String str2="temp:" + Long.toString(tr+1);
//		System.out.printf("%s  %s\n",str1,str2);
		
		//........................................//
		
		
		rmr= new rawMR(0);
		
		num=rmr.jobIntervalEvalQuery(tabname,
				"temp:" + Long.toString(tl)+".0","temp:" + Long.toString(tr+1)+".0", "val"); 
		
		
//		tabop.scanMNext();
//
//		while ((tmpRes = tabop.scanR()) != "NO") {
//			num++;
//
//			tabop.scanGet("attri", "val");
//
//			tabop.scanMNext();
//		}
		return num;
	}

	public double pointQryVal(double val, String tabname) throws Exception {
		
		
		String tmpRes = "";
		String[] qual = new String[3];
		qual[0] = "val";
		double num = 0;

		tabop.scanIni(tabname, "temp:" + valReg((long)val) + ":", "temp:"
				+ valReg((long)val+1) + ":");
		
        rmr= new rawMR(1);
		
		num=rmr.jobIntervalEvalQuery(tabname,
				"temp:" + valReg((long)val) + ":","temp:"
				+ valReg((long)val+1) + ":", "temp"); 
		

		tabop.scanMNext();

//		while ((tmpRes = tabop.scanR()) != "NO") {
//			num++;
//
//			tabop.scanGet("attri", "temp");
//
//			tabop.scanMNext();
//		}
		return num;	
//		
//
//		String tmpRes = "temp:" + valReg((long)val);
//		System.out.print(tmpRes+"\n");
//		
//		tabop.scanPrefixFilter(tabname, tmpRes);
//		tabop.scanMNext();
//		double num = 0.0;
//
//		while ((tmpRes = tabop.scanR()) != "NO") {
//			num++;
//			tabop.scanGet("attri", "temp");
//			tabop.scanMNext();
//		}
//		return num;
	}

	public String valReg(long regval) {
		long tmp = regval;
		int num = 0;
		while (tmp != 0) {
			tmp = tmp / 10;
			num++;
		}
		if (num == 0)
			num = 1;
		return prekey.charAt(num - 1) + "," + regval;
	}
	public double rangeQryVal(double l, double r, String tabname) throws Exception {
		String tmpRes = "";
		String[] qual = new String[3];
		qual[0] = "val";
		double num = 0;

		tabop.scanIni(tabname, "temp:" + valReg((long)l) + ":", "temp:"
				+ valReg((long)r+1) + ":");

		
		  rmr= new rawMR(1);
			
			num=rmr.jobIntervalEvalQuery(tabname,
					"temp:" + valReg((long)l) + ":",
					"temp:"+ valReg((long)r+1) + ":","temp"); 
			

			tabop.scanMNext();
		
		
		
//		
//		tabop.scanMNext();
//
//		while ((tmpRes = tabop.scanR()) != "NO") {
//			num++;
//
//			tabop.scanGet("attri", "temp");
//
//			tabop.scanMNext();
//		}
		return num;
	}

	// public static class mapTempRange extends
	// TableMapper<ImmutableBytesWritable, Text> {
	//
	// private double cntnum = 0;
	// private Text model = new Text();
	//
	// String modinfor = new String();
	//
	// double[] tmpbd = new double[3];
	// ImmutableBytesWritable intkey = new ImmutableBytesWritable();
	// double lb = 0.0, rb = 0.0;
	// int modOrd = 0;
	//
	// String lbstr = new String();
	// String rbstr = new String();
	// String modOrdstr = new String();
	//
	// public void rowkeyParser(String rowkey, double bound[]) // 0 left, 1
	// // right
	// {
	// int len = rowkey.length(), seg1 = 0, seg2 = 0, seg3 = 0;
	// for (int i = 0; i < len; ++i) {
	// if (rowkey.charAt(i) == ',') {
	// if (seg1 == 0) {
	// seg1 = i + 1;
	// } else if (seg2 == 0) {
	// seg2 = i + 1;
	// bound[0] = Double
	// .parseDouble(rowkey.substring(seg1, i));
	// bound[1] = Double.parseDouble(rowkey.substring(seg2,
	// len));
	// break;
	// }
	// }
	// }
	// return;
	// }
	//
	// public String modelContract(int order, String lb, String rb,
	// String modinfor) {
	// String res = "";
	// res = lb + ",";
	// res += rb + ",";
	//
	// return res + modinfor;
	// }
	//
	// @Override
	// public void map(ImmutableBytesWritable row, Result values,
	// Context context) throws IOException {
	//
	// modinfor = Bytes.toString(values.getValue(Bytes.toBytes("model"),
	// Bytes.toBytes("cof1")));
	//
	// rowkeyParser(Bytes.toString((row.get())), tmpbd);
	//
	// try {
	// Configuration conf = context.getConfiguration();
	// lbstr = conf.get("lb");
	// rbstr = conf.get("rb");
	// modOrdstr = conf.get("modelOrd");
	//
	// lb = Double.parseDouble(lbstr);
	// rb = Double.parseDouble(rbstr);
	// modOrd = Integer.parseInt(modOrdstr);
	//
	// model.set(modelContract(modOrd, Double.toString(tmpbd[0]),
	// Double.toString(tmpbd[1]), modinfor));
	// // gTabNum++;
	//
	// if (lb > tmpbd[1] || rb < tmpbd[0]) // no overlap
	// {
	//
	// } else {
	// // qualNum++;
	// cntnum++;
	//
	// intkey.set(Bytes.toBytes(cntnum));
	//
	// context.write(intkey, model);
	// }
	// } catch (InterruptedException e) {
	// throw new IOException(e);
	// }
	// }
	// }

}
