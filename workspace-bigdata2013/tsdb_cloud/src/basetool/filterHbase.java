package basetool;

import java.io.BufferedWriter;
import java.io.FileWriter;
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
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.Arrays;

import org.apache.hadoop.hbase.filter.Filter;

import org.apache.hadoop.hbase.filter.FilterList.Operator;

public class filterHbase {

	public Configuration config = HBaseConfiguration.create();
	public int modord;

	public filterHbase(int modorder) {
		modord = modorder;

	}

	public void modelEval(double model[], double lb, double rb) {

		double step = (rb - lb) / 10.0;

		if (step > 0) {
			for (double i = lb; i <= rb; i = i + step) {

			}
		}
		return;
	}

	public double pointQuery(String tablename, double val, String cf,
			String qual[]) throws IOException {

		double[] modinfor = new double[10];
		HTable table = new HTable(config, tablename);

		double cnt = 0.0;
		String vlstr = "", vrstr = "";
		double vlv = 0.0, vrv = 0.0;

		Scan s1 = new Scan();
		s1.addColumn("attri".getBytes(), qual[0].getBytes());
		s1.addColumn("attri".getBytes(), qual[1].getBytes());

		s1.addFamily("model".getBytes());
	//	String valstr = Long.toString((long)val);
		
		String valstr = Double.toString(val);

		List list = new ArrayList<Filter>();
		Filter f1=new SingleColumnValueFilter(cf.getBytes(), qual[0]
				.getBytes(), CompareOp.LESS_OR_EQUAL,  Bytes.toBytes(valstr));

		list.add(f1);
		Filter f2=new SingleColumnValueFilter(cf.getBytes(), qual[1]
				.getBytes(), CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(valstr));
		list.add(f2);
		
		FilterList filterList = new FilterList(
				FilterList.Operator.MUST_PASS_ALL, list);

		s1.setFilter(filterList);
		ResultScanner Res = table.getScanner(s1);

		for (Result res : Res) {

			cnt++;
			valstr = new String(res.getValue("model".getBytes(),
					"coef0".getBytes()));
			vlstr = new String(res.getValue("attri".getBytes(),
					qual[0].getBytes()));
			vrstr = new String(res.getValue("attri".getBytes(),
					qual[1].getBytes()));
			modinfor[0] = Double.parseDouble(valstr);

			vlv = Double.parseDouble(vlstr);
			vrv = Double.parseDouble(vrstr);

			modelEval(modinfor, vlv, vrv);

		}
		Res.close();

		return cnt;
	}

	public void rangeEval(double model[], double l, double r) {

		double tmp = 0.0;
		double step = (r - l) / 10.0;
		if (step > 0) {
			for (double i = l; i <= r; i += step) {
				tmp = model[0];
			}
		}
		return;
	}

	public double rangeQuery(String tablename, double l, double r, String cf,
			String qual[]) throws IOException {

		double[] modinfor = new double[10];
		double cnt = 0.0;
		String modstr = "", vlstr = "", vrstr = "";
		double vlv = 0.0, vrv = 0.0;

		HTable table = new HTable(config, tablename);

		Scan s1 = new Scan();
		
		s1.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(qual[0]));
		s1.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(qual[1]));
		s1.addFamily(Bytes.toBytes("model"));

	//	String lstr = Long.toString((long)l), rstr = Long.toString((long)r);
		String lstr =Double.toString(l), rstr = Double.toString(r);
		
		List list = new ArrayList<Filter>();
		Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("attri"),
				qual[0].getBytes(), CompareOp.LESS_OR_EQUAL,  Bytes.toBytes(lstr));
	
		Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("attri"),
				qual[1].getBytes(), CompareOp.GREATER_OR_EQUAL,  Bytes.toBytes(lstr));
		
		list.add(filter1);
		list.add(filter2);
		FilterList filterList1 = new FilterList(FilterList.Operator.MUST_PASS_ALL,list);
		
		
		List list2 = new ArrayList<Filter>();
		Filter filter12 = new SingleColumnValueFilter(Bytes.toBytes("attri"), qual[0]
		     .getBytes(), CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(lstr));
	
		Filter filter22 = new SingleColumnValueFilter(Bytes.toBytes("attri"),
				qual[0].getBytes(), CompareOp.LESS_OR_EQUAL, Bytes.toBytes(rstr));
		list2.add(filter12);
		list2.add(filter22);
		
		FilterList filterList2 = new FilterList(FilterList.Operator.MUST_PASS_ALL,list2);
		
		FilterList chieffl = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		chieffl.addFilter(filterList1);
		chieffl.addFilter(filterList2);

		s1.setFilter(chieffl);
		ResultScanner Res = table.getScanner(s1);
		
		for (Result res : Res) {
			cnt++;

			modstr = new String(res.getValue("model".getBytes(),
					"coef0".getBytes()));
			vlstr = new String(res.getValue("attri".getBytes(),
					qual[0].getBytes()));
			vrstr = new String(res.getValue("attri".getBytes(),
					qual[1].getBytes()));
		

			modinfor[0] = Double.parseDouble(modstr);
			vlv = Double.parseDouble(vlstr);
			vrv = Double.parseDouble(vrstr);
			
			modelEval(modinfor, vlv, vrv);
		}

		Res.close();

		return cnt;
	}
}
