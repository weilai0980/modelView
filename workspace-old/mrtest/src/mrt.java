import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class mrt {

	public static int modorder;
	public int rednum;
	public static double qualNum;
	public static double gTabNum;
	public static double cnt = 0.0;
	public static tabOperation tabop = new tabOperation();

	// /public String tabname;

	public mrt(int modelord) {
		modorder = modelord;
		rednum = 5;

		qualNum = 0;
		gTabNum = 0;
	}

	public static class mapValPointTest1 extends
			TableMapper<ImmutableBytesWritable, IntWritable> {
		// TableMapper<Text, IntWritable> {

		Text interkey = new Text();
		IntWritable ONE = new IntWritable(1);

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {

			cnt++;

			String ikey = "", rkey = new String(row.get());
			int len = rkey.length(), st = 0;
			// for(int i=0;i<len;++i)
			// {
			// if(rkey.charAt(i)==',' && st==0)
			// {
			// st=i+1;
			// }
			// else if(rkey.charAt(i)==',')
			// {
			// ikey=rkey.substring(st,i);
			// break;
			// }
			// }
			// interkey.set(row);
			try {
				context.write(row, ONE);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	public static class ReducerEvalTest1
			extends
			// TableReducer<Text, IntWritable, ImmutableBytesWritable> {
			TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {

		// public Put put;
		public static double putnum = 0.0;

		@Override
		public void reduce(ImmutableBytesWritable key,
				Iterable<IntWritable> values,
				// public void reduce(Text, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int tmpcnt = 0;
			// putnum++;
			for (IntWritable val : values) {

				tmpcnt += val.get();

			}
			Put put = new Put(Bytes.toBytes(key.toString()));
			// Put put= new Put(Bytes.toBytes(putnum));
			put.add("attri".getBytes(), "val".getBytes(),
					Bytes.toBytes(Integer.toString(tmpcnt)));
			context.write(null, put);
		}
	}

	public static void main(String[] args) throws Exception {

		tabop.creTab("test_guo");

		Configuration conf = HBaseConfiguration.create();
		// conf.addResource(new Path("lib/hbase-site.xml"));

		Job job = new Job(conf, "job");
		job.setJarByClass(mrt.class);
		job.getConfiguration().setInt("mapred.map.tasks", 2000);

		Scan scan = new Scan();
		scan.setCaching(100000);
		scan.setCacheBlocks(false);
		//
		//
		// // tabOperation tabop=new tabOperation();
		// //
		// //
		// // ResultScanner rs;
		// // Result scanr;
		// //
		// // try {
		// // HTable table = new HTable(conf, "tempTest");
		// // Scan s = new Scan();
		// // s.setStartRow("temp,1269040113.0,1269040114.0".getBytes());
		// // s.setStopRow("temp,1269040302.0,1269040303.0".getBytes());
		// // rs = null;
		// // rs = table.getScanner(s);
		// // scanr = null;
		// //
		// //
		// // Result r = rs.next();
		// // String res="";
		// //
		// // while(r != null)
		// // {
		// // res = new String(r.getValue("attri".getBytes(),
		// // "vl".getBytes()));
		// // System.out.print(res+"\n");
		// // r = rs.next();
		// // }
		// //
		// //
		// // // return rs;
		// //
		// // } catch (IOException e) {
		// // System.out.println("scan initialization problem");
		// // // return null;
		// // }
		// // tabop.scanIni("tempTest", "temp,1269040113.0,1269040114.0",
		// "temp,1269040302.0,1269040303.0");
		// //
		// // String str="";
		// // while((str=tabop.scanNext("attri", "vl") )!="NO")
		// // {
		// // System.out.print(str+"\n");
		// // }
		//
		// // scan.addColumn("attri".getBytes(), "cof1".getBytes());
		//
		TableMapReduceUtil.initTableMapperJob("tempIdxmod", scan,
				mapValPointTest1.class, Text.class, IntWritable.class, job);

		TableMapReduceUtil.initTableReducerJob("tempIdxmodRes",
				ReducerEvalTest1.class, job);
		// job.setOutputFormatClass(NullOutputFormat.class);
		job.setNumReduceTasks(36);

		cnt = 0.0;

		boolean done = job.waitForCompletion(true);
		if (!done) {
			throw new IOException("error with job!");
			// }
			//
			// FileWriter fstream;
			// try {
			// fstream = new FileWriter("./conf.txt", true);
			// BufferedWriter out = new BufferedWriter(fstream);
			// String str = Double.toString(cnt);
			// out.write(str);
			// out.write("\n");
			// out.close();
			// } catch (IOException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }

			// Configuration config = HBaseConfiguration.create();
			// HTable table = new HTable(config, "tempRaw_guo");
			//
			// Scan s1 = new Scan();
			// s1.addColumn("attri".getBytes(), "val".getBytes());
			// // s1.addColumn("attri".getBytes(), "vr".getBytes());
			//
			// //....test....//
			// double val=-10;
			// System.out.printf("%f\n",val);
			//
			// //val=0.0;
			// //............//
			//
			// String str= Double.toString(val);
			// FilterList filterList = new FilterList(
			// FilterList.Operator.MUST_PASS_ALL);
			//
			// filterList.addFilter(new
			// SingleColumnValueFilter("attri".getBytes(),
			// "val"
			// .getBytes(), CompareOp.GREATER_OR_EQUAL, str.getBytes()));
			// // filterList.addFilter(new
			// SingleColumnValueFilter(cf.getBytes(),
			// // "vr"
			// // .getBytes(), CompareOp.LESS_OR_EQUAL, Bytes.toBytes(val)));
			//
			// s1.setFilter(filterList);
			// ResultScanner Res = table.getScanner(s1);
			// double cnt=0.0;
			//
			// for ( Result res: Res)
			// {
			// cnt++;
			// for (KeyValue kv: res.raw())
			// {
			// // System.out.println("Row is"+" "+Bytes.toString(kv.getRow()));
			// //
			// System.out.println("Family is"+" "+Bytes.toString(kv.getFamily())
			// );
			// //
			// System.out.println("Qualifir is "+" "+Bytes.toString(kv.getQualifier())
			// );
			// //
			// System.out.println("Value is"+" "+Bytes.toString(kv.getValue())
			// );
			// }
			//
			// }
			// Res.close();
			//
			// System.out.printf("%f\n",cnt);
			// // }

			// for (Result rr = Res.next(); rr != null; rr = Res
			// .next()) {
			// cnt++;
			// modinfor[0] = Double.parseDouble(new String(rr.getValue(
			// "model".getBytes(), "cof1".getBytes())));
			// pointEval(modinfor);
			//
			// }

		}
	}
}
