package basetool;

import java.io.*;
import java.util.*;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

public class kviMRpro {
	// double lb, rb;
	public static int modorder;
	public static double numMapIn;
	public static double numMapOut;
	// public static int rednum;
	public String tabname;
	
	public int scanCache=1000000;
	public int nored=30;

	int cnt = 0;

	public kviMRpro(int modelord, String tabidx) {
		modorder = modelord;
		tabname = tabidx;
		numMapIn = 0;
		numMapOut = 0;
	}

	static class MapperInterval extends
			TableMapper<ImmutableBytesWritable, Text> {

		private long cntnum = 0;
		private Text model = new Text();

		String interval = new String();
		double bd[] = new double[3];
		String interv = new String();

		String lbstr = new String();
		String rbstr = new String();
		String modOrdstr = new String();
		
		double lb=0.0, rb=0.0;
		int modOrd=0;

		ImmutableBytesWritable intkey = new ImmutableBytesWritable();

		public String modelContract(Result values, int order, double lb,
				double rb) {
			String res = "";
			res = Double.toString(lb) + ",";
			res += Double.toString(rb) + ",";

			//String strtmp = "";
			for (int i = 0; i < order + 1; i++) {

				res = res+ Bytes.toString(values.getValue(Bytes.toBytes("model"),
						Bytes.toBytes("coef" + Integer.toString(i))));
				// tmp=Double.parseDouble(strtmp);
				// strtmp=Double.toString(tmp);

				//res = res + strtmp;
			}

			return res;
		}

		public String modelContract(Result values, int order, String bdstr) {
			String res = new String();
			res = bdstr + ",";
			// res = Double.toString(lb) + ",";
			// res += Double.toString(rb) + ",";

			//String strtmp = "";
			for (int i = 0; i < order + 1; i++) {

				res = res+ Bytes.toString(values.getValue(Bytes.toBytes("model"),
						Bytes.toBytes("coef" + Integer.toString(i))));

//				res = res + strtmp;
			}

			return res;
		}

		public void attriCfParser(String range, double interv[]) {
			int len = range.length();
			int st = 0;
			int num = 0;
			for (int i = 0; i < len; ++i) {
				if (range.charAt(i) == ',') {
					interv[num++] = Double.parseDouble(range.substring(st, i));
					st = i + 1;
				}
			}

			return;
		}

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {

			interv = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
					Bytes.toBytes("primIntev")));
			attriCfParser(interv, bd);

			try {
				Configuration conf = context.getConfiguration();
				lbstr = conf.get("lb");
				rbstr = conf.get("rb");
				modOrdstr = conf.get("modelOrd");

				 lb = Double.parseDouble(lbstr);
				 rb = Double.parseDouble(rbstr);
				 modOrd = Integer.parseInt(modOrdstr);

				// ...................//

				// model.set(modelContract(values, modOrd, bd[0], bd[1]));
				 
				numMapIn++;
				model.set(modelContract(values, modOrd, interv));

				if (lb > bd[1] || rb < bd[0]) // no overlap
				{

				} else {

					intkey.set(Bytes.toBytes(cntnum), 0, Bytes.SIZEOF_LONG);
					context.write(intkey, model);
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
	}

	static class MapperIntervalNoRed extends
			TableMapper<Text, Text> {


		String interval = new String();
		double bd[] = new double[3];
		String interv = new String();

		String lbstr = new String();
		String rbstr = new String();
		String modOrdstr = new String();
		
		double lb=0.0, rb=0.0;
		int modOrd=0;

		ImmutableBytesWritable intkey = new ImmutableBytesWritable();

		public void attriCfParser(String range, double interv[]) {
			int len = range.length();
			int st = 0;
			int num = 0;
			for (int i = 0; i < len; ++i) {
				if (range.charAt(i) == ',') {
					interv[num++] = Double.parseDouble(range.substring(st, i));
					st = i + 1;
				}
			}

			return;
		}

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {

			interv = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
					Bytes.toBytes("primIntev")));
			attriCfParser(interv, bd);

			Configuration conf = context.getConfiguration();
			lbstr = conf.get("lb");
			rbstr = conf.get("rb");
			modOrdstr = conf.get("modelOrd");

			 lb = Double.parseDouble(lbstr);
			 rb = Double.parseDouble(rbstr);
			 modOrd = Integer.parseInt(modOrdstr);

			// ...................//

			// model.set(modelContract(values, modOrd, bd[0], bd[1]));
			// numMapIn++;
		//	model.set(modelContract(values, modOrd, interv));

			if (lb > bd[1] || rb < bd[0]) // no overlap
			{

			} else {

				//intkey.set(Bytes.toBytes(cntnum), 0, Bytes.SIZEOF_LONG);
				//context.write(intkey, model);
			}
		}
	}

	public static class ReducerEval extends
			TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {

		public double cntnum = 0;
		public double[] coef = new double[10], range = new double[5];
		public double[] res = new double[100];
		public double respoint = 0.0;

		public double step = 0.0;
		public double st = 0.0, ed = 0.0;
		String reskey = new String();
		
		public double gridstep=20.0;

		public void modelParser(String modelstr, double coef[], double range[])// range[0]:
																				// left
		{

			int len = modelstr.length(), st = 0, num = 0;
			for (int i = 0; i < len; ++i) {
				if (modelstr.charAt(i) == ',' && num < 2) {
					range[num++] = Double
							.parseDouble(modelstr.substring(st, i));
					st = i + 1;
				} else if (modelstr.charAt(i) == ',' && num >= 2) {

					num++;
					st = i + 1;
					break;
				}
			}

			coef[num - 2] = Double.parseDouble(modelstr.substring(st, len));

			return;
		}

		public void modelParser2(String modelstr, double coef[], double range[])// range[0]:
		// left
		{

			int len = modelstr.length(), st = 0, num = 0;
			for (int i = 0; i < len; ++i) {
				if (modelstr.charAt(i) == ',') {
					range[num++] = Double
							.parseDouble(modelstr.substring(st, i));
					st = i + 1;
					if (modelstr.charAt(i) == ',' && num >= 4) {

						// num++;
						st = i + 1;
						break;
					}
				}
			}

			coef[0] = Double.parseDouble(modelstr.substring(st, len));

			return;
		}

		// @Override
		public void reduce(ImmutableBytesWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			for (Text val : values) {

				modelParser2(val.toString(), coef, range);

				step = ((range[1] - range[0]) / gridstep);
				st = range[0];
				ed = range[1];
				int num = 0;

				reskey = "";
				if (step > 0) {
					for (double i = st; i <= ed; i += step) {

						res[num] = coef[0];
						respoint = coef[0];

						// put = new
						// Put(Bytes.toBytes(Integer.toString((int)i)));
						// put.add(Bytes.toBytes("attri"),
						// Bytes.toBytes("results"),
						// Bytes.toBytes(Double.toString(respoint)));
						//
						// context.write(null, put);
					}
				}

			}

		}
	}

	public double jobIntervalEvalQuery(double lb, double rb, String idxtabname,
			String strow, String edrow) throws Exception {

		Configuration conf = HBaseConfiguration.create();

		conf.set("lb", Double.toString(lb));
		conf.set("rb", Double.toString(rb));
		conf.set("modelOrd", Integer.toString(modorder));

		// conf.set("reducernum", Double.toString(rednum));

		Job job = new Job(conf, "job");
		job.setJarByClass(kviMRpro.class);
		job.getConfiguration().setInt("mapred.map.tasks", 2000);
		job.setNumReduceTasks(nored);

		Scan scan = new Scan();
		scan.setCaching(scanCache);
		scan.setCacheBlocks(false);
		scan.setStartRow(strow.getBytes());
		scan.setStopRow(edrow.getBytes());
		scan.addColumn(Bytes.toBytes("attri"),
					Bytes.toBytes("primIntev"));
		scan.addFamily(Bytes.toBytes("model"));

		TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
				MapperInterval.class, ImmutableBytesWritable.class, Text.class,
				job);

		TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
				ReducerEval.class, job);

		boolean done = job.waitForCompletion(true);
		if (!done) {
			throw new IOException("error with job!");
		}

		return job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_INPUT_RECORDS").getValue();

	}

	public double jobIntervalEvalQueryNoReducer(double lb, double rb,
			String idxtabname, String strow, String edrow) throws Exception {

		Configuration conf = HBaseConfiguration.create();

		conf.set("lb", Double.toString(lb));
		conf.set("rb", Double.toString(rb));
		conf.set("modelOrd", Integer.toString(modorder));

		Job job = new Job(conf, "job");
		job.setJarByClass(kviMRpro.class);
		job.getConfiguration().setInt("mapred.map.tasks", 2000);
		job.setNumReduceTasks(nored);

		Scan scan = new Scan();
		scan.setCaching(scanCache);
		scan.setCacheBlocks(false);
		scan.setStartRow(strow.getBytes());
		scan.setStopRow(edrow.getBytes());
		scan.addColumn(Bytes.toBytes("attri"),
				Bytes.toBytes("primIntev"));

		TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
				MapperIntervalNoRed.class, null, null, job);

		job.setOutputFormatClass(NullOutputFormat.class);

		boolean done = job.waitForCompletion(true);
		if (!done) {
			throw new IOException("error with job!");
		}

		return job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_INPUT_RECORDS").getValue();

	}
}
