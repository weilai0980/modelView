package basetool;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;

import basetool.kviMRenhance.MapperComposInterval;
//import basetool.kviMRenhance.ReducerEval;
import basetool.kviMRenhance.RegIndiviTableInputFormat;

public class MRScan {

	public static int modorder;
	public int rednum;
	public static double qualNum;
	public static double gTabNum;

	// for temp
	// public int scanCacheT=128;//128;//125; //300
	public int noreducerT = 16;

	public int scanCacheT = 50;

	// for value
	public int scanCacheV = 111; // 300
	public int noreducerV = 16;// 15;

	public MRScan(int modelord) {// 0: temp 1: val
		modorder = modelord;
		rednum = 5;

		qualNum = 0;
		gTabNum = 0;

		// if(sign==0)
		// {
		// scanCache=129; //300
		// noreducer=16;
		// }
		// else
		// {
		// scanCache=111; //300
		// noreducer=15;
		// }
	}

	public static class mapSpaceStat extends TableMapper<Text, IntWritable> {

		private Text model = new Text();

		public String rowkeyParser(String rowkey) {
			int len = rowkey.length(), i = 0, tmp = 0;
			for (i = 0; i < len; ++i) {
				if (rowkey.charAt(i) == ',') {
					tmp++;
					if (tmp == 2) {
						break;
					}
				}
			}

			return rowkey.substring(0, i);
		}

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {

			try {

				Text model = new Text();
				IntWritable ONE = new IntWritable(1);
				String nval = rowkeyParser(Bytes.toString((row.get())));
				model.set(nval);

				context.write(model, ONE);
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
	}

	public static class ReducerSpaceStat extends
			TableReducer<Text, IntWritable, ImmutableBytesWritable> {

		public static final byte[] CF = "attri".getBytes();
		public static final byte[] COUNT = "count".getBytes();

		// @Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int i = 0;
			for (IntWritable val : values) {
				i += val.get();
			}
			Put put = new Put(Bytes.toBytes(key.toString()));

			// String tmp=i.toString();
			put.add(CF, COUNT, Bytes.toBytes(Integer.toString(i)));

			context.write(null, put);
		}
	}

	public double jobSpaceStat(String idxtabname) throws Exception {

		Configuration conf = HBaseConfiguration.create();

		Job job = new Job(conf, "job");
		job.setJarByClass(MRScan.class);

		job.setNumReduceTasks(noreducerT);

		Scan scan = new Scan();
		scan.setCaching(scanCacheT);
		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
				mapSpaceStat.class, Text.class, IntWritable.class, job);

		TableMapReduceUtil.initTableReducerJob(idxtabname + "Dis-guo",
				ReducerSpaceStat.class, job);
		// System.exit(job.waitForCompletion(true) ? 0 : 1);

		boolean done = job.waitForCompletion(true);
		if (!done) {
			throw new IOException("error with job!");
		}
		return job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_OUTPUT_RECORDS").getValue();

	}

	public static class mapTempRange extends
			TableMapper<ImmutableBytesWritable, Text> {

		public double cntn = 0;
		private Text model = new Text();
		String modinfor = new String();

		double[] tmpbd = new double[3];
		ImmutableBytesWritable intkey = new ImmutableBytesWritable();
		double lb = 0.0, rb = 0.0;
		int modOrd = 0;

		String lbstr = new String();
		String rbstr = new String();
		String modOrdstr = new String();

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

		public String modelContract(int order, String lb, String rb,
				String modinfor) {
			String res = "";
			res = lb + ",";
			res += rb + ",";

			return res + modinfor;
		}

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {

			modinfor = Bytes.toString(values.getValue(Bytes.toBytes("model"),
					Bytes.toBytes("coef0")));

			// rowkeyParser(Bytes.toString((row.get())), tmpbd);

			// try {
			Configuration conf = context.getConfiguration();
			lbstr = conf.get("lb");
			rbstr = conf.get("rb");
			modOrdstr = conf.get("modelOrd");

			lb = Double.parseDouble(lbstr);
			rb = Double.parseDouble(rbstr);
			modOrd = Integer.parseInt(modOrdstr);

			lbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
					Bytes.toBytes("st")));
			rbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
					Bytes.toBytes("ed")));

			tmpbd[0] = Double.parseDouble(lbstr);
			tmpbd[1] = Double.parseDouble(rbstr);

			model.set(modelContract(modOrd, lbstr, rbstr, modinfor));
			gTabNum++;

			if (lb > tmpbd[1] || rb < tmpbd[0]) // no overlap
			{

			} else {
				qualNum++;
				cntn++;

				intkey.set(Bytes.toBytes(cntn));

				try {
					context.write(intkey, model);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			// } catch (InterruptedException e) {
			// throw new IOException(e);
			// }
		}
	}

	public static class mapTempPoint extends
			TableMapper<ImmutableBytesWritable, Text> {

		private long cntn = 0;
		private Text model = new Text();
		String modinfor = new String();
		double[] tmpbd = new double[3];

		// StringBuilder tmpstr = new StringBuilder();
		ImmutableBytesWritable intkey = new ImmutableBytesWritable();
		double val = 0.0;
		int modOrd = 0;

		String valstr = "", modOrdstr = "";
		String lbstr = "", rbstr = "";

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

		public String modelContract(int order, String lb, String rb,
				String modinfor) {
			String res = "";
			res = lb + ",";
			res += rb + ",";

			return res + modinfor;
		}

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {

			modinfor = Bytes.toString(values.getValue(Bytes.toBytes("model"),
					Bytes.toBytes("coef0")));
			// tmpstr=new String(row.gt());
			// rowkeyParser(Bytes.toString(row.get()), tmpbd);
			// double stv = tmpbd[0], edv = tmpbd[1];

			try {
				Configuration conf = context.getConfiguration();
				valstr = conf.get("val");
				modOrdstr = conf.get("modelOrd");

				val = Double.parseDouble(valstr);
				modOrd = Integer.parseInt(modOrdstr);

				lbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("st")));
				rbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("ed")));

				tmpbd[0] = Double.parseDouble(lbstr);
				tmpbd[1] = Double.parseDouble(rbstr);

				model.set(modelContract(modOrd, lbstr, rbstr, modinfor));

				if (val >= tmpbd[0] && val <= tmpbd[1]) {

					qualNum++;
					cntn++;
					intkey.set(Bytes.toBytes(cntn), 0, Bytes.SIZEOF_LONG);
					context.write(intkey, model);
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
	}

	public static class mapValRange extends
			TableMapper<ImmutableBytesWritable, Text> {

		// private static final IntWritable one = new IntWritable(1);
		// private double cntnum = 0.0;
		private long cntn = 0;
		private Text model = new Text();

		double lb = 0.0, rb = 0.0;
		String modinfor = new String();
		int modOrd = 0;
		double vlv = 0.0, vrv = 0.0;

		String lbstr = new String();
		String rbstr = new String();
		String modOrdstr = new String();
		ImmutableBytesWritable intkey = new ImmutableBytesWritable();

		public String modelContract(int order, String lb, String rb,
				String modinfor) {
			String res = "";
			res = lb + ",";
			res += rb + ",";
			return res + modinfor;
		}

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {

			modinfor = Bytes.toString(values.getValue(Bytes.toBytes("model"),
					Bytes.toBytes("coef0")));

			try {
				Configuration conf = context.getConfiguration();
				lbstr = conf.get("lb");
				rbstr = conf.get("rb");
				modOrdstr = conf.get("modelOrd");

				lb = Double.parseDouble(lbstr);
				rb = Double.parseDouble(rbstr);
				modOrd = Integer.parseInt(modOrdstr);

				lbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("vl")));
				rbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("vr")));
				vlv = Double.parseDouble(lbstr);
				vrv = Double.parseDouble(rbstr);

				model.set(modelContract(modOrd, lbstr, rbstr, modinfor));

				if (lb > vrv || rb < vlv) // no overlap
				{

				} else {

					qualNum++;
					cntn++;
					intkey.set(Bytes.toBytes(cntn), 0, Bytes.SIZEOF_LONG);
					context.write(intkey, model);
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
	}

	public static class mapValPoint extends
			TableMapper<ImmutableBytesWritable, Text> {

		private long cntn = 0;
		private Text model = new Text();

		String modinfor = new String();
		double vlv = 0.0, vrv = 0.0;
		double val = 0.0;
		int modOrd = 0;

		String lbstr = new String();
		String rbstr = new String();
		String valstr = new String();
		String modOrdstr = new String();
		ImmutableBytesWritable intkey = new ImmutableBytesWritable();

		public String modelContract(int order, String lb, String rb,
				String modinfor) {
			String res = "";
			res = lb + ",";
			res += rb + ",";
			return res + modinfor;
		}

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {

			modinfor = Bytes.toString(values.getValue(Bytes.toBytes("model"),
					Bytes.toBytes("coef0")));

			try {
				Configuration conf = context.getConfiguration();
				valstr = conf.get("val");
				modOrdstr = conf.get("modelOrd");

				val = Double.parseDouble(valstr);
				modOrd = Integer.parseInt(modOrdstr);

				lbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("vl")));
				rbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("vr")));
				vlv = Double.parseDouble(lbstr);
				vrv = Double.parseDouble(rbstr);

				model.set(modelContract(modOrd, lbstr, rbstr, modinfor));
				if (val >= vlv && val <= vrv) {
					qualNum++;
					cntn++;
					intkey.set(Bytes.toBytes(cntn), 0, Bytes.SIZEOF_LONG);
					context.write(intkey, model);
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
	}

	static class mapComposInterval extends
			TableMapper<ImmutableBytesWritable, Text> {

		public static long cntnum = 0;
		public Text model = new Text();

		String interval = new String();
		double bd[] = new double[3];
		String interv = new String();

		String tlbstr = new String();
		String trbstr = new String();
		String vlbstr = new String();
		String vrbstr = new String();
		String modOrdstr = new String();

		double tlb = 0.0, trb = 0.0, vlb = 0.0, vrb = 0.0;
		int modOrd = 0;

		ImmutableBytesWritable intkey = new ImmutableBytesWritable();

		public String modelContract(Result values, int order, double lb,
				double rb) {
			String res = "";
			res = Double.toString(lb) + ",";
			res += Double.toString(rb) + ",";

			// String strtmp = "";
			for (int i = 0; i < order + 1; i++) {

				res = res
						+ Bytes.toString(values.getValue(
								Bytes.toBytes("model"),
								Bytes.toBytes("coef" + Integer.toString(i))));
				// tmp=Double.parseDouble(strtmp);
				// strtmp=Double.toString(tmp);

				// res = res + strtmp;
			}

			return res;
		}

		public String modelContract(Result values, int order, String bdstr) {
			String res = new String();
			res = bdstr + ",";
			// res = Double.toString(lb) + ",";
			// res += Double.toString(rb) + ",";

			// String strtmp = "";
			for (int i = 0; i < order + 1; i++) {

				res = res
						+ Bytes.toString(values.getValue(
								Bytes.toBytes("model"),
								Bytes.toBytes("coef" + Integer.toString(i))));

				// res = res + strtmp;
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

			try {
				Configuration conf = context.getConfiguration();
				tlbstr = conf.get("tlb");
				trbstr = conf.get("trb");

				vlbstr = conf.get("vlb");
				vrbstr = conf.get("vrb");

				modOrdstr = conf.get("modelOrd");

				tlb = Double.parseDouble(tlbstr);
				trb = Double.parseDouble(trbstr);
				vlb = Double.parseDouble(vlbstr);
				vrb = Double.parseDouble(vrbstr);
				modOrd = Integer.parseInt(modOrdstr);

				tlbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("st")));
				trbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("ed")));

				vlbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("vl")));
				vrbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("vr")));

				bd[0] = Double.parseDouble(tlbstr);
				bd[1] = Double.parseDouble(trbstr);

				bd[2] = Double.parseDouble(vlbstr);
				bd[3] = Double.parseDouble(vrbstr);

				// ...................//

				model.set(modelContract(values, modOrd, bd[0], bd[1]));
//				numMapIn++;
				cntnum++;
				// model.set(modelContract(values, modOrd, interv));

				if (tlb > bd[1] || trb < bd[0]) // no overlap
				{

				} else {

					if (vlb > bd[1] || vrb < bd[0]) {

					} else {
						intkey.set(Bytes.toBytes(cntnum), 0, Bytes.SIZEOF_LONG);
						context.write(intkey, model);
					}
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
	}

	public static class ReducerEval extends
			TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {
		// TableReducer<ImmutableBytesWritable, Iterable<Text>,
		// ImmutableBytesWritable> {

		public double cntnum = 0;
		public double[] coef = new double[2], range = new double[3];
		public double respoint = 0.0;
		public double step = 0.0;
		public double st = 0.0, ed = 0.0;
		public double gridstep = 30.0;

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
					// coef[(num-2)] = Double.parseDouble(modelstr.substring(st,
					// i));
					num++;
					st = i + 1;
					break;
				}
			}
			coef[num - 2] = Double.parseDouble(modelstr.substring(st, len));

			return;
		}

		// @Override
		public void reduce(ImmutableBytesWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			// int sum = 0;

			for (Text val : values) {

				modelParser(val.toString(), coef, range);

				step = ((range[1] - range[0]) / gridstep);
				st = range[0];
				ed = range[1];

				if (step > 0) {
					for (double i = st; i <= ed; i += step) {

						respoint = coef[0];

						// put = new Put(Bytes.toBytes(Integer.toString((int)
						// i)));
						// put.add(Bytes.toBytes("attri"),
						// Bytes.toBytes("results"),
						// Bytes.toBytes(Double.toString(respoint)));
						//
						// context.write(null, put);
					}
				} else {

					// put = new Put(Bytes.toBytes(Integer
					// .toString((int) range[0])));
					// put.add(Bytes.toBytes("attri"), Bytes.toBytes("results"),
					// Bytes.toBytes(Double.toString(coef[0])));
					//
					// context.write(null, put);

				}

			}
		}
	}

	public double jobTempPoint(double val, String idxtabname) throws Exception {

		Configuration conf = HBaseConfiguration.create();

		conf.set("val", Double.toString(val));

		conf.set("modelOrd", Integer.toString(modorder));
		Job job = new Job(conf, "job");
		job.setJarByClass(MRScan.class);

		// scanCache=129; //300
		// noreducer=16;

		qualNum = 0;
		// job.getConfiguration().setInt("mapred.map.tasks", 2000);
		job.setNumReduceTasks(noreducerT);

		Scan scan = new Scan();
		scan.setCaching(scanCacheT);
		scan.setCacheBlocks(false);
		scan.addFamily(Bytes.toBytes("model"));
		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes("st"));
		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes("ed"));

		TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
				mapTempPoint.class, ImmutableBytesWritable.class, Text.class,
				job);

		TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
				ReducerEval.class, job);
		// System.exit(job.waitForCompletion(true) ? 0 : 1);

		boolean done = job.waitForCompletion(true);
		if (!done) {
			throw new IOException("error with job!");
		}
		return job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_OUTPUT_RECORDS").getValue();

	}

	public double jobTempRange(double lb, double rb, String idxtabname)
			throws Exception {

		Configuration conf = HBaseConfiguration.create();

		conf.set("lb", Double.toString(lb));
		conf.set("rb", Double.toString(rb));
		conf.set("modelOrd", Integer.toString(modorder));
		Job job = new Job(conf, "job");
		job.setJarByClass(MRScan.class);

		// scanCache=129; //300
		// noreducer=16;

		// qualNum = 0;

		// job.getConfiguration().setInt("mapred.map.tasks", 2000);
		job.setNumReduceTasks(noreducerT);

		Scan scan = new Scan();
		scan.setCaching(scanCacheT);
		scan.setCacheBlocks(false);
		scan.addFamily(Bytes.toBytes("model"));
		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes("st"));
		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes("ed"));

		TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
				mapTempRange.class, ImmutableBytesWritable.class, Text.class,
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
						"MAP_OUTPUT_RECORDS").getValue();

	}

	public double jobValPoint(double val, String idxtabname) throws Exception {

		Configuration conf = HBaseConfiguration.create();

		conf.set("val", Double.toString(val));

		conf.set("modelOrd", Integer.toString(modorder));
		// conf.set("reducernum", Double.toString(rednum));

		// scanCache=111; //300
		// noreducer=15;

		Job job = new Job(conf, "job");
		job.setJarByClass(MRScan.class);

		qualNum = 0;

		job.getConfiguration().setInt("mapred.map.tasks", 2000);
		job.setNumReduceTasks(noreducerV);

		Scan scan = new Scan();
		scan.setCaching(scanCacheV);
		scan.setCacheBlocks(false);
		scan.addFamily(Bytes.toBytes("model"));
		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes("vl"));
		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes("vr"));

		TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
				mapValPoint.class, ImmutableBytesWritable.class, Text.class,
				job);

		TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
				ReducerEval.class, job);
		// System.exit(job.waitForCompletion(true) ? 0 : 1);

		boolean done = job.waitForCompletion(true);
		if (!done) {
			throw new IOException("error with job!");
		}
		return job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_OUTPUT_RECORDS").getValue();

	}

	public double jobValRange(double lb, double rb, String idxtabname)
			throws Exception {

		Configuration conf = HBaseConfiguration.create();

		conf.set("lb", Double.toString(lb));
		conf.set("rb", Double.toString(rb));
		conf.set("modelOrd", Integer.toString(modorder));
		// conf.set("reducernum", Double.toString(rednum));
		//
		// scanCache=111; //300
		// noreducer=15;

		Job job = new Job(conf, "job");
		job.setJarByClass(MRScan.class);

		qualNum = 0;

		job.getConfiguration().setInt("mapred.map.tasks", 2000);
		job.setNumReduceTasks(noreducerV);

		Scan scan = new Scan();
		scan.setCaching(scanCacheV);
		scan.setCacheBlocks(false);
		scan.addFamily(Bytes.toBytes("model"));
		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes("vl"));
		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes("vr"));

		TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
				mapValRange.class, ImmutableBytesWritable.class, Text.class,
				job);

		TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
				ReducerEval.class, job);
		// System.exit(job.waitForCompletion(true) ? 0 : 1);

		boolean done = job.waitForCompletion(true);
		if (!done) {
			throw new IOException("error with job!");
		}
		return job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_OUTPUT_RECORDS").getValue();
	}
	public double jobCompRange(double tlb, double trb,
			double vlb, double vrb, String idxtabname) throws Exception {

		long st = 0, ed = 0;

		Configuration conf = HBaseConfiguration.create();

		conf.set("tlb", Double.toString(tlb));
		conf.set("trb", Double.toString(trb));
		conf.set("vlb", Double.toString(vlb));
		conf.set("vrb", Double.toString(vrb));

		conf.set("modelOrd", Integer.toString(modorder));

		Job job = new Job(conf, "job");
		job.setJarByClass(kviMRproCol.class);
		job.setNumReduceTasks(noreducerV);

		Scan scan = new Scan();
		scan.setCaching(100);
		scan.setCacheBlocks(false);

//		scan.setStartRow(Bytes.toBytes(strow));
//		scan.setStopRow(Bytes.toBytes(edrow));
//		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[0]));
//		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[1]));
		scan.addFamily(Bytes.toBytes("attri"));
		scan.addFamily(Bytes.toBytes("model"));

		job.setInputFormatClass(RegIndiviTableInputFormat.class);

//		if (col[0] == "st") {
//			TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
//					MapperComposInterval.class, ImmutableBytesWritable.class,
//					Text.class, job, true, RegIndiviTableInputFormat.class);
//
//			TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
//					ReducerEval.class, job);
//
//		} else {
//			TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
//					MapperComposInterval.class, ImmutableBytesWritable.class,
//					Text.class, job, true, RegIndiviTableInputFormat.class);
//
//			TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
//					ReducerEval.class, job);
//		}

		TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
				mapComposInterval.class, ImmutableBytesWritable.class, Text.class,
				job);

		TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
				ReducerEval.class, job);
		
		
		boolean done = job.waitForCompletion(true);
		if (!done) {
			throw new IOException("error with job!");
		}

//		runpara[0] = job
//				.getCounters()
//				.findCounter("org.apache.hadoop.mapred.Task$Counter",
//						"MAP_OUTPUT_RECORDS").getValue();
//		runpara[1] = job
//				.getCounters()
//				.findCounter("org.apache.hadoop.mapred.JobInProgress$Counter",
//						"TOTAL_LAUNCHED_MAPS").getValue();
//
//		return job
//				.getCounters()
//				.findCounter("org.apache.hadoop.mapred.Task$Counter",
//						"MAP_INPUT_RECORDS").getValue();
		
		return job
		.getCounters()
		.findCounter("org.apache.hadoop.mapred.Task$Counter",
				"MAP_OUTPUT_RECORDS").getValue();

	}


}
