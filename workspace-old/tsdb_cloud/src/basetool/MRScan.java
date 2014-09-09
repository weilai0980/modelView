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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;

import basetool.kviMRpro.MapperInterval;
import basetool.kviMRpro.ReducerEval;

public class MRScan {

	public static int modorder;
	public int rednum;
	public static double qualNum;
	public static double gTabNum;
	public int scanCache=1000;
	public int noreducer=24;


	public MRScan(int modelord) {
		modorder = modelord;
		rednum = 5;

		qualNum = 0;
		gTabNum = 0;
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

//		public void rowkeyParser(String rowkey, double bound[]) // 0 left, 1
//																// right
//		{
//			int len = rowkey.length(), seg1 = 0, seg2 = 0, seg3 = 0;
//			for (int i = 0; i < len; ++i) {
//				if (rowkey.charAt(i) == ',') {
//					if (seg1 == 0) {
//						seg1 = i + 1;
//					} else if (seg2 == 0) {
//						seg2 = i + 1;
//						bound[0] = Double
//								.parseDouble(rowkey.substring(seg1, i));
//						bound[1] = Double.parseDouble(rowkey.substring(seg2,
//								len));
//						break;
//					}
//				}
//			}
//			return;
//		}

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

			//rowkeyParser(Bytes.toString((row.get())), tmpbd);

			try {
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
				
				tmpbd[0]=Double.parseDouble(lbstr);
				tmpbd[1]=Double.parseDouble(rbstr);

				model.set(modelContract(modOrd, lbstr,
						rbstr, modinfor));
				gTabNum++;

				if (lb > tmpbd[1] || rb < tmpbd[0]) // no overlap
				{

				} else {
					qualNum++;
					cntn++;

					intkey.set(Bytes.toBytes(cntn));

					context.write(intkey, model);
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
	}

	public static class mapTempPoint extends
			TableMapper<ImmutableBytesWritable, Text> {

		private long cntn = 0;
		private Text model = new Text();
		String modinfor = new String();
		double[] tmpbd = new double[3];

	//	StringBuilder tmpstr = new StringBuilder();
		ImmutableBytesWritable intkey = new ImmutableBytesWritable();
		double val = 0.0;
		int modOrd = 0;

		String valstr ="", modOrdstr ="";
	    String lbstr="",rbstr="";

//		public void rowkeyParser(String rowkey, double bound[]) // 0 left, 1
//		// right
//		{
//			int len = rowkey.length(), seg1 = 0, seg2 = 0, seg3 = 0;
//			for (int i = 0; i < len; ++i) {
//				if (rowkey.charAt(i) == ',') {
//					if (seg1 == 0) {
//						seg1 = i + 1;
//					} else if (seg2 == 0) {
//						seg2 = i + 1;
//						bound[0] = Double
//								.parseDouble(rowkey.substring(seg1, i));
//						bound[1] = Double.parseDouble(rowkey.substring(seg2,
//								len));
//						break;
//					}
//				}
//			}
//			return;
//		}

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
			//rowkeyParser(Bytes.toString(row.get()), tmpbd);
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
				
				tmpbd[0]=Double.parseDouble(lbstr);
				tmpbd[1]=Double.parseDouble(rbstr);

				model.set(modelContract(modOrd, lbstr,
						rbstr, modinfor));

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

	    double lb=0.0,rb=0.0;
		String modinfor = new String();
		int modOrd=0; 
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
		double val=0.0;
		int modOrd=0;

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

	public static class ReducerEval extends
			TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {
		// TableReducer<ImmutableBytesWritable, Iterable<Text>,
		// ImmutableBytesWritable> {

		public double cntnum = 0;
		public double[] coef = new double[2], range = new double[3];
		public double respoint = 0.0;
		public double step = 0.0;
		public double st = 0.0, ed = 0.0;
		public double gridstep=30.0;

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

		qualNum = 0;
		job.getConfiguration().setInt("mapred.map.tasks", 2000);
		job.setNumReduceTasks(noreducer);

		Scan scan = new Scan();
		scan.setCaching(scanCache);
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

		qualNum = 0;

		job.getConfiguration().setInt("mapred.map.tasks", 2000);
		job.setNumReduceTasks(noreducer);


		Scan scan = new Scan();
		scan.setCaching(scanCache);
		scan.setCacheBlocks(false);
		scan.addFamily(Bytes.toBytes("model"));
		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes("st"));
		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes("ed"));
		
		TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
				mapTempRange.class, ImmutableBytesWritable.class, Text.class,
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

	public double jobValPoint(double val, String idxtabname) throws Exception {

		Configuration conf = HBaseConfiguration.create();

		conf.set("val", Double.toString(val));

		conf.set("modelOrd", Integer.toString(modorder));
		// conf.set("reducernum", Double.toString(rednum));

		Job job = new Job(conf, "job");
		job.setJarByClass(MRScan.class);

		qualNum = 0;

		job.getConfiguration().setInt("mapred.map.tasks", 2000);
		job.setNumReduceTasks(noreducer);


		Scan scan = new Scan();
		scan.setCaching(scanCache);
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

		Job job = new Job(conf, "job");
		job.setJarByClass(MRScan.class);

		qualNum = 0;

		job.getConfiguration().setInt("mapred.map.tasks", 2000);
		job.setNumReduceTasks(noreducer);

		Scan scan = new Scan();
		scan.setCaching(scanCache);
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

}