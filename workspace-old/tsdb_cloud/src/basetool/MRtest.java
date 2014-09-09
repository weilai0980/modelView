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


public class MRtest {

	public static int modorder;
	public int rednum;
	public static double qualNum;
	public static double gTabNum;

	// /public String tabname;

	public MRtest(int modelord) {
		modorder = modelord;
		rednum = 5;
		
		qualNum=0;
		gTabNum=0;
	}


	public static class mapValPointTest extends TableMapper<ImmutableBytesWritable, Text> {

		private long cntnum = 0;
		private Text model = new Text();

		public void rowkeyParser(String rowkey, double bound[]) // 0 left, 1
																// right
		{
			int len = rowkey.length(), seg1 = 0, seg2 = 0, seg3 = 0;
			for (int i = 0; i < len; ++i) {
				if (rowkey.charAt(i) == ',') {
					if (seg1 == 0) {
						seg1 = i;
					} else if (seg2 == 0) {
						seg2 = i;

					} else if (seg3 == 0) {
						seg3 = i;
						break;
					}
				}
			}
			bound[0] = Double.parseDouble(rowkey.substring(seg1, seg2));
			bound[1] = Double.parseDouble(rowkey.substring(seg2, seg3));
			return;
		}

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

			String vl = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
					Bytes.toBytes("vl")));
			String vr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
					Bytes.toBytes("vr")));
			String modinfor = Bytes.toString(values.getValue(
					Bytes.toBytes("model"), Bytes.toBytes("cof1")));
			double vlv = Double.parseDouble(vl), vrv = Double.parseDouble(vr);

			try {
				Configuration conf = context.getConfiguration();
				String valstr = conf.get("val");
				String modOrdstr = conf.get("modelOrd");

				double val = Double.parseDouble(valstr);
				int modOrd = Integer.parseInt(modOrdstr);

				model.set(modelContract(modOrd, vl, vr, modinfor));
				if (val >= vlv && val <= vrv) {
					qualNum++;
					cntnum++;
					ImmutableBytesWritable intkey = new ImmutableBytesWritable(
							Bytes.toBytes(cntnum), 0, Bytes.SIZEOF_INT);
					context.write(intkey, model);
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
	}

	public static class ReducerEvalTest extends
			TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {
		// TableReducer<ImmutableBytesWritable, Iterable<Text>,
		// ImmutableBytesWritable> {

		public double cntnum = 0;

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
		
			double[] coef = new double[10], range = new double[5];
			double respoint = 0.0;

			double step = 0.0;
			double st = 0.0, ed = 0.0;
			Put put;
			for (Text val : values) {

				modelParser(val.toString(), coef, range);

				step = ((range[1] - range[0]) / 10.0);
				st = range[0];
				ed = range[1];

				if (step > 0) {
					for (double i = st; i <= ed; i += step) {

						respoint = coef[0];	
					}
				} else {		

				}

			}
		}
	}

	public void jobValPoint(double val, String idxtabname) throws Exception {

		//Configuration conf = HBaseConfiguration.create();
		HBaseConfiguration conf = new HBaseConfiguration();

		conf.set("val", Double.toString(val));

		conf.set("modelOrd", Integer.toString(modorder));
		// conf.set("reducernum", Double.toString(rednum));

		Job job = new Job(conf, "testjob");
		Job a= new Job();
		job.setJarByClass(MRtest.class);

		qualNum=0;

		Scan scan = new Scan();

		TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
				mapValPointTest.class, ImmutableBytesWritable.class, Text.class,
				job);

		TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
				ReducerEvalTest.class, job);
		// System.exit(job.waitForCompletion(true) ? 0 : 1);

		boolean done = job.waitForCompletion(true);
		if (!done) {
			throw new IOException("error with job!");
		}

	}

	
}
