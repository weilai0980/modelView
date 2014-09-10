package basetool;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import basetool.kviMRproCol.MapperTempIntervalNoRed;
import basetool.kviMRproCol.MapperValIntervalNoRed;

public class rawMR {

	// double lb, rb;
	public static int modorder;
	public static double numMapIn;
	public static double numMapOut;

	public int scanCache; // = 200;//100000;
	public int nored; // = 16;

	int cnt = 0;

	public rawMR(int sign) {// 0: temp 1:val
		

		numMapIn = 0;
		numMapOut = 0;

		if (sign == 0)// temp
		{
			scanCache = 500;// 300;//175;//170;//170;//200;//100000;
			nored = 16;
		} else// value
		{
			scanCache = 500;// 1500;//100000;
			nored = 16;
		}
	}

	static class MapperTempRaw extends
			TableMapper<ImmutableBytesWritable, Text> {

		
	
		String str = new String();

		ImmutableBytesWritable intkey = new ImmutableBytesWritable();

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {

				str = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("val")));

				numMapIn++;
//				cntnum++;
//
//				intkey.set(Bytes.toBytes(cntnum), 0, Bytes.SIZEOF_LONG);
				//context.write(intkey, model);
		}
	}

	static class MapperValRaw extends
			TableMapper<ImmutableBytesWritable, Text> {

	
		String interv = new String();
		String str = new String();
	
		ImmutableBytesWritable intkey = new ImmutableBytesWritable();

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {

		

				str = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("temp")));

				numMapIn++;
//				cntnum++;
//
//				intkey.set(Bytes.toBytes(cntnum), 0, Bytes.SIZEOF_LONG);
//				//context.write(intkey, model);

		}
	}

	public double jobIntervalEvalQuery(String idxtabname,
			String strow, String edrow, String col) throws Exception {

		Configuration conf = HBaseConfiguration.create();

		Job job = new Job(conf, "job");
		job.setJarByClass(rawMR.class);// modification
		// job.getConfiguration().setInt("mapred.map.tasks", 2000);
	//	job.setNumReduceTasks(nored);

		Scan scan = new Scan();
		scan.setCaching(scanCache);
		scan.setCacheBlocks(false);

		scan.setStartRow(Bytes.toBytes(strow));
		scan.setStopRow(Bytes.toBytes(edrow));
		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col));

		if(col=="temp")
		{
			//scan.setCaching(250);
			TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
					MapperValRaw.class, null, null, job);
		
		}
		else
		{
			//scan.setCaching(180);
			TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
					MapperTempRaw.class, null, null, job);
			
		}
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
