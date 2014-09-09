//package basetool;
//
//public class kviMRenhance {
//
//}

package basetool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

@SuppressWarnings("unused")
public class kviMRenhance {

	// double lb, rb;
	public static int modorder;
	public static double numMapIn;
	public static double numMapOut;
	// public static int rednum;
	public String tabname;

	// for temp
	public int scanCache; // = 200;//100000;
	public int nored; // = 16;

//	long[] relreg = new long[100];
//	int relregnum = 0;
	int[] relregFlag = new int[100];
	public static String prekey = "abcdefghijklmnopqrstuvwxyz";

	// for val
	// public int scanCache = 150;//1500;//100000;
	// public int nored = 16;

	int cnt = 0;

	
	public kviMRenhance(int modelord, String tabidx, int sign) {// 0: temp 1:val
		modorder = modelord;
		tabname = tabidx;
		numMapIn = 0;
		numMapOut = 0;

//		relregnum = 0;

		for (int i = 0; i < 100; ++i)
			relregFlag[i] = 0;

		if (sign == 0)// temp
		{
			// scanCache = 350;//300;//175;//170;//170;//200;//100000;

			// scanCache = 650;
			scanCache = 350;
			nored = 16;
		} else// value
		{
			// scanCache = 155;// 1500;//100000;
			scanCache = 3;
			nored = 16;
		}
	}

	
	public static String regvalKeyCon(long regval) {
		long tmp = regval;
		int num = 0;
		while (tmp != 0) {
			tmp = tmp / 10;
			num++;
		}
		if (num == 0)
			num = 1;
		return prekey.charAt(num - 1) + "," + Long.toString(regval);

	}
	public void getRelReg(long regid[], int num) {

		// for(int i=0;i<100;++i)
		// relregFlag[i]=0;

		for (int i = 0; i < num; ++i) {
			// relregFlag[(int)regid[i]]=1;

//			relreg[i] = regid[i];

		}
//		relregnum = num;
		return;
	}

	public static class RegTableInputFormat extends TableInputFormat {
		// the whole region with relevant node is for one split

		// final Log LOG = LogFactory.getLog(TableInputFormatBase.class);

		public static int staRelregnum = 0;
		public static long[] staRelreg = new long[200];

		public static void getRegion(int num, long regid[]) {

			// System.out.printf("regions of interest: ");

			for (int i = 0; i < num; ++i) {
				staRelreg[i] = regid[i];
				// System.out.printf(" %d,",regid[i]);
			}
			staRelregnum = num;

			// System.out.printf("\n");
			return;
		}

		@Override
		public List<InputSplit> getSplits(JobContext context)
				throws IOException {

			int j = 0;
//			int tmpcnt = 0;
			long[] selReg = new long[200];
			int selRegCnt = 0;

			HTable table = this.getHTable(); // maybe super.getHTable
			Scan scan = this.getScan(); // maybe super.getHTable

			if (table == null) {
				throw new IOException("No table was provided.");
			}
			Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
			if (keys == null || keys.getFirst() == null
					|| keys.getFirst().length == 0) {
				throw new IOException("Expecting at least one region.");
			}
			// int count = 0;

			List<InputSplit> splits = new ArrayList<InputSplit>(
					keys.getFirst().length);

			String regionLocation;
			HRegionInfo reginfor;
			long tmpid = 0;

			for (int i = 0; i < keys.getFirst().length; i++) {
				if (!includeRegionInSplit(keys.getFirst()[i],
						keys.getSecond()[i])) {
					continue;
				}

				regionLocation = table.getRegionLocation(keys.getFirst()[i])
						.getHostname();

				reginfor = table.getRegionLocation(keys.getFirst()[i])
						.getRegionInfo();

				tmpid = reginfor.getRegionId();

				byte[] startRow = scan.getStartRow();
				byte[] stopRow = scan.getStopRow();
				// determine if the given start an stop key fall into the region
				if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes
						.compareTo(startRow, keys.getSecond()[i]) < 0)
						&& (stopRow.length == 0 || Bytes.compareTo(stopRow,
								keys.getFirst()[i]) > 0)) {
					byte[] splitStart = startRow.length == 0
							|| Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ? keys
							.getFirst()[i] : startRow;
					byte[] splitStop = (stopRow.length == 0 || Bytes.compareTo(
							keys.getSecond()[i], stopRow) <= 0)
							&& keys.getSecond()[i].length > 0 ? keys
							.getSecond()[i] : stopRow;
					InputSplit split = new TableSplit(table.getTableName(),
							splitStart, splitStop, regionLocation);
					splits.add(split);
					// if (LOG.isDebugEnabled())
					// LOG.debug("getSplits: split -> " + (count++) + " -> " +
					// split);

					selReg[selRegCnt++] = tmpid;

				} else {
					// //eliminate current split
					for (j = 0; j < staRelregnum; ++j) {
						if (staRelreg[j] == tmpid) {
							break;
						}

					}
					if (j == staRelregnum)
						continue;
					else {
						int k = 0;
						for (k = 0; k < selRegCnt; ++k) {
							if (selReg[k] == tmpid) {
								break;
							}
						}
						if (k == selRegCnt) {
							selReg[selRegCnt++] = tmpid;

							InputSplit split = new TableSplit(
									table.getTableName(), keys.getFirst()[i],
									keys.getSecond()[i], regionLocation);
							splits.add(split);

						} else {
							continue;
						}
					}
				}
			}

			// ........test...................//
			System.out.printf("the number of splits: %d\n", splits.size());

			// ...............................//

			return splits;

			// List<InputSplit> splits = new ArrayList<InputSplit>();
			// Scan scan = getScan();
			// byte startRow[] = scan.getStartRow(), stopRow[] =
			// scan.getStopRow();
			// byte prefixedStartRow[] = new byte[startRow.length + 1];
			// byte prefixedStopRow[] = new byte[stopRow.length + 1];
			// System.arraycopy(startRow, 0, prefixedStartRow, 1,
			// startRow.length);
			// System.arraycopy(stopRow, 0, prefixedStopRow, 1, stopRow.length);
			//
			// for (int prefix = -128; prefix < 128; prefix++) {
			// prefixedStartRow[0] = (byte) prefix;
			// prefixedStopRow[0] = (byte) prefix;
			// scan.setStartRow(prefixedStartRow);
			// scan.setStopRow(prefixedStopRow);
			// setScan(scan);
			// splits.addAll(super.getSplits(context));
			// }

			// return splits;
		}
	}

	public static class RegScanTableInputFormat extends TableInputFormat {
		// the whole region with relevant node is for one split

		// final Log LOG = LogFactory.getLog(TableInputFormatBase.class);

		public static int staScanRelregnum = 0;
		public static long[] staScanRelreg = new long[200];

		// public static int staScanPerReg = 0;
		public static String[][] staScanPerReg = new String[200][3];

		public static void getRegion(int num, long regid[],
				String scanPerReg[][]) {

			// System.out.printf("regions of interest: ");

			for (int i = 0; i < num; ++i) {
				staScanRelreg[i] = regid[i];
				staScanPerReg[i][0] = scanPerReg[i][0];
				staScanPerReg[i][1] = scanPerReg[i][1];
				// System.out.printf(" %d,",regid[i]);
			}
			staScanRelregnum = num;

			// System.out.printf("\n");
			return;
		}

		@Override
		public List<InputSplit> getSplits(JobContext context)
				throws IOException {

			int j = 0;
			int tmpcnt = 0;
			long[] selReg = new long[200];
			int selRegCnt = 0;

			HTable table = this.getHTable(); // maybe super.getHTable
			Scan scan = this.getScan(); // maybe super.getHTable

			if (table == null) {
				throw new IOException("No table was provided.");
			}
			Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
			if (keys == null || keys.getFirst() == null
					|| keys.getFirst().length == 0) {
				throw new IOException("Expecting at least one region.");
			}

			int relRegNum = 0;
			List<InputSplit> splits = new ArrayList<InputSplit>(
					keys.getFirst().length);

			String regionLocation;
			HRegionInfo reginfor;
			long tmpid = 0;
			int k = 0;

			for (int i = 0; i < keys.getFirst().length; i++) {
				if (!includeRegionInSplit(keys.getFirst()[i],
						keys.getSecond()[i])) {
					continue;
				}

				regionLocation = table.getRegionLocation(keys.getFirst()[i])
						.getHostname();

				reginfor = table.getRegionLocation(keys.getFirst()[i])
						.getRegionInfo();

				tmpid = reginfor.getRegionId();

				byte[] startRow = scan.getStartRow();
				byte[] stopRow = scan.getStopRow();
				
				// determine if the given start an stop key fall into the region
				if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes
						.compareTo(startRow, keys.getSecond()[i]) < 0)
						&& (stopRow.length == 0 || Bytes.compareTo(stopRow,
								keys.getFirst()[i]) > 0)) {
					byte[] splitStart = startRow.length == 0
							|| Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ? keys
							.getFirst()[i] : startRow;
					byte[] splitStop = (stopRow.length == 0 || Bytes.compareTo(
							keys.getSecond()[i], stopRow) <= 0)
							&& keys.getSecond()[i].length > 0 ? keys
							.getSecond()[i] : stopRow;

					InputSplit split = new TableSplit(table.getTableName(),
							splitStart, splitStop, regionLocation);

					splits.add(split);
					// if (LOG.isDebugEnabled())
					// LOG.debug("getSplits: split -> " + (count++) + " -> " +
					// split);
					
					
//					for (k = 0; k < selRegCnt; ++k) {
//						if (selReg[k] == tmpid) {
//							break;
//						}
//					}
//					if (k == selRegCnt) {
//						selReg[selRegCnt++] = tmpid;
					
					
					
				} else {
					// //eliminate current split
					for (j = 0; j < staScanRelregnum; ++j) {
						if (staScanRelreg[j] == tmpid) {
							relRegNum = j;
							break;
						}

					}
					if (j == staScanRelregnum)
						continue;
					else {
						
						for (k = 0; k < selRegCnt; ++k) {
							if (selReg[k] == tmpid) {
								break;
							}
						}
						if (k == selRegCnt) {
							selReg[selRegCnt++] = tmpid;

							byte[] splitStart1 = Bytes
									.toBytes(staScanPerReg[relRegNum][0]);
							byte[] splitStop1 = Bytes
									.toBytes(staScanPerReg[relRegNum][1]);

							InputSplit split1 = new TableSplit(
									table.getTableName(), splitStart1,
									splitStop1, regionLocation);

							splits.add(split1);

						} else {
							continue;
						}
					}

				}

			}

			// ........test...................//
			System.out.printf("the number of splits: %d\n", splits.size());

			// ...............................//

			return splits;

			// List<InputSplit> splits = new ArrayList<InputSplit>();
			// Scan scan = getScan();
			// byte startRow[] = scan.getStartRow(), stopRow[] =
			// scan.getStopRow();
			// byte prefixedStartRow[] = new byte[startRow.length + 1];
			// byte prefixedStopRow[] = new byte[stopRow.length + 1];
			// System.arraycopy(startRow, 0, prefixedStartRow, 1,
			// startRow.length);
			// System.arraycopy(stopRow, 0, prefixedStopRow, 1, stopRow.length);
			//
			// for (int prefix = -128; prefix < 128; prefix++) {
			// prefixedStartRow[0] = (byte) prefix;
			// prefixedStopRow[0] = (byte) prefix;
			// scan.setStartRow(prefixedStartRow);
			// scan.setStopRow(prefixedStopRow);
			// setScan(scan);
			// splits.addAll(super.getSplits(context));
			// }

			// return splits;
		}
	}
	public static class RegIndiviTableInputFormat extends TableInputFormat {
		// the whole region with relevant node is for one split

		// final Log LOG = LogFactory.getLog(TableInputFormatBase.class);

		public static int staRelregnum = 0;
		public static long[] staRelreg = new long[200];
		public static double[][] nodeReg= new double[200][100];
		public static int[] nodeRegCnt=new int[200];

		public static void getRegion(int num, long regid[], double nodePerReg[][], int nodePerRegCnt[]) {

			// System.out.printf("regions of interest: ");

			int tmp=0;
			for (int i = 0; i < num; ++i) {
				staRelreg[i] = regid[i];
				
				nodeRegCnt[i]=nodePerRegCnt[i];
				tmp=nodeRegCnt[i];
				
				for(int k=0;k<tmp;++k)
				{
					nodeReg[i][k]=nodePerReg[i][k];
				}
//				public static int[] nodeRegCnt=new int[200];
				
			}
			staRelregnum = num;

			// System.out.printf("\n");
			return;
		}

		@Override
		public List<InputSplit> getSplits(JobContext context)
				throws IOException {

			int j = 0;
//			int tmpcnt = 0;
			long[] selReg = new long[200];
			int selRegCnt = 0;

			HTable table = this.getHTable(); // maybe super.getHTable
			Scan scan = this.getScan(); // maybe super.getHTable

			if (table == null) {
				throw new IOException("No table was provided.");
			}
			Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
			if (keys == null || keys.getFirst() == null
					|| keys.getFirst().length == 0) {
				throw new IOException("Expecting at least one region.");
			}
			// int count = 0;

			List<InputSplit> splits = new ArrayList<InputSplit>(
					keys.getFirst().length);

			String regionLocation;
			HRegionInfo reginfor;
			long tmpid = 0;

			for (int i = 0; i < keys.getFirst().length; i++) {
				if (!includeRegionInSplit(keys.getFirst()[i],
						keys.getSecond()[i])) {
					continue;
				}

				regionLocation = table.getRegionLocation(keys.getFirst()[i])
						.getHostname();

				reginfor = table.getRegionLocation(keys.getFirst()[i])
						.getRegionInfo();

				tmpid = reginfor.getRegionId();

				byte[] startRow = scan.getStartRow();
				byte[] stopRow = scan.getStopRow();
				// determine if the given start an stop key fall into the region
				if ((startRow.length == 0 || keys.getSecond()[i].length == 0 || Bytes
						.compareTo(startRow, keys.getSecond()[i]) < 0)
						&& (stopRow.length == 0 || Bytes.compareTo(stopRow,
								keys.getFirst()[i]) > 0)) {
					byte[] splitStart = startRow.length == 0
							|| Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ? keys
							.getFirst()[i] : startRow;
					byte[] splitStop = (stopRow.length == 0 || Bytes.compareTo(
							keys.getSecond()[i], stopRow) <= 0)
							&& keys.getSecond()[i].length > 0 ? keys
							.getSecond()[i] : stopRow;
							
							
					InputSplit split = new TableSplit(table.getTableName(),
							splitStart, splitStop, regionLocation);
					splits.add(split);
					// if (LOG.isDebugEnabled())
					// LOG.debug("getSplits: split -> " + (count++) + " -> " +
					// split);

					selReg[selRegCnt++] = tmpid;

				} else {
					// //eliminate current split
					for (j = 0; j < staRelregnum; ++j) {
						if (staRelreg[j] == tmpid) {
							break;
						}

					}
					if (j == staRelregnum)
						continue;
					else {
						int k = 0;
						for (k = 0; k < selRegCnt; ++k) {
							if (selReg[k] == tmpid) {
								break;
							}
						}
						if (k == selRegCnt) {
							selReg[selRegCnt++] = tmpid;
							
							
							int tmpcnt= nodeRegCnt[j];
								
							for(int p=j;p<tmpcnt;++p)
							{

								InputSplit split = new TableSplit(
										table.getTableName(), Bytes.toBytes(regvalKeyCon((long) nodeReg[j][p]) + "," + "0"),
										 Bytes.toBytes(regvalKeyCon((long) nodeReg[j][p]+1) + "," + "0"), regionLocation);
								splits.add(split);								
								
							}
						} else {
							continue;
						}
					}
				}
			}

			// ........test...................//
			System.out.printf("the number of splits: %d\n", splits.size());

			// ...............................//

			return splits;

			// List<InputSplit> splits = new ArrayList<InputSplit>();
			// Scan scan = getScan();
			// byte startRow[] = scan.getStartRow(), stopRow[] =
			// scan.getStopRow();
			// byte prefixedStartRow[] = new byte[startRow.length + 1];
			// byte prefixedStopRow[] = new byte[stopRow.length + 1];
			// System.arraycopy(startRow, 0, prefixedStartRow, 1,
			// startRow.length);
			// System.arraycopy(stopRow, 0, prefixedStopRow, 1, stopRow.length);
			//
			// for (int prefix = -128; prefix < 128; prefix++) {
			// prefixedStartRow[0] = (byte) prefix;
			// prefixedStopRow[0] = (byte) prefix;
			// scan.setStartRow(prefixedStartRow);
			// scan.setStopRow(prefixedStopRow);
			// setScan(scan);
			// splits.addAll(super.getSplits(context));
			// }

			// return splits;
		}
	}

	static class MapperTempInterval extends
			TableMapper<ImmutableBytesWritable, Text> {

		public static long cntnum = 0;
		public Text model = new Text();

		String interval = new String();
		double bd[] = new double[3];
		String interv = new String();

		String lbstr = new String();
		String rbstr = new String();
		String modOrdstr = new String();

		double lb = 0.0, rb = 0.0;
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

				bd[0] = Double.parseDouble(lbstr);
				bd[1] = Double.parseDouble(rbstr);

				// ...................//

				model.set(modelContract(values, modOrd, bd[0], bd[1]));
				numMapIn++;
				cntnum++;
				// model.set(modelContract(values, modOrd, interv));

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

	static class MapperValInterval extends
			TableMapper<ImmutableBytesWritable, Text> {

		private long cntnum = 0;
		private Text model = new Text();

		String interval = new String();
		double bd[] = new double[3];
		String interv = new String();

		String lbstr = new String();
		String rbstr = new String();
		String modOrdstr = new String();

		double lb = 0.0, rb = 0.0;
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

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {

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

				bd[0] = Double.parseDouble(lbstr);
				bd[1] = Double.parseDouble(rbstr);

				// ...................//

				model.set(modelContract(values, modOrd, bd[0], bd[1]));
				numMapIn++;
				cntnum++;
				// model.set(modelContract(values, modOrd, interv));

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

	static class MapperTempIntervalNoRed extends TableMapper<Text, Text> {

		String interval = new String();
		double bd[] = new double[3];
		String interv = new String();

		String lbstr = new String();
		String rbstr = new String();
		String modOrdstr = new String();

		double lb = 0.0, rb = 0.0;
		int modOrd = 0;

		ImmutableBytesWritable intkey = new ImmutableBytesWritable();

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {

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

			bd[0] = Double.parseDouble(lbstr);
			bd[1] = Double.parseDouble(rbstr);

			// ...................//

			// model.set(modelContract(values, modOrd, bd[0], bd[1]));
			// numMapIn++;
			// model.set(modelContract(values, modOrd, interv));

			if (lb > bd[1] || rb < bd[0]) // no overlap
			{

			} else {

				// intkey.set(Bytes.toBytes(cntnum), 0, Bytes.SIZEOF_LONG);
				// context.write(intkey, model);
			}
		}
	}

	static class MapperValIntervalNoRed extends TableMapper<Text, Text> {

		String interval = new String();
		double bd[] = new double[3];
		String interv = new String();

		String lbstr = new String();
		String rbstr = new String();
		String modOrdstr = new String();

		double lb = 0.0, rb = 0.0;
		int modOrd = 0;

		ImmutableBytesWritable intkey = new ImmutableBytesWritable();

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {

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

			bd[0] = Double.parseDouble(lbstr);
			bd[1] = Double.parseDouble(rbstr);

			// ...................//

			// model.set(modelContract(values, modOrd, bd[0], bd[1]));
			// numMapIn++;
			// model.set(modelContract(values, modOrd, interv));

			if (lb > bd[1] || rb < bd[0]) // no overlap
			{

			} else {

				// intkey.set(Bytes.toBytes(cntnum), 0, Bytes.SIZEOF_LONG);
				// context.write(intkey, model);
			}
		}
	}

	static class MapperTempPoint extends
			TableMapper<ImmutableBytesWritable, Text> {

		public static long cntnum = 0;
		public Text model = new Text();

		String interval = new String();
		double bd[] = new double[3];
		double val = 0.0;

		String valstr = new String();
		String lbstr = new String();
		String rbstr = new String();
		String modOrdstr = new String();

		double lb = 0.0, rb = 0.0;
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

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {

			try {
				Configuration conf = context.getConfiguration();
				valstr = conf.get("val");
				modOrdstr = conf.get("modelOrd");
				modOrd = Integer.parseInt(modOrdstr);

				lbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("st")));
				rbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("ed")));

				val = Double.parseDouble(valstr);
				bd[0] = Double.parseDouble(lbstr);
				bd[1] = Double.parseDouble(rbstr);

				// ...................//

				model.set(modelContract(values, modOrd, bd[0], bd[1]));
				numMapIn++;
				cntnum++;

				if (val <= bd[1] && val >= bd[0]) {
					intkey.set(Bytes.toBytes(cntnum), 0, Bytes.SIZEOF_LONG);
					context.write(intkey, model);
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
	}

	static class MapperValPoint extends
			TableMapper<ImmutableBytesWritable, Text> {

		public static long cntnum = 0;
		public Text model = new Text();

		String interval = new String();
		double bd[] = new double[3];
		double val = 0.0;

		String valstr = new String();
		String lbstr = new String();
		String rbstr = new String();
		String modOrdstr = new String();

		double lb = 0.0, rb = 0.0;
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

		@Override
		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException {

			try {
				Configuration conf = context.getConfiguration();
				valstr = conf.get("val");
				modOrdstr = conf.get("modelOrd");
				modOrd = Integer.parseInt(modOrdstr);

				lbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("vl")));
				rbstr = Bytes.toString(values.getValue(Bytes.toBytes("attri"),
						Bytes.toBytes("vr")));

				val = Double.parseDouble(valstr);
				bd[0] = Double.parseDouble(lbstr);
				bd[1] = Double.parseDouble(rbstr);

				// ...................//

				model.set(modelContract(values, modOrd, bd[0], bd[1]));
				numMapIn++;
				cntnum++;

				if (val <= bd[1] && val >= bd[0]) {
					intkey.set(Bytes.toBytes(cntnum), 0, Bytes.SIZEOF_LONG);
					context.write(intkey, model);
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
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

		public double gridstep = 5;// 30.0;

		public void modelParser2(String modelstr, double coef[], double range[])// range[0]:
		// left
		{

			int len = modelstr.length(), st = 0, num = 0;
			for (int i = 0; i < len; ++i) {
				if (modelstr.charAt(i) == ',') {
					range[num++] = Double
							.parseDouble(modelstr.substring(st, i));
					st = i + 1;
					if (modelstr.charAt(i) == ',' && num >= 2) { // modification

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

	public static class ReducerPoint extends
			TableReducer<ImmutableBytesWritable, Text, ImmutableBytesWritable> {

		public double cntnum = 0;
		public double[] coef = new double[10], range = new double[5];
		public double[] res = new double[100];
		public double respoint = 0.0;

		public double step = 0.0;
		public double st = 0.0, ed = 0.0;
		String reskey = new String();

		public double gridstep = 1;// 30.0;

		public void modelParser2(String modelstr, double coef[], double range[])// range[0]:
		// left
		{

			int len = modelstr.length(), st = 0, num = 0;
			for (int i = 0; i < len; ++i) {
				if (modelstr.charAt(i) == ',') {
					range[num++] = Double
							.parseDouble(modelstr.substring(st, i));
					st = i + 1;
					if (modelstr.charAt(i) == ',' && num >= 2) { // modification

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

	public double jobPointEvalQuery(double val, String idxtabname,
			String strow, String edrow, String col[], double runpara[])
			throws Exception {

		long st = 0, ed = 0;

		Configuration conf = HBaseConfiguration.create();

		conf.set("val", Double.toString(val));
		conf.set("modelOrd", Integer.toString(modorder));

		// conf.set("reducernum", Double.toString(rednum));

		Job job = new Job(conf, "job");
		job.setJarByClass(kviMRproCol.class);// modification
		// job.getConfiguration().setInt("mapred.map.tasks", 2000);
		// job.setNumReduceTasks(nored);

		Scan scan = new Scan();
		scan.setCaching(scanCache);
		scan.setCacheBlocks(false);

		// .....test.............//
		// System.out.printf("%s    %s\n",col[0],col[1]);

		// ......................//

		scan.setStartRow(Bytes.toBytes(strow));
		scan.setStopRow(Bytes.toBytes(edrow));
		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[0]));
		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[1]));
		scan.addFamily(Bytes.toBytes("model"));

		if (col[0] == "st") {

			job.setNumReduceTasks(1);
			TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
					MapperTempPoint.class, ImmutableBytesWritable.class,
					Text.class, job, true, RegTableInputFormat.class);

			TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
					ReducerPoint.class, job);

		} else {

			job.setNumReduceTasks(1);
			TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
					MapperValPoint.class, ImmutableBytesWritable.class,
					Text.class, job, true, RegTableInputFormat.class);

			TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
					ReducerPoint.class, job);
		}

		boolean done = job.waitForCompletion(true);
		if (!done) {
			throw new IOException("error with job!");
		}

		runpara[0] = job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_OUTPUT_RECORDS").getValue();
		runpara[1] = job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.JobInProgress$Counter",
						"TOTAL_LAUNCHED_MAPS").getValue();

		return job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_INPUT_RECORDS").getValue();

		// TOTAL_LAUNCHED_MAPS

	}

	public double jobIntervalEvalQuery(double lb, double rb, String idxtabname,
			String strow, String edrow, String col[], double runpara[])
			throws Exception {

		long st = 0, ed = 0;

		Configuration conf = HBaseConfiguration.create();

		conf.set("lb", Double.toString(lb));
		conf.set("rb", Double.toString(rb));
		conf.set("modelOrd", Integer.toString(modorder));

		// conf.set("reducernum", Double.toString(rednum));

		Job job = new Job(conf, "job");
		job.setJarByClass(kviMRproCol.class);// modification
		// job.getConfiguration().setInt("mapred.map.tasks", 2000);
		job.setNumReduceTasks(nored);

		Scan scan = new Scan();
		scan.setCaching(scanCache);
		scan.setCacheBlocks(false);

		scan.setStartRow(Bytes.toBytes(strow));
		scan.setStopRow(Bytes.toBytes(edrow));
		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[0]));
		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[1]));
		scan.addFamily(Bytes.toBytes("model"));

		job.setInputFormatClass(RegTableInputFormat.class);

		if (col[0] == "st") {
			TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
					MapperTempInterval.class, ImmutableBytesWritable.class,
					Text.class, job, true, RegTableInputFormat.class);

			TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
					ReducerEval.class, job);

		} else {
			TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
					MapperValInterval.class, ImmutableBytesWritable.class,
					Text.class, job, true, RegTableInputFormat.class);

			TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
					ReducerEval.class, job);
		}

		boolean done = job.waitForCompletion(true);
		if (!done) {
			throw new IOException("error with job!");
		}

		runpara[0] = job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_OUTPUT_RECORDS").getValue();
		runpara[1] = job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.JobInProgress$Counter",
						"TOTAL_LAUNCHED_MAPS").getValue();

		return job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_INPUT_RECORDS").getValue();

		// TOTAL_LAUNCHED_MAPS

	}

	public double jobIntervalEvalQueryNoReducer(double lb, double rb,
			String idxtabname, String strow, String edrow, String col[])
			throws Exception {

		Configuration conf = HBaseConfiguration.create();

		conf.set("lb", Double.toString(lb));
		conf.set("rb", Double.toString(rb));
		conf.set("modelOrd", Integer.toString(modorder));

		Job job = new Job(conf, "job");
		job.setJarByClass(kviMRproCol.class);// modification

		// job.getConfiguration().setInt("mapred.map.tasks", 2000);
		job.setNumReduceTasks(0);

		Scan scan = new Scan();
		scan.setCaching(scanCache);
		scan.setCacheBlocks(false);
		// scan.setStartRow(strow.getBytes());
		// scan.setStopRow(edrow.getBytes());

		scan.setStartRow(Bytes.toBytes(strow));
		scan.setStopRow(Bytes.toBytes(edrow));
		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[0]));
		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[1]));
		// scan.addFamily(Bytes.toBytes("model"));

		if (col[0] == "st") {
			scan.setCaching(250);
			TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
					MapperTempIntervalNoRed.class, null, null, job);

			// TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
			// MapperTempIntervalNoRed.class, ImmutableBytesWritable.class,
			// Text.class,
			// job);
			//
			// TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
			// ReducerEval.class, job);

		} else {

			scan.setCaching(180);
			TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
					MapperValIntervalNoRed.class, null, null, job);

			// TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
			// MapperValIntervalNoRed.class, ImmutableBytesWritable.class,
			// Text.class,
			// job);
			//
			// TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
			// ReducerEval.class, job);
		}
		job.setOutputFormatClass(NullOutputFormat.class);

		boolean done = job.waitForCompletion(true);
		if (!done) {
			throw new IOException("error with job!");
		}

		// runpara[0]=job
		// .getCounters()
		// .findCounter("org.apache.hadoop.mapred.Task$Counter",
		// "MAP_OUTPUT_RECORDS").getValue();
		// runpara[1]=job
		// .getCounters()
		// .findCounter("org.apache.hadoop.mapred.JobInProgress$Counter",
		// "TOTAL_LAUNCHED_MAPS").getValue();

		return job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_INPUT_RECORDS").getValue();

	}

	public double jobScanRegIntervalEvalQuery(double lb, double rb,
			String idxtabname, String strow, String edrow, String col[],
			double runpara[]) throws Exception {

		long st = 0, ed = 0;

		Configuration conf = HBaseConfiguration.create();

		conf.set("lb", Double.toString(lb));
		conf.set("rb", Double.toString(rb));
		conf.set("modelOrd", Integer.toString(modorder));

		// conf.set("reducernum", Double.toString(rednum));

		Job job = new Job(conf, "job");
		job.setJarByClass(kviMRproCol.class);// modification
		// job.getConfiguration().setInt("mapred.map.tasks", 2000);
		job.setNumReduceTasks(nored);

		Scan scan = new Scan();
		scan.setCaching(scanCache);
		scan.setCacheBlocks(false);

		scan.setStartRow(Bytes.toBytes(strow));
		scan.setStopRow(Bytes.toBytes(edrow));
		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[0]));
		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[1]));
		scan.addFamily(Bytes.toBytes("model"));

		job.setInputFormatClass(RegScanTableInputFormat.class);

		if (col[0] == "st") {
			TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
					MapperTempInterval.class, ImmutableBytesWritable.class,
					Text.class, job, true, RegScanTableInputFormat.class);

			TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
					ReducerEval.class, job);

		} else {
			TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
					MapperValInterval.class, ImmutableBytesWritable.class,
					Text.class, job, true, RegScanTableInputFormat.class);

			TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
					ReducerEval.class, job);
		}

		boolean done = job.waitForCompletion(true);
		if (!done) {
			throw new IOException("error with job!");
		}

		runpara[0] = job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_OUTPUT_RECORDS").getValue();
		runpara[1] = job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.JobInProgress$Counter",
						"TOTAL_LAUNCHED_MAPS").getValue();

		return job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_INPUT_RECORDS").getValue();

		// TOTAL_LAUNCHED_MAPS

	}
	public double jobIndiviRegIntervalEvalQuery(double lb, double rb,
			String idxtabname, String strow, String edrow, String col[],
			double runpara[]) throws Exception {

		long st = 0, ed = 0;

		Configuration conf = HBaseConfiguration.create();

		conf.set("lb", Double.toString(lb));
		conf.set("rb", Double.toString(rb));
		conf.set("modelOrd", Integer.toString(modorder));

		// conf.set("reducernum", Double.toString(rednum));

		Job job = new Job(conf, "job");
		job.setJarByClass(kviMRproCol.class);// modification
		// job.getConfiguration().setInt("mapred.map.tasks", 2000);
		job.setNumReduceTasks(nored);

		Scan scan = new Scan();
		scan.setCaching(scanCache);
		scan.setCacheBlocks(false);

		scan.setStartRow(Bytes.toBytes(strow));
		scan.setStopRow(Bytes.toBytes(edrow));
		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[0]));
		scan.addColumn(Bytes.toBytes("attri"), Bytes.toBytes(col[1]));
		scan.addFamily(Bytes.toBytes("model"));

		job.setInputFormatClass(RegIndiviTableInputFormat.class);

		if (col[0] == "st") {
			TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
					MapperTempInterval.class, ImmutableBytesWritable.class,
					Text.class, job, true, RegIndiviTableInputFormat.class);

			TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
					ReducerEval.class, job);

		} else {
			TableMapReduceUtil.initTableMapperJob(idxtabname, scan,
					MapperValInterval.class, ImmutableBytesWritable.class,
					Text.class, job, true, RegIndiviTableInputFormat.class);

			TableMapReduceUtil.initTableReducerJob(idxtabname + "Res",
					ReducerEval.class, job);
		}

		boolean done = job.waitForCompletion(true);
		if (!done) {
			throw new IOException("error with job!");
		}

		runpara[0] = job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_OUTPUT_RECORDS").getValue();
		runpara[1] = job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.JobInProgress$Counter",
						"TOTAL_LAUNCHED_MAPS").getValue();

		return job
				.getCounters()
				.findCounter("org.apache.hadoop.mapred.Task$Counter",
						"MAP_INPUT_RECORDS").getValue();

		// TOTAL_LAUNCHED_MAPS

	}


}
