package basetool;

import java.io.*;
import java.util.*;

public class kviManager {

	// public kviIndex kvi;
	public kviIndexCol kviCol;
	// public kviMRpro kviquery;
	public kviMRproCol kviqueryCol;
	public String idxtabname;
	public scanPro tabscan;
	public long tduration;
	public double numtup;
	public String tupRangeL, tupRangeR;
	public long inmem, randacc, mracc;
	public long schlevel;
	public double parall;
	public double mapout, mapcnt;
	public String strk, edrk;

	public kviManager(String tabname, boolean startIdx, int sign)
			throws IOException {// 0: temp 1:val
		// kvi = new kviIndex(tabname);
		kviCol = new kviIndexCol(tabname);

		if (startIdx == true) {

			kviCol.iniIdx();
		} else {

			kviCol.iniQueryRes();
		}

		kviqueryCol = new kviMRproCol(0, tabname, sign);

		idxtabname = tabname;
		tabscan = new scanPro(0);
		tduration = 0;
		schlevel = 0;

		numtup = 0.0;
		tupRangeL = tupRangeR = "";
		inmem = randacc = mracc = 0;

		mapout = 0.0;
		mapcnt = 0.0;

		parall = 4.0;

		strk = new String();
		edrk = new String();

	}

	// public boolean kvi_insert(double l, double r, double modinfo[], int
	// order,
	// double assocl, double assocr, double timecons[]) {
	//
	// double[] timecnt = new double[4];
	// if (kvi.insert(l, r, modinfo, order, assocl, assocr, timecnt) == true) {
	//
	// timecons[0] = timecnt[0];
	// timecons[1] = timecnt[1];
	//
	// return true;
	// } else {
	// return false;
	// }
	// }
	public boolean kvi_insert(double l, double r, double modinfo[], int order,
			double assocl, double assocr, double timecons[], String qual[],
			String assoQual[]) {

		double[] timecnt = new double[4];
		if (kviCol.insert(l, r, modinfo, order, assocl, assocr, timecnt, qual,
				assoQual) == true) {

			timecons[0] = timecnt[0];
			timecons[1] = timecnt[1];

			return true;
		} else {
			return false;
		}
	}

	public boolean kvi_pointSearch(double val, String qual[], int qrytype) {// 0:
																			// temp
																			// 1:
																			// value
		String[][] indi = new String[100][2];
		long stcnt = 0, edcnt = 0;

		stcnt = System.nanoTime();
		int resnum = kviCol.pointSearch_res(indi, val);

		// ..........modification..........................//

		// numtup = tabscan.pointSearch_scan(idxtabname, indi, val, resnum,
		// qual,
		// qrytype);

		edcnt = System.nanoTime();

		// ..........metric.......................//
		tduration = (long) ((edcnt - stcnt) / parall);

		return true;
	}

	public boolean kvi_pointSearch_mr(double val, String qual[], int qrytype) throws Exception {// 0:
		// temp
		// 1:
		// value
		String[] bdrow = new String[5];
		double[] runpara=new double[10];
		long stcnt = 0, edcnt = 0;
		
		String[] tcol={"tl","tr"}, vcol={"vl","vr"};

		stcnt = System.nanoTime();
		int resnum = kviCol.pointSearch_resMr(bdrow, val);
		
		
		//......test................//
		System.out.printf("%s   %s\n", bdrow[0],bdrow[1]);
		
//		if (qrytype==0)
//		{
//		  kviqueryCol.jobPointEvalQuery(val, idxtabname, bdrow[0], bdrow[1], tcol, runpara);
//		}
//		else
//		{
//			kviqueryCol.jobPointEvalQuery(val, idxtabname, bdrow[0], bdrow[1], vcol, runpara);
//		}
		edcnt = System.nanoTime();

		// ..........metric.......................//
		tduration = (long) ((edcnt - stcnt) / parall);

		return true;
	}

	public double kvi_timeCnt() {
		return tduration / 1000000000.0;
	}

	public void kvi_metricOutput(double stat[], String rowrange[]) {
		stat[0] = tduration / 1000000000.0;
		stat[1] = inmem / 1000000000.0;
		stat[2] = randacc / 1000000000.0;
		stat[3] = mracc / 1000000000.0;
		stat[4] = numtup;
		stat[5] = schlevel;

		stat[6] = mapout;
		stat[7] = mapcnt;

		rowrange[0] = tupRangeL;
		rowrange[1] = tupRangeR;
		return;
	}

	public void kvi_metricOutput(double stat[]) {
		stat[0] = tduration / 1000000000.0;
		stat[1] = numtup;
		return;
	}

	// public boolean kvi_rangeSearch(double l, double r) {
	//
	// String[][] indi = new String[100][2];
	// String[][] parallel = new String[100][2];
	// long stcnt = 0, edcnt = 0, midcnt = 0, inmemCnt;
	//
	//
	// // ..test..//
	// double tmp = 0.0;
	//
	// stcnt = System.nanoTime();
	//
	// int resnum = kvi.intervalSearch_res(indi, parallel, l, r);
	//
	// inmemCnt = System.nanoTime();
	//
	// try {
	// kviMRpro.numMapIn = 0;
	// tmp = kviquery.jobIntervalEvalQuery(l, r, idxtabname,
	// parallel[0][0], parallel[1][1]);
	//
	// // tmp = kviqueryCol.jobIntervalEvalQuery(l, r, idxtabname,
	// // parallel[0][0], parallel[1][1]);
	//
	// } catch (Exception e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// midcnt = System.nanoTime();
	//
	// numtup = tabscan.rangeSearch_scan(idxtabname, indi, l, r, resnum) + tmp;
	//
	// edcnt = System.nanoTime();
	//
	// tupRangeL = parallel[0][0];
	// tupRangeR = parallel[1][1];
	//
	// // ......metric and running time record............//
	// tduration = Math.max(midcnt - stcnt, edcnt - midcnt);
	// inmem = inmemCnt - stcnt;
	// mracc = midcnt - inmemCnt;
	// randacc = edcnt - midcnt;
	// schlevel = resnum;
	//
	// return true;
	// }
	public boolean kvi_rangeSearch(double l, double r, String qual[], int sign)// 0:
																				// temp,
																				// 1:value
			throws Exception {

		String[][] indi = new String[100][2];
		String[][] parallel = new String[100][2];
		long stcnt = 0, edcnt = 0, midcnt = 0, inmemCnt;
		double[] runpara = { 0.0, 0.0 };

		// ..test..//
		double tmp = 0.0;

		stcnt = System.nanoTime();

		int resnum = kviCol.intervalSearch_res(indi, parallel, l, r);

		kviMRproCol.numMapIn = 0;

		inmemCnt = System.nanoTime();

		tmp = kviqueryCol.jobIntervalEvalQuery(l, r, idxtabname,
				parallel[0][0], parallel[1][1], qual, runpara);

		midcnt = System.nanoTime();

		// modification
		numtup = tmp;

		if (sign == 0) {
			numtup = tabscan.rangeSearch_scan(idxtabname, indi, l, r, resnum,
					qual) + numtup;
		}

		edcnt = System.nanoTime();

		// .....test...........//
		// System.out.printf("%f", numtup-tmp);

		tupRangeL = parallel[0][0];
		tupRangeR = parallel[1][1];

		// ......metric and running time record............//
		tduration = Math.max(midcnt - stcnt, edcnt - midcnt);
		inmem = inmemCnt - stcnt;
		mracc = midcnt - inmemCnt;
		randacc = edcnt - midcnt;
		schlevel = resnum;

		mapout = runpara[0];
		mapcnt = runpara[1];

		return true;
	}

	public boolean kvi_rangeSearchTest(double l, double r, String qual[],
			int sign)// 0: temp, 1:value
			throws Exception {

		String[] parallel = new String[5];
		long stcnt = 0, edcnt = 0, midcnt = 0, inmemCnt;
		double[] runpara = { 0.0, 0.0 };

		// ..test..//
		double tmp = 0.0;

		stcnt = System.nanoTime();

		int resnum = kviCol.intervalSearch_resTest(parallel, l, r, sign);

		kviMRproCol.numMapIn = 0;

		inmemCnt = System.nanoTime();

		tmp = kviqueryCol.jobIntervalEvalQuery(l, r, idxtabname, parallel[0],
				parallel[1], qual, runpara);

		midcnt = System.nanoTime();

		// modification
		numtup = tmp;
		edcnt = System.nanoTime();

		// ......metric and running time record............//
		tduration = Math.max(midcnt - stcnt, edcnt - midcnt);
		inmem = inmemCnt - stcnt;
		mracc = midcnt - inmemCnt;
		randacc = edcnt - midcnt;
		schlevel = resnum;

		strk = parallel[0];
		edrk = parallel[1];
		mapout = runpara[0];
		mapcnt = runpara[1];

		return true;
	}

	public boolean kvi_rangeSearchTrivial(double l, double r, String qual[],
			int sign)// 0: temp, 1:value
			throws Exception {

		// String[][] indi = new String[100][2];
		String[] parallel = new String[5];
		long stcnt = 0, edcnt = 0, midcnt = 0, inmemCnt;
		double[] runpara = { 0.0, 0.0 };

		// ..test..//
		double tmp = 0.0;

		stcnt = System.nanoTime();

		int resnum = kviCol.intervalSearch_resTrivial(parallel, l, r, sign);

		kviMRproCol.numMapIn = 0;

		inmemCnt = System.nanoTime();

		// ....test...........//
		System.out.printf("start end row for MR: %s   %s\n", parallel[0],
				parallel[1]);
		// ...................//

		// tmp = kviqueryCol.jobIntervalEvalQuery(l, r, idxtabname, parallel[0],
		// parallel[1], qual, runpara);

		midcnt = System.nanoTime();

		// modification
		numtup = tmp;
		edcnt = System.nanoTime();

		// .....test...........//
		// System.out.printf("%f", numtup-tmp);

		// tupRangeL = parallel[0][0];
		// tupRangeR = parallel[1][1];

		// ......metric and running time record............//
		tduration = Math.max(midcnt - stcnt, edcnt - midcnt);
		inmem = inmemCnt - stcnt;
		mracc = midcnt - inmemCnt;
		randacc = edcnt - midcnt;
		schlevel = resnum;

		strk = parallel[0];
		edrk = parallel[1];
		mapout = runpara[0];
		mapcnt = runpara[1];

		return true;
	}

	// public boolean kvi_rangeSearchNoGrid(double l, double r) {
	//
	// String[][] indi = new String[100][2];
	// String[][] parallel = new String[100][2];
	// long stcnt = 0, edcnt = 0, midcnt = 0, inmemCnt;
	//
	// double tmp = 0.0;
	//
	// stcnt = System.nanoTime();
	//
	// int resnum = kviCol.intervalSearch_res(indi, parallel, l, r);
	//
	// inmemCnt = System.nanoTime();
	//
	// try {
	// kviMRpro.numMapIn = 0;
	// tmp = kviqueryCol.jobIntervalEvalQueryNoReducer(l, r, idxtabname,
	// parallel[0][0], parallel[1][1]);
	//
	// } catch (Exception e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// midcnt = System.nanoTime();
	//
	// numtup = tabscan.rangeSearch_scanNoGrid(idxtabname, indi, l, r, resnum) +
	// tmp;
	//
	// edcnt = System.nanoTime();
	//
	// tupRangeL = parallel[0][0];
	// tupRangeR = parallel[1][1];
	//
	// // ......metric and running time record............//
	// tduration = Math.max(midcnt - stcnt, edcnt - midcnt);
	// inmem = inmemCnt - stcnt;
	// mracc = midcnt - inmemCnt;
	// randacc = edcnt - midcnt;
	// schlevel = resnum;
	//
	// return true;
	// }
	public boolean kvi_rangeSearchNoGrid(double l, double r, String qual[])
			throws Exception {

		String[][] indi = new String[100][2];
		String[][] parallel = new String[100][2];
		long stcnt = 0, edcnt = 0, midcnt = 0, inmemCnt;

		double tmp = 0.0;

		stcnt = System.nanoTime();

		int resnum = kviCol.intervalSearch_res(indi, parallel, l, r);

		inmemCnt = System.nanoTime();

		kviMRproCol.numMapIn = 0;
		// tmp = kviquery.jobIntervalEvalQueryNoReducer(l, r, idxtabname,
		// parallel[0][0], parallel[1][1]);

		// //...for test...//
		// System.out.print(parallel[0][0]+","+parallel[0][1]+"\n");
		// //..............//

		tmp = kviqueryCol.jobIntervalEvalQueryNoReducer(l, r, idxtabname,
				parallel[0][0], parallel[1][1], qual);

		midcnt = System.nanoTime();

		numtup = tmp;
		// numtup = tabscan.rangeSearch_scanNoGrid(idxtabname, indi, l, r,
		// resnum,qual) + tmp;

		edcnt = System.nanoTime();

		tupRangeL = parallel[0][0];
		tupRangeR = parallel[1][1];

		// ......metric and running time record............//
		tduration = Math.max(midcnt - stcnt, edcnt - midcnt);
		inmem = inmemCnt - stcnt;
		mracc = midcnt - inmemCnt;
		randacc = edcnt - midcnt;
		schlevel = resnum;

		return true;
	}

	// .........................conventiona kvi search......................//
	// public boolean conve_kvi_rangeSearch(double l, double r) {
	// String[][] indi = new String[100][2];
	// long stcnt = 0, edcnt = 0;
	//
	// stcnt = System.nanoTime();
	// int resnum = kviCol.conve_rangeSearch_res(indi, l, r);
	// // .......metric......................//
	// numtup = tabscan.rangeSearch_scan(idxtabname, indi, l, r, resnum);
	// numtup = numtup + tabscan.rangeSearch_interRangeScan(idxtabname, l, r);
	// edcnt = System.nanoTime();
	// tduration = edcnt - stcnt;
	//
	// return true;
	// }

	public boolean conve_kvi_rangeSearchConsec(double l, double r,
			String qual[], int sign) {// 0: temp, 1: value
		String[][] indi = new String[100][2];
		long stcnt = 0, edcnt = 0, midcnt = 0;

		stcnt = System.nanoTime();
		int resnum = kviCol.conve_rangeSearch_res(indi, l, r);

		numtup = 0.0;
		// some modification
		// if(r<=0)
		// {
		if (sign == 0)
		// .......metric......................//
		{
			numtup = tabscan.rangeSearch_scan(idxtabname, indi, l, r, resnum,
					qual);
		} else {
			resnum = 1;
			numtup = tabscan.rangeSearch_scan(idxtabname, indi, l, r, resnum,
					qual);
		}

		midcnt = System.nanoTime();
		numtup = numtup
				+ tabscan.rangeSearch_interRangeScanConsec(idxtabname, l, r,
						qual);
		edcnt = System.nanoTime();
		tduration = (long) Math.max(midcnt - stcnt, (edcnt - midcnt) / 1);
		// Math.max(midcnt - stcnt, edcnt - midcnt);

		return true;
	}

	// .....................................................................//
	public int paraOut(double para[]) {// change to kviCol class
		double[] param = new double[100];
		int num = kviCol.paraOutput(param);
		for (int i = 0; i < num; ++i) {
			para[i] = param[i];
		}
		return num;
	}

	public void paraSetup(int num, double para[]) {
		kviCol.paramConf(num, para);
		return;
	}

}
