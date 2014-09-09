import basetool.fileIO;
import basetool.HBaseOp;
import basetool.segm;
import basetool.segbTree;
import basetool.onlineFileIO;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class QueryPro {

	public static fileIO fio = new fileIO();
	public static HBaseOp hb = new HBaseOp();
	public static onlineFileIO olfio = new onlineFileIO();

	// public int[][] segnrum = new int[100000][10000];
	public static int tsnum;
	public static int tsrn;
	public static int tsegn;
	public static int mod2cnt;

	public static int[][] date = new int[7][3];

	public static segbTree tidx;// = new segbTree(20);
	public static segbTree vidx;// = new segbTree(20);

	// ........experiment setting...........//
	public static int expn = 100;
	public static double segpre = 0.005;
	public static int excMode = -1; // 1: lazy 0: batch

	QueryPro() {
		tsnum = 0;
		tsrn = 0;
		tsegn = 0;

		date = new int[7][3];

		mod2cnt = 0;

		// tidx = new segbTree(20);
		// vidx = new segbTree(20);

		// expn=10;
	}

	public static int dmodRaw() // trivial data model, similar to TSDB
	{
		String tabName;

		for (int j = 0; j < 1; j++) {
			tabName = (fio.rawid[j] + "Raw");
			hb.creTab(tabName);

			for (int i = 0; i < fio.rawn; ++i) {
				if (hb.put(tabName, fio.rawid[j] + ":" + fio.getrow(i, tsnum),
						"attri", "val", fio.getColVal(i, j)) == 0)
					return 0;
			}
		}
		// hb.scan(fio.rawid[0] + "M1");

		return 1;
	}

	public static int dmod1() // trivial segment data model
	{
		for (int j = 0; j < 1; j++) {

			String tabName;
			tabName = (fio.rawid[j] + "M1");
			hb.creTab(tabName);

			fio.segIni();
			tsegn = fio.seg2(j, segpre);
			// System.out.printf("%d\n", tsegn);
			int i = 0;

			for (i = 0; i < tsegn; ++i) {
				// System.out.printf("%d %d %d %f\n",i,
				// fio.st[i],fio.ed[i],fio.modCoe[i]);

				String row = fio.rawid[j] + "," + fio.tst[i] + "," + fio.ted[i];

				if (hb.put(tabName, row, "attri", "vl",
						Double.toString(fio.vl[i])) == 0)
					return 0;
				if (hb.put(tabName, row, "attri", "vr",
						Double.toString(fio.vr[i])) == 0)
					return 0;

				if (hb.put(tabName, row, "model", "cof1",
						Double.toString(fio.modCoe[i])) == 0)
					return 0;
			}
		}

		// for time index, only the time interval is thrown into index
		// for value index, the value+time interval is indexed.
		// no senID

		return 1;
	}

	public static int dmod2() // seg-model based data model
	{
		for (int j = 0; j < 1; j++) {
			// tsnum
			String tabName = "", tabNameM2V = "";
			tabName = (fio.rawid[j] + "M2");
			hb.creTab(tabName);

			tabNameM2V = (fio.rawid[j] + "M2V");
			hb.creTab(tabNameM2V);

			fio.segIni();
			tsegn = fio.seg2(j, segpre);
			// System.out.printf("%d\n", tsegn);
			int i = 0;

			int bitnum = (int) Math.log10(tsegn);

			// fio.output();

			// for (i = 0; i < tsegn; ++i) {
			// System.out.printf("%d %d %d %f\n",i,
			// fio.segs[i].st,fio.segs[i].et,fio.segs[i].modCoe);
			// }
			String rownum = "";

			for (i = 0; i < tsegn; ++i) {
				// System.out.printf("%d %d %d %f\n",i,
				// fio.st[i],fio.ed[i],fio.modCoe[i]);

				int bni = (int) Math.log10(i + 1);
				rownum = "";

				if (bni < bitnum) {
					for (int k = 0; k < (bitnum - bni); ++k)
						rownum += "0";
				}
				rownum += Integer.toString(i + 1);

				if (hb.put(tabName, fio.rawid[j] + rownum, "attri", "vl",
						Double.toString(fio.vl[i])) == 0)
					return 0;
				if (hb.put(tabName, fio.rawid[j] + rownum, "attri", "vr",
						Double.toString(fio.vr[i])) == 0)
					return 0;
				// if (hb.put(tabName, fio.rawid[j] + rownum, "attri", "st",
				// Integer.toString(fio.st[i])) == 0)
				// return 0;
				// if (hb.put(tabName, fio.rawid[j] + rownum, "attri", "ed",
				// Integer.toString(fio.ed[i])) == 0)
				// return 0;

				if (hb.put(tabName, fio.rawid[j] + rownum, "attri", "tst",
						fio.tst[i]) == 0)
					return 0;
				if (hb.put(tabName, fio.rawid[j] + rownum, "attri", "ted",
						fio.ted[i]) == 0)
					return 0;

				if (hb.put(tabName, fio.rawid[j] + rownum, "model", "cof1",
						Double.toString(fio.modCoe[i])) == 0)
					return 0;

				if (hb.put(tabNameM2V, Double.toString(fio.vl[i]) + ','
						+ Double.toString(fio.vr[i]), "attri", "no", rownum) == 0)
					return 0;
				// else
				// {
				// System.out.printf("%s\n",rownum);
				// }
				// System.out.printf("%s %s\n",fio.tst[i],fio.ted[i]);

			}

			tabName = (fio.rawid[j] + "Map");
			hb.creTab(tabName);

			hb.scanGloIni(fio.rawid[0] + "M2V");
			String tmpRes = "";
			int cnt = 1;
			bitnum = (int) Math.log10(tsegn);
			while ((tmpRes = hb.scanNext("attri", "no")) != "NO") {
				// System.out.printf("%d %s\n", cnt++, tmpRes);

				int bncnt = (int) Math.log10(cnt);
				rownum = "";

				if (bncnt < bitnum) {
					for (int k = 0; k < (bitnum - bncnt); ++k)
						rownum += "0";
				}
				rownum += Integer.toString(cnt++);

				if (hb.put(tabName, rownum, "attri", "no", tmpRes) == 0)
					return 0;
			}
			// getVal(String tab, String row, String cf, String cfq)
			// get(String tabname, String rkey, String cf, String qual)
		}

		return 1;
	}

	public static int upload() {

		tsnum = fio.loadfile();
		tsrn = fio.rawn;
		// hb.ini();

		// System.out.println(fio.getrow(0,6));

		// for (int i = 0; i < fio.rawn; ++i) {
		// if (hb.put("tsTab", fio.getrow(i,6), "attri", "val",
		// fio.getColVal(i, 2)) == 0)
		// return 0;
		// }
		return 1;
	}

	public static String getVal(String tab, String row, String cf, String cfq) {

		String[] res = new String[2];
		String[] qual = { cfq };

		hb.get(tab, row, cf, qual, res);
		// System.out.println(val + '\n');
		return res[0];
	}

	// .....................................ModelRaw.........................//

	public static void dmodRaw_qtp(String qt, String tabname,
			ArrayList<String> tpres) {

		String[] res = new String[3];
		String[] qual = { "val" };
		qt = "temp:" + qt;
		hb.get("tempRaw", qt, "attri", qual, res);
		tpres.add(res[0]);
		return;
	}

	public static long dmodRaw_tstmpMinus(String ts1, String ts2) {
		String tmp1 = "", tmp2 = "";// , res = "";
		int st = 0;
		int w = 1000000;
		long res = 0;
		for (int i = 0; i < ts1.length(); i++) {
			if (ts1.charAt(i) == ':' || ts1.charAt(i) == 'T') {
				tmp1 = ts1.substring(st, i);
				tmp2 = ts2.substring(st, i);
				st = i + 1;
				res += Math.abs(Double.parseDouble(tmp1)
						- Double.parseDouble(tmp2))
						* w;
				w = w / 10;
			}
		}
		tmp1 = ts1.substring(st, ts1.length());
		tmp2 = ts2.substring(st, ts2.length());
		res += Math.abs(Double.parseDouble(tmp1) - Double.parseDouble(tmp2))
				* w;

		return res;
	}

	public static void dmodRaw_qtpSeq(String qt, String tabname,
			ArrayList<String> tpres) {

		hb.scanGloIni(tabname);
		String tmpRes = "";
		String[] res = new String[3];
		String[] qual = new String[3];
		qual[0] = "val";

		hb.scanMNext();
		// String tsmm = "";
		String prer = "";

		long tsminus = 0, tsmm = 0;

		int isfd = 0, i, ini = 1;
		while ((tmpRes = hb.scanR()) != "NO") {

			for (i = 0; i < tmpRes.length(); ++i) {
				if (tmpRes.charAt(i) == ':') {
					// tstmp = tmpRes.substring(i + 1, tmpRes.length());
					break;
				}
			}
			tsminus = dmodRaw_tstmpMinus(
					tmpRes.substring(i + 1, tmpRes.length()),
					qt.substring(i + 1, tmpRes.length()));

			if (ini == 1) {
				tsmm = tsminus;
				prer = tmpRes;
				ini = 0;
			} else {
				if (tsminus <= tsmm) {
					tsmm = tsminus;
					prer = tmpRes;
				} else {
					break;
				}
			}
			// prer = tmpRes;
			hb.scanMNext();
		}
		tpres.add(prer);
		return;
	}

	public static void dmodRaw_qvp(String qv, String tabname,
			ArrayList<String> vpres, String[][] seg) {

		hb.scanGloIni(tabname);
		String tmpRes = "";
		String[] res = new String[3];
		String[] qual = new String[3];
		qual[0] = "val";
		// qual[1] = "vr";
		String trow = "";

		double v = Double.parseDouble(qv);
		hb.scanMNext();

		int inseg=0, segcnt=0;

		while ((tmpRes = hb.scanM("attri", qual, 1, res)) != "NO") {

			// System.out.printf("%s %s\n", res[0], res[1]);

			double tmp = Double.parseDouble(res[0]);

			if (Math.abs(v - tmp) <= 0.007) {

				// if (excMode == 1) {
				trow = hb.scanR();
				vpres.add(trow);
				
				if (inseg == 1) {
					seg[segcnt][1] = trow;
				} else {
					seg[segcnt][0] = trow;
					seg[segcnt][1] = trow;
					inseg = 1;
				}
			} else if (inseg == 1) {
				segcnt++;
				inseg = 0;
			}

			hb.scanMNext();
		}
		seg[segcnt][0] = "-1";
		seg[segcnt][1] = "-1";

	}

	public static void dmodRaw_qtInt(String tl, String tr, String tabname,
			ArrayList<String> tires) {
		hb.scanGloIni(tabname);
		String tmpRes = "";
		String[] res = new String[3];
		String[] qual = new String[3];
		qual[0] = "val";

		hb.scanMNext();

		int numres = 0;
		while ((tmpRes = hb.scanR()) != "NO") {

			int len = tmpRes.length(), strst = 0;

			for (int i = 0; i < len; i++) {
				if (tmpRes.charAt(i) == ':') {
					res[0] = tmpRes.substring(i + 1, len);
					break;
				}
			}

			if (tl.compareTo(res[0]) <= 0 && tr.compareTo(res[0]) >= 0) {

				// if (excMode == 1) {
				// hb.scanM("attri", qual, 1, res);
				// }

				tires.add(tmpRes);
				++numres;

			} else if (tr.compareTo(res[0]) < 0) {
				break;
			}

			hb.scanMNext();
		}

		// if (excMode == 0) {
		// hb.scanIni("tempM1", tires.get(0), tires.get(numres - 1));
		//
		// hb.scanMNext();
		//
		// while ((tmpRes = hb.scanM("attri", qual, 1, res)) != "NO") {
		// hb.scanMNext();
		// }
		// }
		return;
		// return "NO";
		// return numres;
	}

	public static void dmodRaw_qvInt(String vl, String vr, String tabname,
			ArrayList<String> viSeg, String[][] seg) {// vl small; vr big
		hb.scanGloIni(tabname);
		String tmpRes = "";
		String[] res = new String[3];
		String[] qual = new String[3];
		qual[0] = "val";

		hb.scanMNext();

		int isSeg = 0;
		String st = "", curt = "";

		int inseg=0, segcnt=0;
		String trow=new String();
		
		while ((tmpRes = hb.scanM("attri", qual, 1, res)) != "NO") {

			if (vl.compareTo(res[0]) <= 0 && vr.compareTo(res[0]) >= 0) {

				trow = hb.scanR();

				viSeg.add(trow);
				if (inseg == 1) {
					seg[segcnt][1] = trow;
				} else {
					seg[segcnt][0] = trow;
					seg[segcnt][1] = trow;
					inseg = 1;
				}
			} else if (inseg == 1) {
				segcnt++;
				inseg = 0;
			}

			hb.scanMNext();
		}
		seg[segcnt][0] = "-1";
		seg[segcnt][1] = "-1";
		return;
	}

	public static void dmodRaw_compq(String st, String ed, String vl,
			String vr, String tabname, ArrayList<String> reskey) {

		hb.scanGloIni(tabname);
		String tmpRes = "";
		String[] res = new String[3];
		String[] qual = new String[3];

		qual[0] = "val";

		hb.scanMNext();

		while ((tmpRes = hb.scanR()) != "NO") {

			int len = tmpRes.length();

			for (int i = 0; i < len; i++) {
				if (tmpRes.charAt(i) == ':') {
					res[0] = tmpRes.substring(i + 1, len);
					break;
				}
			}
			if (st.compareTo(res[0]) <= 0 && ed.compareTo(res[0]) >= 0) {

				hb.scanM("attri", qual, 1, res);

				if (vl.compareTo(res[0]) <= 0 && vr.compareTo(res[0]) >= 0)
					reskey.add(tmpRes);

			} else if (ed.compareTo(res[0]) < 0) {
				break;
			}

			hb.scanMNext();
		}
		return;
	}

	public static void dmodRaw_fetch(ArrayList<String> reskey) {
		String[] res = new String[3];
		String[] qual = { "value" };
		for (int i = 0; i < reskey.size(); i++) {
			hb.get("tempRaw", reskey.get(i), "attri", qual, res);
		}
		return;
	}

	public static void dmodRaw_join(String tab1, String tab2,
			ArrayList<String> rkey) {
		// ArrayList<String> tpres) {
		hb.multiScanIni(tab1, 0);
		hb.multiScanIni(tab2, 1);
		String tmpr1 = "", tmpr2 = "";

		// "attri", "val"
		String[] qual = { "val" };
		String[] colv1 = new String[3];
		String[] colv2 = new String[3];

		hb.multiScanMNext(0);
		hb.multiScanMNext(1);

		while ((tmpr1 = hb.multiScanR(0)) != "NO"
				&& (tmpr2 = hb.multiScanR(1)) != "NO") {
			// dmod1_rowext(tmpr1, tint1);
			// dmod1_rowext(tmpr2, tint2);
			//
			// comst = dmod1_max(tint1[0], tint2[0]);
			// comed = dmod1_min(tint1[1], tint2[1]);

			// if (comst.compareTo(comed) <= 0) {
			hb.multiScanM("attri", qual, colv1, 0);
			hb.multiScanM("attri", qual, colv2, 1);

			double v1 = Double.parseDouble(colv1[0]);
			double v2 = Double.parseDouble(colv2[0]);

			if (Math.abs(v1 - 2.0 * v2) <= 1e-3) {
				rkey.add(tmpr1);
				rkey.add(tmpr2);
			}
			// }

			// if (tint1[1].compareTo(tint2[1]) < 0) {
			hb.multiScanMNext(0);
			// } else {
			hb.multiScanMNext(1);
			// }

		}
		return;
	}

	// ...........................Model2.......................................//

	public static String rkCon(int no) {
		int bn = (int) Math.log10(tsegn), bnt = (int) Math.log10(no);
		String res = "";
		while (bnt < bn) {
			res += "0";
			bnt++;
		}
		return res + Integer.toString(no);
	}

	public static void dmod2_qtp2search(String qt, String tabname,
			ArrayList<String> tpres) {

		String[] res = new String[3];
		String[] qual = { "tst", "ted" };

		int mid = 0, up = 1, dw = tsegn;

		while (up < dw - 1) {
			mid = up + (dw - up) / 2;

			String tmp = rkCon(mid);
			String row = "temp" + tmp;
			hb.get("tempM2", row, "attri", qual, res);

			if (qt.compareTo(res[0]) >= 0) {
				dw = mid;

				if (qt.compareTo(res[0]) >= 0 && qt.compareTo(res[1]) <= 0) {
					break;
				}

			} else {
				up = mid;
			}
		}
		int tar = 0;
		if (qt.compareTo(res[0]) >= 0 && qt.compareTo(res[1]) <= 0) {
			tar = mid;
			// tpres.add(res[0] + "," + res[1]);

		} else {
			if (dw == mid)
				tar = dw - 1;
			else
				tar = dw;
		}

		String row = "temp" + rkCon(tar);
		tpres.add(row);

		// String[] qualm = { "cof1" };
		// hb.get("tempM2", row, "model", qualm, res);
		return;
	}

	// sch 1: two nodes, 0: single node
	public static void dmod2_tIntSchPro(String qtl, String qtr, String tabname,
			int sno, int bno, int[] resls, int sch) {

		String[] res = new String[3];
		String[] qual = { "tst", "ted" };
		String row = "";

		if (sno == bno - 1) {
			row = "temp" + rkCon(sno);
			hb.get("tempM2", row, "attri", qual, res);

			// if (qtr.compareTo(res[0]) >= 0 && qtr.compareTo(res[1]) <= 0) {
			// resls[1] = sno;
			// }
			// if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {
			// resls[0] = sno;
			// }
			//
			// row = "temp" + rkCon(bno);
			// hb.get("tempM2", row, "attri", qual, res);
			// if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {
			// resls[0] = bno;
			// }
			// if (qtr.compareTo(res[0]) >= 0 && qtr.compareTo(res[1]) <= 0) {
			// resls[1] = bno;
			// }

			if (sch == 0) {
				if (qtr.compareTo(res[0]) >= 0 && qtr.compareTo(res[1]) <= 0) {
					resls[0] = sno;
				} else
					resls[0] = bno;

			} else if (sch == 1) {
				if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {
					resls[1] = sno;
				} else
					resls[1] = bno;
			} else {
				if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {
					resls[0] = sno;
					resls[1] = sno;
				} else if (qtr.compareTo(res[0]) >= 0
						&& qtr.compareTo(res[1]) <= 0) {
					resls[0] = sno;
					resls[1] = bno;
				} else {
					resls[0] = bno;
					resls[1] = bno;
				}

			}

			return;
		}

		int mid = 0;
		mid = sno + (bno - sno) / 2;

		row = "temp" + rkCon(mid);
		hb.get("tempM2", row, "attri", qual, res);

		int sigup = 0, sigdw = 0;
		if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {

			resls[1] = mid;
			sigdw = 1;
			if (sch != 2)
				return;
		}
		if (qtr.compareTo(res[0]) >= 0 && qtr.compareTo(res[1]) <= 0) {
			resls[0] = mid;
			sigup = 1;
			if (sch != 2)
				return;
		}

		if (sch == 2) {

			// if (sigup != 0 || sigdw != 0) {
			// if (sigup == 0)
			// dmod2_tIntSchPro(qtl, qtr, tabname, sno, mid, resls, 0);
			// if (sigdw == 0)
			// dmod2_tIntSchPro(qtl, qtr, tabname, mid, bno, resls, 1);
			// } else {
			// if (qtl.compareTo(res[1]) > 0)
			// dmod2_tIntSchPro(qtl, qtr, tabname, sno, mid, resls, sch);
			// else if (qtr.compareTo(res[0]) < 0)
			// dmod2_tIntSchPro(qtl, qtr, tabname, mid, bno, resls, sch);
			// }

			if (qtl.compareTo(res[1]) > 0)
				dmod2_tIntSchPro(qtl, qtr, tabname, sno, mid, resls, sch);
			else if (qtr.compareTo(res[0]) < 0)
				dmod2_tIntSchPro(qtl, qtr, tabname, mid, bno, resls, sch);

			else {
				if (sigup == 0)
					dmod2_tIntSchPro(qtl, qtr, tabname, sno, mid, resls, 0);
				if (sigdw == 0)
					dmod2_tIntSchPro(qtl, qtr, tabname, mid, bno, resls, 1);
			}
		} else if (sch == 1) {// 1 down
			if (qtl.compareTo(res[0]) <= 0)
				dmod2_tIntSchPro(qtl, qtr, tabname, mid, bno, resls, sch);
			else
				dmod2_tIntSchPro(qtl, qtr, tabname, sno, mid, resls, sch);

		} else {
			if (qtr.compareTo(res[1]) >= 0)
				dmod2_tIntSchPro(qtl, qtr, tabname, sno, mid, resls, sch);
			else
				dmod2_tIntSchPro(qtl, qtr, tabname, mid, bno, resls, sch);
		}
	}

	public static void dmod2_qtInt2Sch(String qtl, String qtr, String tabname,
			ArrayList<String> tires) {

		int up = 1, dw = tsegn;
		int[] resls = { 0, 0, 0 };

		dmod2_tIntSchPro(qtl, qtr, "tempM2", up, dw, resls, 2);

		int s = resls[0], e = resls[1];
		if (s > e) {
			s = s ^ e;
			e = s ^ e;
			s = s ^ e;
		}
		// ............range scan.......................//
		// String a=tires.get(0),b=tires.get(tisz-1);
		if (s == 0)
			s = e;

		tires.add(Integer.toString(s));
		tires.add(Integer.toString(e));

		// String tmpRes = "";
		// String[] res = new String[3];
		// String[] qual = new String[3];
		// qual[0] = "cof1";
		//
		// if (excMode == 1) {
		// for (int i = s; i <= e; i++) {
		//
		// tires.add("temp" + rkCon(i));
		// hb.get("tempM1", "temp" + rkCon(i), "model", qual, res);
		//
		// // System.out.printf("	%s", tires.get(i));
		// }
		// } else if (excMode == 0) {
		//
		// hb.scanIni("tempM2", "temp" + rkCon(s), "temp" + rkCon(e + 1));
		//
		// // String[] qualti = {"tst","ted"};
		//
		// hb.scanMNext();
		// while ((tmpRes = hb.scanM("model", qual, 1, res)) != "NO") {
		// // while ((tmpRes = hb.scanM("attri", qualti, 2, res)) != "NO")
		// // {
		// // System.out.print("	"+res[0]+","+res[1]);
		// hb.scanMNext();
		// }
		// }
		// System.out.printf("   %d",e-s+1);
		return;
	}

	public static void dmod2_vp2SchPro(String qv, String tabname, int[] bdres,
			int upper, int down) {

		// scanNextM(String cf, String[] qual, int qn, String[] res)

		String[] res = new String[3];
		String row = "";

		int upbd = 0, downbd = 0;
		int mid = 0, intres = down, up = upper, dw = down;// tsegn need to
															// change,
															// real number of
															// rows
															// in tempM2V
		char sig = 0;

		while (up < dw - 1) {
			mid = up + (dw - up) / 2;

			String[] qual = { "no" };
			row = rkCon(mid);
			hb.get("tempMap", row, "attri", qual, res);

			String[] qual1 = { "vl", "vr" };
			row = "temp" + res[0];
			hb.get("tempM2", row, "attri", qual1, res);

			if (qv.compareTo(res[0]) >= 0) { // down direction
				up = mid;

				if (qv.compareTo(res[0]) >= 0 && qv.compareTo(res[1]) <= 0
						&& sig == 0) {
					intres = mid;
					sig = 1;
				}

			} else // if
			{
				dw = mid;
			}
		}
		if (qv.compareTo(res[0]) >= 0 && qv.compareTo(res[1]) <= 0)
			downbd = mid;
		else {
			// int tmp = 0;
			if (dw == mid)
				downbd = dw - 1;
			else
				downbd = dw;

			// String[] qual = { "no" };
			// row = rkCon(tmp);
			// hb.get("tempMap", row, "attri", qual, res);
			//
			// String[] qual1 = { "vl", "vr" };
			// row = "temp" + res[0];
			// hb.get("tempM2", row, "attri", qual1, res);

			// hb.get("tempM2", "temp" + Integer.toString(tmp), "attri", qual,
			// res);

			// if (qv.compareTo(res[0]) >= 0 && qv.compareTo(res[1]) <= 0) // my
			// lose some precision
			// downbd = tmp;
		}

		// up direction
		up = 1;
		dw = intres;

		while (up < dw - 1) {
			// mid = (dw - up) >> 1;
			mid = up + (dw - up) / 2;
			// res[0] = "";
			// res[1] = "";

			String[] qual = { "no" };
			row = rkCon(mid);
			hb.get("tempMap", row, "attri", qual, res);

			String[] qual1 = { "vl", "vr" };
			row = "temp" + res[0];
			hb.get("tempM2", row, "attri", qual1, res);

			// if(res[0]==res[1])
			// {
			// mid=mid+2;
			// row = rkCon(mid);
			// hb.get("tempMap", row, "attri", qual, res);
			//
			// row = "temp" + res[0];
			// hb.get("tempM2", row, "attri", qual1, res);
			// }

			// if (qv.compareTo(res[1]) <= 0) { // may need improvement
			// if ( (qv.compareTo(res[0]) >= 0 && qv.compareTo(res[1]) <= 0) ||
			// (qv.compareTo(res[0]) <= 0))
			// {
			// dw = mid;
			// }
			// else if (qv.compareTo(res[1]) > 0)
			// {
			// up=mid;
			// }

			// if(qv.compareTo(res[0]) >= 0)

			if (qv.compareTo(res[1]) > 0) {
				up = mid;
			} else {
				dw = mid;
			}

		}

		if (qv.compareTo(res[0]) >= 0 && qv.compareTo(res[1]) <= 0)
			upbd = mid;
		else {
			// int tmp = 0;
			if (dw == mid)
				upbd = dw - 1;
			else
				upbd = dw;

			// String[] qual = { "no" };
			// row = rkCon(tmp);
			// hb.get("tempMap", row, "attri", qual, res);
			//
			// String[] qual1 = { "vl", "vr" };
			// row = "temp" + res[0];
			// hb.get("tempM2", row, "attri", qual1, res);

			// hb.get("tempM2", "temp" + Integer.toString(tmp), "attri", qual,
			// res);

			// if (qv.compareTo(res[0]) >= 0 && qv.compareTo(res[1]) <= 0)
			// upbd = tmp;
		}

		bdres[0] = upbd;
		bdres[1] = downbd;

		// String row = "temp" + Integer.toString(tar);
		// return getVal("tempM2", row, "model", "cof1");
		// return tar;
	}

	public static void dmod2_qvp2search(String qv, String tabname,
			ArrayList<String> vpres, int ceil, int bot) {// ceil: 1 bot:n
		// ArrayList<String> resls) {

		int[] bdres = { 0, 0, 0, 0 };
		dmod2_vp2SchPro(qv, tabname, bdres, ceil, bot);

		int s = bdres[0], e = bdres[1];
		if (s > e) {
			s = s ^ e;
			e = s ^ e;
			s = s ^ e;
		}
		if (s == 0)
			s = e;

		vpres.add(Integer.toString(s));
		vpres.add(Integer.toString(e));

		// hb.scanIni("tempMap", rkCon(s), rkCon(e));
		//
		// String tmpRes = "";
		// String[] res = new String[3];
		// String[] qual = new String[3];
		// qual[0] = "no";
		//
		// hb.scanMNext();
		// while ((tmpRes = hb.scanM("attri", qual, 1, res)) != "NO") {
		// // (tmpRes = hb.scanM("model", qual, 1, res)) != "NO") {
		//
		// // ...test...//
		// // String rw = "temp" + res[0];
		// // String[] qualv = { "vl", "vr" };
		// // hb.get("tempM2", rw, "attri", qualv, res);
		// // System.out.printf("		%s %s",res[0],res[1]);
		// //
		// hb.scanMNext();
		// }
		// ...test...//
		// System.out.printf("		number: %d",e-s+1);

		return;
	}

	public static void dmod2_vpGrasp(int no, String[] vrang) {
		String[] qual = { "no" };
		String[] res = new String[3];
		String row = rkCon(no);
		hb.get("tempMap", row, "attri", qual, res);

		String[] qual1 = { "vl", "vr" };
		row = "temp" + res[0];
		hb.get("tempM2", row, "attri", qual1, vrang);

	}

	public static void dmod2_vptGrasp(int no, String[] vrang) {
		String[] qual = { "no" };
		String[] res = new String[3];
		String row = rkCon(no);
		hb.get("tempMap", row, "attri", qual, res);

		String[] qual1 = { "tst", "ted" };
		row = "temp" + res[0];
		hb.get("tempM2", row, "attri", qual1, vrang);

	}

	public static void dmod2_vIntSchPro(String qtl, String qtr, String tabname,
			int sno, int bno, int[] resls, int sch) {
		// resls[0] small; resls[1] big
		String[] res = new String[3];

		int mid = 0;
		mid = sno + (bno - sno) / 2;

		if (sno == bno - 1) {

			dmod2_vpGrasp(sno, res);

			if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {
				resls[0] = sno;
			}
			if (qtr.compareTo(res[0]) >= 0 && qtr.compareTo(res[1]) <= 0) {
				resls[1] = sno;
			}

			dmod2_vpGrasp(bno, res);

			if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {
				resls[0] = bno;
			}
			if (qtr.compareTo(res[0]) >= 0 && qtr.compareTo(res[1]) <= 0) {
				resls[1] = bno;
			}

			return;
		}

		dmod2_vpGrasp(mid, res);

		if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {
			resls[0] = mid;//
			// if (sch != 2)
			// return;
		}
		if (qtr.compareTo(res[0]) >= 0 && qtr.compareTo(res[1]) <= 0) {
			resls[1] = mid;
			// if (sch != 2)
			// return;
		}

		if (sch == 2) {
			if (qtl.compareTo(res[1]) >= 0)
				dmod2_vIntSchPro(qtl, qtr, tabname, mid, bno, resls, sch);
			else if (qtr.compareTo(res[0]) <= 0)
				dmod2_vIntSchPro(qtl, qtr, tabname, sno, mid, resls, sch);

			else {
				// if (sch == 2) {
				if (resls[0] == 0)
					dmod2_vIntSchPro(qtl, qtr, tabname, sno, mid, resls, 0);
				if (resls[1] == 0)
					dmod2_vIntSchPro(qtl, qtr, tabname, mid, bno, resls, 1);// 1:
																			// down,
																			// 0:up
				// } else if (sch == 1) {
				// dmod2_vIntSchPro(qtl, qtr, tabname, mid, bno, resls, 1);
				// } else {
				// dmod2_vIntSchPro(qtl, qtr, tabname, sno, mid, resls, 0);
				// }
			}
		} else if (sch == 1) {
			if (qtr.compareTo(res[0]) >= 0)
				dmod2_vIntSchPro(qtl, qtr, tabname, mid, bno, resls, sch);
			else
				dmod2_vIntSchPro(qtl, qtr, tabname, sno, mid, resls, sch);

		} else {

			if (qtl.compareTo(res[1]) <= 0)
				dmod2_vIntSchPro(qtl, qtr, tabname, sno, mid, resls, sch);
			else
				dmod2_vIntSchPro(qtl, qtr, tabname, mid, bno, resls, sch);

			// if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0)
			// dmod2_vIntSchPro(qtl, qtr, tabname, sno, mid, resls, sch);
			// else
			// dmod2_vIntSchPro(qtl, qtr, tabname, mid, bno, resls, sch);
		}
	}

	public static void dmod2_vIntSchProNoMono(String qtl, String qtr,
			String tabname, int sno, int bno, int[] resls, int sch) {
		// resls[0] small; resls[1] big
		String[] res = new String[3];

		int mid = 0;
		mid = sno + (bno - sno) / 2;

		if (sno == bno - 1) {

			dmod2_vpGrasp(sno, res);

			if (sch == 1) {
				if (qtr.compareTo(res[0]) >= 0 && qtr.compareTo(res[1]) <= 0) {
					resls[1] = sno;
				} else {
					resls[1] = bno;
				}
			} else if (sch == 0) {
				if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {
					resls[0] = sno;
				} else {
					resls[0] = bno;
				}
			} else {
				resls[0] = sno;
				resls[1] = bno;
			}

			//

			// if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {
			// resls[0] = sno;
			// }
			// if (qtr.compareTo(res[0]) >= 0 && qtr.compareTo(res[1]) <= 0) {
			// resls[1] = sno;
			// }

			// dmod2_vpGrasp(bno, res);

			// if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {
			// resls[0] = bno;
			// }
			// if (qtr.compareTo(res[0]) >= 0 && qtr.compareTo(res[1]) <= 0) {
			// resls[1] = bno;
			// }

			return;
		}

		dmod2_vpGrasp(mid, res);

		if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {
			resls[0] = mid;//
			// if (sch != 2)
			// return;
		}
		if (qtr.compareTo(res[0]) >= 0 && qtr.compareTo(res[1]) <= 0) {
			resls[1] = mid;
			// if (sch != 2)
			// return;
		}

		if (sch == 2) {
			if (qtl.compareTo(res[1]) > 0)
				dmod2_vIntSchProNoMono(qtl, qtr, tabname, mid, bno, resls, sch);
			else if (qtr.compareTo(res[0]) < 0)
				dmod2_vIntSchProNoMono(qtl, qtr, tabname, sno, mid, resls, sch);

			else {
				// if (sch == 2) {
				if (resls[0] == 0)
					dmod2_vIntSchProNoMono(qtl, qtr, tabname, sno, mid, resls,
							0);
				if (resls[1] == 0)
					dmod2_vIntSchProNoMono(qtl, qtr, tabname, mid, bno, resls,
							1);// 1:
				// down,
				// 0:up
				// } else if (sch == 1) {
				// dmod2_vIntSchPro(qtl, qtr, tabname, mid, bno, resls, 1);
				// } else {
				// dmod2_vIntSchPro(qtl, qtr, tabname, sno, mid, resls, 0);
				// }
			}
		} else if (sch == 1) {
			if (qtr.compareTo(res[0]) >= 0)
				dmod2_vIntSchProNoMono(qtl, qtr, tabname, mid, bno, resls, sch);
			else
				dmod2_vIntSchProNoMono(qtl, qtr, tabname, sno, mid, resls, sch);

		} else {

			if (qtl.compareTo(res[1]) <= 0)
				dmod2_vIntSchProNoMono(qtl, qtr, tabname, sno, mid, resls, sch);
			else
				dmod2_vIntSchProNoMono(qtl, qtr, tabname, mid, bno, resls, sch);

			// if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0)
			// dmod2_vIntSchPro(qtl, qtr, tabname, sno, mid, resls, sch);
			// else
			// dmod2_vIntSchPro(qtl, qtr, tabname, mid, bno, resls, sch);
		}
	}

	public static void dmod2_qvInt2Sch(String qvl, String qvr, String tabname,
			ArrayList<String> vires, int ceil, int bot) {
		// String[] valres) {

		// int up = 0, dw = tsegn - 1;
		int up = ceil, dw = bot; // ordinal: from 1 to number of segments
		// ArrayList<Integer> resls = new ArrayList<Integer>();

		int[] resls = { 0, 0, 0 };
		dmod2_vIntSchProNoMono(qvl, qvr, tabname, up, dw, resls, 2);

		// String[] res = new String[3];

		int s = resls[0], e = resls[1];

		if (s > e) {
			int tmp = 0;
			tmp = s;
			s = e;
			e = tmp;
		}

		if (s == 0)
			s = e;

		vires.add(Integer.toString(s));
		vires.add(Integer.toString(e));

		// hb.scanIni("tempMap", rkCon(resls[0]), rkCon(resls[1] + 1));
		//
		// String tmpRes = "";
		// String[] qualm = { "cof1" };
		// String[] qual = { "no" };
		// String rw = "";
		//
		// hb.scanMNext();
		// while ((tmpRes = hb.scanM("attri", qual, 1, res)) != "NO") {
		//
		// // ...test...//
		// rw = "temp" + res[0];
		// // String[] qualv = { "vl", "vr" };
		// // String[] qual = { "cof1" };
		// // hb.get("tempM2", rw, "model", qualm, res);
		// // System.out.printf("		%s %s",res[0],res[1]);
		//
		// hb.scanMNext();
		// }

		// for(int i=0;i<tseqn;i++)
		// {
		// System.out.printf("%s  \n", tseq[i]);
		// }
		//
		// // Arrays.sort(tseq);
		//
		// String[] qual = { "tst","ted" };
		// String row="";
		// for(int i=0;i<tseqn;i++)
		// {
		// row="temp"+tseq[i];
		// hb.get("tempM2", row, "attri", qual, res);
		// System.out.printf("%s   %s \n", res[0], res[1]);
		// }

		return;
	}

	public static void dmod2_compq(String st, String ed, String vs, String vb,
			String tabname, ArrayList<String> comRes) {
		// String[] valres) {

		int up = 1, dw = tsegn;
		// ArrayList<Integer> resls = new ArrayList<Integer>();

		int[] resls = { 0, 0, 0 };

		dmod2_tIntSchPro(st, ed, "tempM2", up, dw, resls, 2);

		// int num = resls.size();
		int s = resls[0], e = resls[1];
		if (s > e) {
			s = s ^ e;
			e = s ^ e;
			s = s ^ e;
		}
		if (s == 0)
			s = e;

		int[] vresls = { 0, 0, 0 };
		up = 1;
		dw = mod2cnt;
		dmod2_vIntSchProNoMono(vs, vb, "tempM2", up, dw, vresls, 2);

		int vals = resls[0], vale = resls[1];

		if (vals > vale) {
			int tmp = 0;
			tmp = vals;
			vals = vale;
			vale = tmp;
		}

		if (vals == 0)
			vals = vale;

		// System.out.printf("   %d  %d",vresls[0],vresls[1]);

		hb.scanIni("tempMap", rkCon(vals), rkCon(vale + 1));
		String tmpRes = "";
		String[] qual = new String[3];
		String[] res = new String[3];
		qual[0] = "no";

		hb.scanMNext();
		while ((tmpRes = hb.scanM("attri", qual, 1, res)) != "NO") {
			//
			int tmp = Integer.valueOf(res[0]);
			if (tmp >= s && tmp <= e) {

				comRes.add("temp" + res[0]);
			}

			hb.scanMNext();
		}

	}

	public static void dmod2_compqVal(String st, String ed, String vs,
			String vb, String tabname, ArrayList<String> comRes) {
		// String[] valres) {

		int up = 1, dw = tsegn;
		// ArrayList<Integer> resls = new ArrayList<Integer>();

		int[] resls = { 0, 0, 0 };

		dmod2_tIntSchPro(st, ed, "tempM2", up, dw, resls, 2);

		// int num = resls.size();
		int s = resls[0], e = resls[1];
		if (s > e) {
			s = s ^ e;
			e = s ^ e;
			s = s ^ e;
		}

		int[] vresls = { 0, 0, 0 };

		dmod2_vIntSchPro(vs, vb, "tempM2", up, dw, vresls, 2);

		String[] res = new String[3];
		for (int i = vresls[0]; i <= vresls[1]; i++) {

			String[] qual = { "no" };
			String row = rkCon(i);
			hb.get("tempMap", row, "attri", qual, res);

			int tmp = Integer.valueOf(res[0]);

			if (tmp >= s && tmp <= e) {

				comRes.add("temp" + res[0]);
			}
			// // test
			// dmod2_vpGrasp(i, res);
			// System.out.printf("%s   %s\n ", res[0], res[1]);
		}
	}

	public static void dmod2_join(String tab1, String tab2,
			ArrayList<String> rkey) {

		hb.multiScanIni(tab1, 0);
		hb.multiScanIni(tab2, 1);
		String tmpr1 = "", tmpr2 = "";

		String[] tint1 = new String[3];
		String[] tint2 = new String[3];

		String comst = "", comed = "";

		String[] qual = { "tst", "ted" };
		String[] qualv = { "cof1" };
		String[] colv1 = new String[3];
		String[] colv2 = new String[3];

		hb.multiScanMNext(0);
		hb.multiScanMNext(1);

		while ((tmpr1 = hb.multiScanM("attri", qual, tint1, 0)) != "NO"
				&& (tmpr2 = hb.multiScanM("attri", qual, tint2, 1)) != "NO") {
			// dmod1_rowext(tmpr1, tint1);
			// dmod1_rowext(tmpr2, tint2);

			comst = dmod1_max(tint1[0], tint2[0]);
			comed = dmod1_min(tint1[1], tint2[1]);

			if (comst.compareTo(comed) <= 0) {
				hb.multiScanM("model", qualv, colv1, 0);
				hb.multiScanM("model", qualv, colv2, 1);

				double v1 = Double.parseDouble(colv1[0]);
				double v2 = Double.parseDouble(colv2[0]);

				if (Math.abs(v1 - 2.0 * v2) <= 1e-3) {
					rkey.add(tmpr1);
					rkey.add(tmpr2);
				}
			}

			if (tint1[0].compareTo(tint2[0]) >= 0) {
				hb.multiScanMNext(0);
			} else {
				hb.multiScanMNext(1);
			}
		}

		while ((tmpr1 = hb.multiScanM("attri", qual, tint1, 0)) != "NO") {
			// dmod1_rowext(tmpr1, tint1);
			// dmod1_rowext(tmpr2, tint2);

			comst = dmod1_max(tint1[0], tint2[0]);
			comed = dmod1_min(tint1[1], tint2[1]);

			if (comst.compareTo(comed) <= 0) {
				hb.multiScanM("model", qualv, colv1, 0);
				// hb.multiScanM("model", qual,colv2, 1);

				double v1 = Double.parseDouble(colv1[0]);
				double v2 = Double.parseDouble(colv2[0]);

				if (Math.abs(v1 - 2.0 * v2) <= 1e-3) {
					rkey.add(tmpr1);
					rkey.add(tmpr2);
				}
			}
			hb.multiScanMNext(0);
		}
		while ((tmpr2 = hb.multiScanM("attri", qual, tint2, 1)) != "NO") {
			// dmod1_rowext(tmpr1, tint1);
			// dmod1_rowext(tmpr2, tint2);

			comst = dmod1_max(tint1[0], tint2[0]);
			comed = dmod1_min(tint1[1], tint2[1]);

			if (comst.compareTo(comed) <= 0) {
				// hb.multiScanM("model", qual,colv1, 0);
				hb.multiScanM("model", qualv, colv2, 1);

				double v1 = Double.parseDouble(colv1[0]);
				double v2 = Double.parseDouble(colv2[0]);

				if (Math.abs(v1 - 2.0 * v2) <= 1e-3) {
					rkey.add(tmpr1);
					rkey.add(tmpr2);
				}
			}
			hb.multiScanMNext(1);
		}
		return;
	}

	// ...........................................Model1.................//

	public static void dmod1_idx(int base) {

		// String[] tst=new String[tsegn+10];
		// String[] ted=new String[tsegn+10];
		// double[] vl=new double[tsegn+10];
		// double[] vr=new double[tsegn+10];
		//
		// for(int i=0;i<tsegn;i++)
		// {
		// tst[i]=fio.tst[i];
		// ted[i]=fio.ted[i];
		// vl[i]=fio.vl[i];
		// vr[i]=fio.vr[i];
		// }

		// tidx.crtTTree();

		fio.segIni();
		tsegn = fio.seg2(0, segpre);
		tidx = new segbTree(base);
		vidx = new segbTree(base);

		tidx.crtTTree(fio.tst, fio.ted, tsegn);
		vidx.crtVTree(fio.tst, fio.ted, fio.vl, fio.vr, tsegn);
		return;
	}

	public static void dmod1_idxQtp(String tp, ArrayList<String> tires) {
		String[] qual = { "cof1" };
		String[] res = new String[3];
		int[] tpres = new int[10];

		String row = "temp" + "," + tidx.schTpTree(tp, tpres);
		hb.get("tempM1", row, "model", qual, res);

		tires.add(row);
		// ......test...........//
		// System.out.printf("index result\n%s\n",tidx.schTpTree(tp, tpres) );

		// hb.get("tempM1", row, "model", qual, res);

		return;
	}

	public static void dmod1_idxQti(String tis, String tie, ArrayList<String> ti) {
		ArrayList<String> tires = new ArrayList<String>();
		tidx.scanTIntTree(tis, tie, tires);

		String[] qual = { "cof1" };
		String tmpRes = "";
		String[] res = new String[3];
		int tmp = 0;

		for (int i = 0; i < tires.size(); i++) {

			ti.add("temp" + "," + tires.get(i));
			// hb.get("tempM1", ti.get(i), "model", qual, res);

			// System.out.printf("	%s", tires.get(i));
		}

		// System.out.printf("	%d", tires.size());

		return;
	}

	public static void dmod1_idxQtiIntScan(String tis, String tie,
			ArrayList<String> ti) {
		ArrayList<String> tires = new ArrayList<String>();
		tidx.scanTIntTree(tis, tie, tires);

		String[] qual = { "cof1" };
		String[] qualt = { "tst", "ted" };
		String[] res = new String[3];
		String tmpRes = "";

		// ............range scan.......................//
		// String a=tires.get(0),b=tires.get(tisz-1);
		int knum = tires.size();

		String stk = "temp," + tires.get(0);
		String etk = "temp," + tires.get(knum - 1);

		hb.scanIni("tempM1", stk, etk);

		hb.scanMNext();

		// while ((tmpRes = hb.scanM("attri", qualt, 2, res)) != "NO"){
		//
		// System.out.print("	"+res[0]+","+res[1]);
		// hb.scanMNext();
		// }

		while ((tmpRes = hb.scanM("model", qual, 1, res)) != "NO") {// ((tmpRes
																	// =
																	// hb.scanM("attri",
																	// qualt, 2,
																	// res)) !=
																	// "NO"){

			// System.out.print("	"+res[0]+","+res[1]);
			hb.scanMNext();
		}
		return;
	}

	public static void dmod1_idxQvp(String vp, ArrayList<String> vplist) {
		ArrayList<String> vpres = new ArrayList<String>();
		// vidx.schVpTree(vp, vpres);
		vidx.schVpTreeLowBd(vp, vpres);

		String[] qual = { "cof1" };
		String[] attqual = { "vl", "vr" };
		String[] res = new String[3];

		String[] tmpvp = new String[1000];
		int tmpvpcnt = 0;

		for (int i = 0; i < vpres.size(); i++) {
			String tkey = "";
			// ........extract time interval key.......//
			int st = 0;
			String curstr = vpres.get(i);
			for (int j = 0; j < curstr.length(); j++) {
				if (curstr.charAt(j) == ',') {
					if (st != 0) {
						tkey = curstr.substring(j + 1, curstr.length());

						// ...test...//
						// System.out.printf("		%s",curstr.substring(0,j));
						break;
					}
					st = j;
				}
			}
			// ........................................//
			vplist.add("temp" + "," + tkey);
			tmpvp[tmpvpcnt++] = "temp" + "," + tkey;
			// hb.get("tempM1", "temp" + "," + tkey, "attri", attqual, res);
			// hb.get("tempM1", "temp" + "," + tkey, "model", qual, res);
			// System.out.printf("    %s,%s", res[0],res[1]);
		}

		// java.util.Arrays.sort(tmpvp);
		// for(int i=0;i<tmpvpcnt;i++)
		// {
		//
		// }
		// System.out.printf("		number: %d",vpres.size());
		return;
	}

	public static void dmod1_idxQvi(String vismal, String vibig,
			ArrayList<String> vilst) {
		ArrayList<String> vires = new ArrayList<String>();
		vidx.scanVIntTreeLowBd(vismal, vibig, vires);

		String[] qual = { "cof1" };
		String[] qualval = { "vl", "vr" };
		String[] res = new String[3];

		for (int i = 0; i < vires.size(); i++) {
			String tkey = "";
			// ........extract time interval key.......//
			int st = 0;
			String curstr = vires.get(i);
			for (int j = 0; j < curstr.length(); j++) {
				if (curstr.charAt(j) == ',') {
					if (st != 0) {
						tkey = curstr.substring(j + 1, curstr.length());
						break;
					}
					st = j;
				}
			}
			// ........................................//

			vilst.add("temp" + "," + tkey);
			// tmpvp[tmpvpcnt++] = "temp" + "," + tkey;
			// hb.get("tempM1", "temp" + "," + tkey, "model", qual, res);
			// System.out.printf("	%s,%s",res[0],res[1]);
		}

		// System.out.printf("   %d",vires.size());
		return;
	}

	public static void dmod1_idxCompq(String st, String ed, String vs,
			String vb, String tabname) {

		ArrayList<String> tires = new ArrayList<String>();
		ArrayList<String> cmpres = new ArrayList<String>();
		tidx.scanTIntTree(st, ed, tires);

		String[] qual = { "vl", "vr" };
		String[] qualm = { "cof1" };
		String[] res = new String[3];

		for (int i = 0; i < tires.size(); i++) {

			hb.get("tempM1", "temp" + "," + tires.get(i), "attri", qual, res);

			if (vs.compareTo(res[1]) > 0 || vb.compareTo(res[0]) < 0) {

			} else {

				// hb.get("tempM1", "temp" + "," + tires.get(i), "model", qualm,
				// res);
				cmpres.add("temp" + "," + tires.get(i));
			}
		}
		// String[] qual1 = { "cof1" };
		//
		// for (int i = 0; i < cmpres.size(); i++) {
		// hb.get("tempM1", tires.get(i), "model", qual1, res);
		//
		// }
		return;
	}

	public static void dmod1_qtpSeq(String qt, String tabname,
			ArrayList<String> tires) {

		// scanNextM(String cf, String[] qual, int qn, String[] res)

		hb.scanGloIni(tabname);
		String tmpRes = "";
		String[] res = new String[3];
		String[] qual = new String[3];
		qual[0] = "tst";
		qual[1] = "ted";

		hb.scanMNext();

		String tmpqt = qt.substring(0, qt.length() - 2), tmpsec = qt.substring(
				qt.length() - 2, qt.length());
		tmpqt += "00";

		while ((tmpRes = hb.scanR()) != "NO") {

			int len = tmpRes.length(), strst = 0;

			for (int i = 0; i < len; i++) {
				if (tmpRes.charAt(i) == ',' && strst == 0) {
					strst = i + 1;
				} else if (tmpRes.charAt(i) == ',') {

					res[0] = tmpRes.substring(strst, i);
					res[1] = tmpRes.substring(i + 1, len);
					break;
				}
			}

			if (res[0].equals(res[1]) == false)
				tmpqt = qt;

			if (tmpqt.compareTo(res[0]) >= 0 && tmpqt.compareTo(res[1]) <= 0) {
				// if (qt.compareTo(res[0]) >= 0 && qt.compareTo(res[1]) <= 0) {

				tires.add(tmpRes);

				// qual[0] = "cof1";
				// res[0] = "";
				// hb.scanM("model", qual, 1, res);

				break;
			}
			hb.scanMNext();
		}
	}

	public static void dmod1_qvpSeq(String qv, String tabname,
			ArrayList<String> vpres, String[][] seg) {
		// ArrayList<String> resls) {

		// scanNextM(String cf, String[] qual, int qn, String[] res)

		hb.scanGloIni(tabname);
		String tmpRes = "";
		String[] res = new String[3];
		String[] qual = new String[3];
		qual[0] = "vl"; // small
		qual[1] = "vr"; // bigger
		String tmprow = "";

		int segcnt = 0, inseg = 0;

		hb.scanMNext();
		while ((tmpRes = hb.scanM("attri", qual, 2, res)) != "NO") {

			// System.out.printf("%s %s\n", res[0], res[1]);

			if (qv.compareTo(res[0]) >= 0 && qv.compareTo(res[1]) <= 0) {

				// if (excMode == 1) {
				// qual[0] = "cof1";
				// res[0] = "";
				// tmpRes = hb.scanM("model", qual, 1, res);
				// }
				tmprow = hb.scanR();
				vpres.add(tmprow);

				if (inseg == 1) {
					seg[segcnt][1] = tmprow;
				} else {
					seg[segcnt][0] = tmprow;
					seg[segcnt][1] = tmprow;
					inseg = 1;
				}
			} else if (inseg == 1) {
				segcnt++;
				inseg = 0;
			}
			hb.scanMNext();
		}
		seg[segcnt][0] = "-1";
		seg[segcnt][1] = "-1";

		// if (excMode == 0) {
		// String[] qualm = {"cof1"};
		// for (int i = 0; i < vpres.size(); i++) {
		//
		// // tires.add("temp" + rkCon(i));
		// hb.get("tempM1", vpres.get(i), "model", qualm, res);
		//
		// // System.out.printf("	%s", tires.get(i));
		// }
		// }

		// System.out.printf("		%d",cnt);
	}

	public static void dmod1_qtIntSeq(String qtl, String qtr, String tabname,
			ArrayList<String> tires) {

		// scanNextM(String cf, String[] qual, int qn, String[] res)

		hb.scanGloIni(tabname);
		String tmpRes = "";
		String[] res = new String[3];
		String[] qual = new String[3];
		qual[0] = "tst";
		qual[1] = "ted";

		hb.scanMNext();

		while ((tmpRes = hb.scanR()) != "NO") {

			int len = tmpRes.length(), strst = 0;

			for (int i = 0; i < len; i++) {
				if (tmpRes.charAt(i) == ',' && strst == 0) {
					strst = i + 1;
				} else if (tmpRes.charAt(i) == ',') {

					res[0] = tmpRes.substring(strst, i);
					res[1] = tmpRes.substring(i + 1, len);
				}
			}

			if (qtr.compareTo(res[0]) < 0) {
				break;
			} else if (qtl.compareTo(res[1]) > 0) {

			} else {

				tires.add(tmpRes);

				// if (excMode == 1) {
				// qual[0] = "cof1";
				// res[0] = "";
				// tmpRes = hb.scanM("model", qual, 1, res);
				// }

			}

			hb.scanMNext();
		}
		// valres[renum++] = "NO";

		// int tmp = tires.size();
		//
		// if (excMode == 0) {
		// hb.scanIni("tempM1", tires.get(0), tires.get(tmp - 1));
		// hb.scanMNext();
		//
		// while ((tmpRes = hb.scanM("model", qual, 1, res)) != "NO") {
		// hb.scanMNext();
		// }
		// }

		return;
	}

	public static void dmod1_qvIntSeq(String qvl, String qvr, String tabname,
			ArrayList<String> vires, String[][] seg) {
		// String[] valres) {

		hb.scanGloIni(tabname);
		String tmpRes = "";
		String[] res = new String[3];
		String[] qual = new String[3];
		qual[0] = "vl";
		qual[1] = "vr";
		String tmprow = "";

		hb.scanMNext();
		int inseg = 0, segcnt = 0;

		while ((tmpRes = hb.scanM("attri", qual, 2, res)) != "NO") {

			if (qvr.compareTo(res[0]) < 0 || qvl.compareTo(res[1]) > 0) {

				if (inseg == 1) {
					segcnt++;
					inseg = 0;
				}

			} else {
				vires.add(hb.scanR());
				tmprow = hb.scanR();

				if (inseg == 1) {
					seg[segcnt][1] = tmprow;
				} else {
					seg[segcnt][0] = tmprow;
					seg[segcnt][1] = tmprow;
					inseg = 1;
				}
			}
			hb.scanMNext();
		}
		// System.out.printf("   %d",vires.size());
		// valres[renum++] = "NO";
		return;
	}

	public static void dmod1_compq(String st, String ed, String vs, String vb,
			String tabname, ArrayList<String> rkey) {

		hb.scanGloIni(tabname);
		String tmpRes = "";
		String[] res = new String[3];
		String[] qual = new String[3];
		qual[0] = "tst";
		qual[1] = "ted";
		hb.scanMNext();

		while ((tmpRes = hb.scanR()) != "NO") {

			int len = tmpRes.length(), strst = 0;

			for (int i = 0; i < len; i++) {
				if (tmpRes.charAt(i) == ',' && strst == 0) {
					strst = i + 1;
				} else if (tmpRes.charAt(i) == ',') {

					res[0] = tmpRes.substring(strst, i);
					res[1] = tmpRes.substring(i + 1, len);
				}
			}
			if (ed.compareTo(res[0]) < 0) {
				break;
			} else if (st.compareTo(res[1]) > 0) {

			} else {
				qual[0] = "vl";
				qual[1] = "vr";
				hb.scanM("attri", qual, 2, res);

				if (vs.compareTo(res[1]) > 0 || vb.compareTo(res[0]) < 0) {

				} else {

					// qual[0] = "cof1";
					// hb.scanM("model", qual, 1, res);
					rkey.add(tmpRes);
				}
			}
			hb.scanMNext();
		}
		return;
	}

	public static void dmod1_fetch(String tabname, ArrayList<String> rkey) {
		String[] res = new String[3];
		String[] qual = { "cof1" };

		for (int i = 0; i < rkey.size(); i++) {
			hb.get("tempM1", rkey.get(i), "model", qual, res);
		}

		// System.out.printf("%s  %s", res[0],res[1]);
	}

	public static void dmod1_rowext(String row, String[] tint) {
		int len = row.length(), strst = 0;

		for (int i = 0; i < len; i++) {
			if (row.charAt(i) == ',' && strst == 0) {
				strst = i + 1;
			} else if (row.charAt(i) == ',') {

				tint[0] = row.substring(strst, i);
				tint[1] = row.substring(i + 1, len);
			}
		}
	}

	public static String dmod1_max(String a, String b) {
		if (a.compareTo(b) >= 0)
			return a;
		else
			return b;
	}

	public static String dmod1_min(String a, String b) {
		if (a.compareTo(b) >= 0)
			return b;
		else
			return a;
	}

	public static void dmod1_join(String tab1, String tab2,
			ArrayList<String> rkey) {

		hb.multiScanIni(tab1, 0);
		hb.multiScanIni(tab2, 1);
		String tmpr1 = "", tmpr2 = "";

		String[] tint1 = new String[3];
		String[] tint2 = new String[3];

		String comst = "", comed = "";

		String[] qual = { "cof1" };
		String[] colv1 = new String[3];
		String[] colv2 = new String[3];

		hb.multiScanMNext(0);
		hb.multiScanMNext(1);

		while ((tmpr1 = hb.multiScanR(0)) != "NO"
				&& (tmpr2 = hb.multiScanR(1)) != "NO") {
			dmod1_rowext(tmpr1, tint1);
			dmod1_rowext(tmpr2, tint2);

			comst = dmod1_max(tint1[0], tint2[0]);
			comed = dmod1_min(tint1[1], tint2[1]);

			if (comst.compareTo(comed) <= 0) {
				hb.multiScanM("model", qual, colv1, 0);
				hb.multiScanM("model", qual, colv2, 1);

				double v1 = Double.parseDouble(colv1[0]);
				double v2 = Double.parseDouble(colv2[0]);

				if (Math.abs(v1 - 2.0 * v2) <= 1e-3) {
					rkey.add(tmpr1);
					rkey.add(tmpr2);
				}
			}

			if (tint1[1].compareTo(tint2[1]) < 0) {
				hb.multiScanMNext(0);
			} else {
				hb.multiScanMNext(1);
			}
		}

		while ((tmpr1 = hb.multiScanR(0)) != "NO") {
			dmod1_rowext(tmpr1, tint1);
			// dmod1_rowext(tmpr2, tint2);

			comst = dmod1_max(tint1[0], tint2[0]);
			comed = dmod1_min(tint1[1], tint2[1]);

			if (comst.compareTo(comed) <= 0) {
				hb.multiScanM("model", qual, colv1, 0);
				// hb.multiScanM("model", qual,colv2, 1);

				double v1 = Double.parseDouble(colv1[0]);
				double v2 = Double.parseDouble(colv2[0]);

				if (Math.abs(v1 - 2.0 * v2) <= 1e-3) {
					rkey.add(tmpr1);
					rkey.add(tmpr2);
				}
			}
			hb.multiScanMNext(0);
		}
		while ((tmpr2 = hb.multiScanR(1)) != "NO") {
			// dmod1_rowext(tmpr1, tint1);
			dmod1_rowext(tmpr2, tint2);

			comst = dmod1_max(tint1[0], tint2[0]);
			comed = dmod1_min(tint1[1], tint2[1]);

			if (comst.compareTo(comed) <= 0) {
				// hb.multiScanM("model", qual,colv1, 0);
				hb.multiScanM("model", qual, colv2, 1);

				double v1 = Double.parseDouble(colv1[0]);
				double v2 = Double.parseDouble(colv2[0]);

				if (Math.abs(v1 - 2.0 * v2) <= 1e-3) {
					rkey.add(tmpr1);
					rkey.add(tmpr2);
				}
			}
			hb.multiScanMNext(1);
		}
		return;
	}

	// .............cache model2.................//

	public static void dmod2_qtp2searchCache(String qt, String tabname,
			ArrayList<String> tpres) {

		hb.cacheIni();

		String[] res = new String[3];
		String[] qual = { "tst", "ted" };

		int mid = 0, up = 1, dw = tsegn;

		while (up < dw - 1) {
			mid = up + (dw - up) / 2;

			String tmp = rkCon(mid);
			String row = "temp" + tmp;
			hb.cacheGetPut("tempM2", row, "attri", qual, res);

			if (qt.compareTo(res[0]) >= 0) {
				dw = mid;

				if (qt.compareTo(res[0]) >= 0 && qt.compareTo(res[1]) <= 0) {
					break;
				}

			} else {
				up = mid;
			}
		}
		int tar = 0;
		if (qt.compareTo(res[0]) >= 0 && qt.compareTo(res[1]) <= 0) {
			tar = mid;
			// tpres.add(res[0] + "," + res[1]);

		} else {
			if (dw == mid)
				tar = dw - 1;
			else
				tar = dw;
		}

		String row = "temp" + rkCon(tar);
		tpres.add(row);

		// String[] qualm = { "cof1" };
		//
		// hb.cacheGet("tempM2", row, "model", qualm, res);

		return;
	}

	// sch 1: two nodes, 0: single node
	public static void dmod2_tIntSchProCache(String qtl, String qtr,
			String tabname, int sno, int bno, int[] resls, int sch) {

		String[] res = new String[3];
		String[] qual = { "tst", "ted" };
		String row = "";

		if (sno == bno - 1) {
			row = "temp" + rkCon(sno);
			hb.cacheGet("tempM2", row, "attri", qual, res);

			// if (qtr.compareTo(res[0]) >= 0 && qtr.compareTo(res[1]) <= 0) {
			// resls[1] = sno;
			// }
			// if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {
			// resls[0] = sno;
			// }
			//
			// row = "temp" + rkCon(bno);
			// hb.get("tempM2", row, "attri", qual, res);
			// if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {
			// resls[0] = bno;
			// }
			// if (qtr.compareTo(res[0]) >= 0 && qtr.compareTo(res[1]) <= 0) {
			// resls[1] = bno;
			// }

			if (sch == 0) {
				if (qtr.compareTo(res[0]) >= 0 && qtr.compareTo(res[1]) <= 0) {
					resls[0] = sno;
				} else
					resls[0] = bno;

			} else if (sch == 1) {
				if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {
					resls[1] = sno;
				} else
					resls[1] = bno;
			} else {
				if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {
					resls[0] = sno;
					resls[1] = sno;
				} else if (qtr.compareTo(res[0]) >= 0
						&& qtr.compareTo(res[1]) <= 0) {
					resls[0] = sno;
					resls[1] = bno;
				} else {
					resls[0] = bno;
					resls[1] = bno;
				}
			}
			return;
		}

		int mid = 0;
		mid = sno + (bno - sno) / 2;

		row = "temp" + rkCon(mid);
		hb.cacheGetPut("tempM2", row, "attri", qual, res);

		int sigup = 0, sigdw = 0;
		if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {
			resls[0] = mid;
			sigdw = 1;
			if (sch != 2)
				return;
		}
		if (qtr.compareTo(res[0]) >= 0 && qtr.compareTo(res[1]) <= 0) {
			resls[1] = mid;
			sigup = 1;
			if (sch != 2)
				return;
		}

		if (sch == 2) {

			if (qtl.compareTo(res[1]) > 0)
				dmod2_tIntSchPro(qtl, qtr, tabname, sno, mid, resls, sch);
			else if (qtr.compareTo(res[0]) < 0)
				dmod2_tIntSchPro(qtl, qtr, tabname, mid, bno, resls, sch);

			else {
				if (sigup == 0)
					dmod2_tIntSchPro(qtl, qtr, tabname, sno, mid, resls, 0);
				if (sigdw == 0)
					dmod2_tIntSchPro(qtl, qtr, tabname, mid, bno, resls, 1);
			}
		} else if (sch == 1) {// 1 down
			if (qtl.compareTo(res[0]) <= 0)
				dmod2_tIntSchPro(qtl, qtr, tabname, mid, bno, resls, sch);
			else
				dmod2_tIntSchPro(qtl, qtr, tabname, sno, mid, resls, sch);

		} else {
			if (qtr.compareTo(res[1]) >= 0)
				dmod2_tIntSchPro(qtl, qtr, tabname, sno, mid, resls, sch);
			else
				dmod2_tIntSchPro(qtl, qtr, tabname, mid, bno, resls, sch);
		}
	}

	public static void dmod2_qtInt2SchCache(String qtl, String qtr,
			String tabname, ArrayList<String> tires) {

		int up = 1, dw = tsegn;
		int[] resls = { 0, 0, 0 };

		hb.cacheIni();

		dmod2_tIntSchProCache(qtl, qtr, "tempM2", up, dw, resls, 2);

		int s = resls[0], e = resls[1];
		if (s > e) {
			s = s ^ e;
			e = s ^ e;
			s = s ^ e;
		}
		if (s == 0)
			s = e;

		tires.add(Integer.toString(s));
		tires.add(Integer.toString(e));

		// ............range scan.......................//
		// String a=tires.get(0),b=tires.get(tisz-1);

		// String tmpRes = "";
		// String[] res = new String[3];
		// String[] qual = new String[3];
		// qual[0] = "cof1";

		// hb.scanIni("tempM2", "temp" + rkCon(s), "temp" + rkCon(e + 1));
		//
		// String tmpRes = "";
		// String[] res = new String[3];
		// String[] qual = new String[3];
		// qual[0] = "cof1";
		//
		// hb.scanMNext();
		//
		// for (int i = s; i < e + 1; i++) {
		// if (hb.cacheIsExist("temp" + rkCon(i)) == 1) {
		//
		// } else {
		// hb.scanM("model", qual, 1, res);
		// }
		// hb.scanMNext();
		// }

		// while ((tmpRes = hb.scanM("model", qual, 1, res)) != "NO") {
		// // (tmpRes = hb.scanM("model", qual, 1, res)) != "NO") {
		// // System.out.print("	"+res[0]+","+res[1]);
		// hb.scanMNext();
		// }
		// System.out.printf("   %d",e-s+1);
		return;
	}

	public static void dmod2_vp2SchProCache(String qv, String tabname,
			int[] bdres, int upper, int down) {

		// scanNextM(String cf, String[] qual, int qn, String[] res)

		String[] res = new String[3];
		String row = "";

		int upbd = 0, downbd = 0;
		int mid = 0, intres = down, up = upper, dw = down;// tsegn need to
															// change,
															// real number of
															// rows
															// in tempM2V
		char sig = 0;

		while (up < dw - 1) {
			mid = up + (dw - up) / 2;

			String[] qual = { "no" };
			row = rkCon(mid);
			hb.cacheGetPut("tempMap", row, "attri", qual, res);

			String[] qual1 = { "vl", "vr" };
			row = "temp" + res[0];
			hb.cacheGetPut("tempM2", row, "attri", qual1, res);

			if (qv.compareTo(res[0]) >= 0) { // down direction
				up = mid;

				if (qv.compareTo(res[0]) >= 0 && qv.compareTo(res[1]) <= 0
						&& sig == 0) {
					intres = mid;
					sig = 1;
				}

			} else // if
			{
				dw = mid;
			}
		}
		if (qv.compareTo(res[0]) >= 0 && qv.compareTo(res[1]) <= 0)
			downbd = mid;
		else {
			// int tmp = 0;
			if (dw == mid)
				downbd = dw - 1;
			else
				downbd = dw;

			// String[] qual = { "no" };
			// row = rkCon(tmp);
			// hb.get("tempMap", row, "attri", qual, res);
			//
			// String[] qual1 = { "vl", "vr" };
			// row = "temp" + res[0];
			// hb.get("tempM2", row, "attri", qual1, res);

			// hb.get("tempM2", "temp" + Integer.toString(tmp), "attri", qual,
			// res);

			// if (qv.compareTo(res[0]) >= 0 && qv.compareTo(res[1]) <= 0) // my
			// los some precision
			// downbd = tmp;
		}

		// up direction
		up = 1;
		dw = intres;

		while (up < dw - 1) {
			// mid = (dw - up) >> 1;
			mid = up + (dw - up) / 2;
			// res[0] = "";
			// res[1] = "";

			String[] qual = { "no" };
			row = rkCon(mid);
			hb.cacheGetPut("tempMap", row, "attri", qual, res);

			String[] qual1 = { "vl", "vr" };
			row = "temp" + res[0];
			hb.cacheGetPut("tempM2", row, "attri", qual1, res);

			// if(res[0]==res[1])
			// {
			// mid=mid+2;
			// row = rkCon(mid);
			// hb.get("tempMap", row, "attri", qual, res);
			//
			// row = "temp" + res[0];
			// hb.get("tempM2", row, "attri", qual1, res);
			// }

			// if (qv.compareTo(res[1]) <= 0) { // may need improvement
			// if ( (qv.compareTo(res[0]) >= 0 && qv.compareTo(res[1]) <= 0) ||
			// (qv.compareTo(res[0]) <= 0))
			// {
			// dw = mid;
			// }
			// else if (qv.compareTo(res[1]) > 0)
			// {
			// up=mid;
			// }

			// if(qv.compareTo(res[0]) >= 0)

			if (qv.compareTo(res[1]) > 0) {
				up = mid;
			} else {
				dw = mid;
			}
		}

		if (qv.compareTo(res[0]) >= 0 && qv.compareTo(res[1]) <= 0)
			upbd = mid;
		else {
			// int tmp = 0;
			if (dw == mid)
				upbd = dw - 1;
			else
				upbd = dw;

			// String[] qual = { "no" };
			// row = rkCon(tmp);
			// hb.get("tempMap", row, "attri", qual, res);
			//
			// String[] qual1 = { "vl", "vr" };
			// row = "temp" + res[0];
			// hb.get("tempM2", row, "attri", qual1, res);

			// hb.get("tempM2", "temp" + Integer.toString(tmp), "attri", qual,
			// res);

			// if (qv.compareTo(res[0]) >= 0 && qv.compareTo(res[1]) <= 0)
			// upbd = tmp;
		}

		bdres[0] = upbd;
		bdres[1] = downbd;

		// String row = "temp" + Integer.toString(tar);
		// return getVal("tempM2", row, "model", "cof1");
		// return tar;
	}

	public static void dmod2_qvp2searchCache(String qv, String tabname,
			ArrayList<String> vpres, int ceil, int bot) {// ceil: 1 bot:n
		// ArrayList<String> resls) {

		hb.cacheIni();

		int[] bdres = { 0, 0, 0, 0 };
		dmod2_vp2SchProCache(qv, tabname, bdres, ceil, bot);

		int s = bdres[0], e = bdres[1];
		if (s > e) {
			s = s ^ e;
			e = s ^ e;
			s = s ^ e;
		}
		if (s == 0)
			s = e;

		vpres.add(Integer.toString(s));
		vpres.add(Integer.toString(e));
		// hb.scanIni("tempMap", rkCon(s), rkCon(e));
		//
		// String tmpRes = "";
		// String[] res = new String[3];
		// String[] qual = new String[3];
		// qual[0] = "no";
		//
		// hb.scanMNext();
		// for (int i = s; i < e + 1; i++) {
		// if (hb.cacheIsExist(rkCon(i)) == 1) {
		//
		// } else {
		// hb.scanM("attri", qual, 1, res);
		// }
		// hb.scanMNext();
		// }

		// hb.scanMNext();
		// while ((tmpRes = hb.scanM("attri", qual, 1, res)) != "NO") {
		// // (tmpRes = hb.scanM("model", qual, 1, res)) != "NO") {
		//
		// // ...test...//
		// // String rw = "temp" + res[0];
		// // String[] qualv = { "vl", "vr" };
		// // hb.get("tempM2", rw, "attri", qualv, res);
		// // System.out.printf("		%s %s",res[0],res[1]);
		// //
		// hb.scanMNext();
		// }
		// ...test...//
		// System.out.printf("		number: %d",e-s+1);

		return;
	}

	public static void dmod2_vpGraspCache(int no, String[] vrang) {
		String[] qual = { "no" };
		String[] res = new String[3];
		String row = rkCon(no);
		hb.cacheGetPut("tempMap", row, "attri", qual, res);

		String[] qual1 = { "vl", "vr" };
		row = "temp" + res[0];
		hb.cacheGetPut("tempM2", row, "attri", qual1, vrang);

	}

	public static void dmod2_vptGraspCache(int no, String[] vrang) {
		String[] qual = { "no" };
		String[] res = new String[3];
		String row = rkCon(no);
		hb.cacheGetPut("tempMap", row, "attri", qual, res);

		String[] qual1 = { "tst", "ted" };
		row = "temp" + res[0];
		hb.cacheGetPut("tempM2", row, "attri", qual1, vrang);

	}

	public static void dmod2_vIntSchProCache(String qtl, String qtr,
			String tabname, int sno, int bno, int[] resls, int sch) {
		// resls[0] small; resls[1] big
		String[] res = new String[3];

		int mid = 0;
		mid = sno + (bno - sno) / 2;

		if (sno == bno - 1) {

			dmod2_vpGraspCache(sno, res);

			if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {
				resls[0] = sno;
			}
			if (qtr.compareTo(res[0]) >= 0 && qtr.compareTo(res[1]) <= 0) {
				resls[1] = sno;
			}

			dmod2_vpGraspCache(bno, res);

			if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {
				resls[0] = bno;
			}
			if (qtr.compareTo(res[0]) >= 0 && qtr.compareTo(res[1]) <= 0) {
				resls[1] = bno;
			}

			return;
		}

		dmod2_vpGraspCache(mid, res);

		if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {
			resls[0] = mid;//
			// if (sch != 2)
			// return;
		}
		if (qtr.compareTo(res[0]) >= 0 && qtr.compareTo(res[1]) <= 0) {
			resls[1] = mid;
			// if (sch != 2)
			// return;
		}

		if (sch == 2) {
			if (qtl.compareTo(res[1]) >= 0)
				dmod2_vIntSchPro(qtl, qtr, tabname, mid, bno, resls, sch);
			else if (qtr.compareTo(res[0]) <= 0)
				dmod2_vIntSchPro(qtl, qtr, tabname, sno, mid, resls, sch);

			else {
				// if (sch == 2) {
				if (resls[0] == 0)
					dmod2_vIntSchPro(qtl, qtr, tabname, sno, mid, resls, 0);
				if (resls[1] == 0)
					dmod2_vIntSchPro(qtl, qtr, tabname, mid, bno, resls, 1);// 1:
																			// down,
																			// 0:up
				// } else if (sch == 1) {
				// dmod2_vIntSchPro(qtl, qtr, tabname, mid, bno, resls, 1);
				// } else {
				// dmod2_vIntSchPro(qtl, qtr, tabname, sno, mid, resls, 0);
				// }
			}
		} else if (sch == 1) {
			if (qtr.compareTo(res[0]) >= 0)
				dmod2_vIntSchPro(qtl, qtr, tabname, mid, bno, resls, sch);
			else
				dmod2_vIntSchPro(qtl, qtr, tabname, sno, mid, resls, sch);

		} else {

			if (qtl.compareTo(res[1]) <= 0)
				dmod2_vIntSchPro(qtl, qtr, tabname, sno, mid, resls, sch);
			else
				dmod2_vIntSchPro(qtl, qtr, tabname, mid, bno, resls, sch);

			// if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0)
			// dmod2_vIntSchPro(qtl, qtr, tabname, sno, mid, resls, sch);
			// else
			// dmod2_vIntSchPro(qtl, qtr, tabname, mid, bno, resls, sch);
		}
	}

	public static void dmod2_vIntSchProNoMonoCache(String qtl, String qtr,
			String tabname, int sno, int bno, int[] resls, int sch) {
		// resls[0] small; resls[1] big
		String[] res = new String[3];

		String[] qual = { "no" };
		String[] qual1 = { "vl", "vr" };

		int mid = 0;
		mid = sno + (bno - sno) / 2;

		if (sno == bno - 1) {

			String row = rkCon(sno);
			hb.cacheGet("tempMap", row, "attri", qual, res);

			row = "temp" + res[0];
			hb.cacheGet("tempM2", row, "attri", qual1, res);

			if (sch == 1) {
				if (qtr.compareTo(res[0]) >= 0 && qtr.compareTo(res[1]) <= 0) {
					resls[1] = sno;
				} else {
					resls[1] = bno;
				}
			} else if (sch == 0) {
				if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {
					resls[0] = sno;
				} else {
					resls[0] = bno;
				}
			} else {
				resls[0] = sno;
				resls[1] = bno;
			}

			//

			// if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {
			// resls[0] = sno;
			// }
			// if (qtr.compareTo(res[0]) >= 0 && qtr.compareTo(res[1]) <= 0) {
			// resls[1] = sno;
			// }

			// dmod2_vpGrasp(bno, res);

			// if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {
			// resls[0] = bno;
			// }
			// if (qtr.compareTo(res[0]) >= 0 && qtr.compareTo(res[1]) <= 0) {
			// resls[1] = bno;
			// }

			return;
		}

		dmod2_vpGraspCache(mid, res);

		if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0) {
			resls[0] = mid;//
			// if (sch != 2)
			// return;
		}
		if (qtr.compareTo(res[0]) >= 0 && qtr.compareTo(res[1]) <= 0) {
			resls[1] = mid;
			// if (sch != 2)
			// return;
		}

		if (sch == 2) {
			if (qtl.compareTo(res[1]) >= 0)
				dmod2_vIntSchProNoMono(qtl, qtr, tabname, mid, bno, resls, sch);
			else if (qtr.compareTo(res[0]) <= 0)
				dmod2_vIntSchProNoMono(qtl, qtr, tabname, sno, mid, resls, sch);

			else {
				// if (sch == 2) {
				if (resls[0] == 0)
					dmod2_vIntSchProNoMono(qtl, qtr, tabname, sno, mid, resls,
							0);
				if (resls[1] == 0)
					dmod2_vIntSchProNoMono(qtl, qtr, tabname, mid, bno, resls,
							1);// 1:
				// down,
				// 0:up
				// } else if (sch == 1) {
				// dmod2_vIntSchPro(qtl, qtr, tabname, mid, bno, resls, 1);
				// } else {
				// dmod2_vIntSchPro(qtl, qtr, tabname, sno, mid, resls, 0);
				// }
			}
		} else if (sch == 1) {
			if (qtr.compareTo(res[0]) >= 0)
				dmod2_vIntSchProNoMono(qtl, qtr, tabname, mid, bno, resls, sch);
			else
				dmod2_vIntSchProNoMono(qtl, qtr, tabname, sno, mid, resls, sch);

		} else {

			if (qtl.compareTo(res[1]) <= 0)
				dmod2_vIntSchProNoMono(qtl, qtr, tabname, sno, mid, resls, sch);
			else
				dmod2_vIntSchProNoMono(qtl, qtr, tabname, mid, bno, resls, sch);

			// if (qtl.compareTo(res[0]) >= 0 && qtl.compareTo(res[1]) <= 0)
			// dmod2_vIntSchPro(qtl, qtr, tabname, sno, mid, resls, sch);
			// else
			// dmod2_vIntSchPro(qtl, qtr, tabname, mid, bno, resls, sch);
		}
	}

	public static void dmod2_qvInt2SchCache(String qvl, String qvr,
			String tabname, ArrayList<String> vires, int ceil, int bot) {
		// String[] valres) {

		// int up = 0, dw = tsegn - 1;
		int up = ceil, dw = bot; // ordinal: from 1 to number of segments
		// ArrayList<Integer> resls = new ArrayList<Integer>();

		hb.cacheIni();

		int[] resls = { 0, 0, 0 };
		dmod2_vIntSchProNoMonoCache(qvl, qvr, tabname, up, dw, resls, 2);

		int s = resls[0], e = resls[1];

		if (s > e) {
			int tmp = 0;
			tmp = s;
			s = e;
			e = tmp;
		}
		if (s == 0)
			s = e;

		vires.add(Integer.toString(s));
		vires.add(Integer.toString(e));
		// hb.scanIni("tempMap", rkCon(s), rkCon(e + 1));
		//
		// String[] res = new String[3];
		// String tmpRes = "";
		// String[] qualm = { "cof1" };
		// String[] qual = { "no" };
		//
		// hb.scanMNext();
		//
		// for (int i = s; i < e; i++) {
		// if (hb.cacheIsExist(rkCon(i)) == 1) {
		//
		// } else {
		// hb.scanM("attri", qual, 1, res);
		// }
		// hb.scanMNext();
		// }

		// hb.scanMNext();
		// while ((tmpRes = hb.scanM("attri", qual, 1, res)) != "NO") {
		//
		// // ...test...//
		// rw = "temp" + res[0];
		// // String[] qualv = { "vl", "vr" };
		// // String[] qual = { "cof1" };
		// hb.get("tempM2", rw, "model", qualm, res);
		// // System.out.printf("		%s %s",res[0],res[1]);
		//
		// hb.scanMNext();
		// }

		return;
	}

	// ........................Model 2 no common path
	// optimization(NCPO).............................................//

	// ...........key fetching phase......................//

	public static String dateGen() {
		// year month day hour minute second
		// 0: min 1:max

		int len = fio.lastT.length();
		// fio.oldT
		int st = 0, tmp = 0;
		for (int i = 0; i < len; ++i) {
			if (fio.lastT.charAt(i) == '-' || fio.lastT.charAt(i) == ':'
					|| fio.lastT.charAt(i) == 'T') {

				date[tmp][0] = Integer.parseInt(fio.oldT.substring(st, i));
				date[tmp++][1] = Integer.parseInt(fio.lastT.substring(st, i));
				st = i + 1;
			}
		}

		String res = "";
		double tmpr = 0.0;
		int interv = 0;
		int mon = 0;
		// year
		interv = (date[0][1] - date[0][0] + 1);
		tmpr = Math.random();
		tmp = (int) (tmpr * (interv) + date[0][0]);

		res = res + Integer.toString(tmp) + "-";

		// mon + day
		interv = (date[1][1] - date[1][0]) * 31 - date[2][0] + date[2][1] + 1;
		tmpr = Math.random();// 0~1
		tmp = (int) (tmpr * (interv));
		// res = res + (Integer.toString(tmp));

		if (tmp + date[2][0] > 31) {
			mon = (int) ((tmp - (31 - date[2][0])) / 32) + date[1][0] + 1;
		} else {
			mon = date[1][0];
		}

		if (Math.log10(mon) < 1) {
			res = res + "0" + Integer.toString(mon) + "-";
		} else {
			res = res + Integer.toString(mon) + "-";
		}

		int day = 0;
		tmp += date[2][0];

		if (tmp > 31)
			day = (int) (tmp % 31);
		else
			day = tmp;
		if (Math.log10(day) < 1) {
			res = res + "0" + Integer.toString(day) + "T";
		} else {
			res = res + Integer.toString(day) + "T";
		}
		return res;
	}

	public static String tGen(int secEx) {

		int len = fio.lastT.length();
		// fio.oldT
		int st = 0, tmp = 0;
		for (int i = 0; i < len; ++i) {
			if (fio.lastT.charAt(i) == '-' || fio.lastT.charAt(i) == ':'
					|| fio.lastT.charAt(i) == 'T') {

				date[tmp][0] = Integer.parseInt(fio.oldT.substring(st, i));
				date[tmp++][1] = Integer.parseInt(fio.lastT.substring(st, i));
				st = i + 1;
			}
		}

		String res = "";
		double tmpr = 0.0;
		int interv = 0;
		int mon = 0;
		// year
		interv = (date[0][1] - date[0][0] + 1);
		tmpr = Math.random();
		tmp = (int) (tmpr * (interv) + date[0][0]);

		res = res + Integer.toString(tmp) + "-";

		// mon + day
		interv = (date[1][1] - date[1][0]) * 31 - date[2][0] + date[2][1] + 1;
		tmpr = Math.random();// 0~1
		tmp = (int) (tmpr * (interv));
		// res = res + (Integer.toString(tmp));

		if (tmp + date[2][0] > 31) {
			mon = (int) ((tmp - (31 - date[2][0])) / 32) + date[1][0] + 1;
		} else {
			mon = date[1][0];
		}

		if (Math.log10(mon) < 1) {
			res = res + "0" + Integer.toString(mon) + "-";
		} else {
			res = res + Integer.toString(mon) + "-";
		}

		int day = 0;
		tmp += date[2][0];

		if (tmp > 31)
			day = (int) (tmp % 31);
		else
			day = tmp;
		if (Math.log10(day) < 1) {
			res = res + "0" + Integer.toString(day) + "T";
		} else {
			res = res + Integer.toString(day) + "T";
		}
		// hour

		int tmph = 0, tmpmin = 0, tmpsec = 0;

		if (day == date[2][0]) {
			while ((tmph = (int) (Math.random() * 24)) < date[3][0]) {

			}
			if (tmph == date[3][0]) {
				while ((tmpmin = (int) (Math.random() * 60)) < date[4][0]) {

				}
			} else
				tmpmin = (int) (Math.random() * 60);
		}

		else if (day == date[2][1]) {
			while ((tmph = (int) (Math.random() * 24)) > date[3][1]) {

			}

			if (tmph == date[3][1]) {
				while ((tmpmin = (int) (Math.random() * 60)) >= date[4][1]) {

				}
			} else
				tmpmin = (int) (Math.random() * 60);

		} else {

			tmpmin = (int) (Math.random() * 60);
			tmph = (int) (Math.random() * 24);
		}
		tmpsec = (int) (Math.random() * 60);

		if (Math.log10(tmph) < 1)
			res += ("0" + Integer.toString(tmph) + ":");
		else
			res += (Integer.toString(tmph) + ":");
		if (Math.log10(tmpmin) < 1)
			res += ("0" + Integer.toString(tmpmin) + ":");
		else
			res += (Integer.toString(tmpmin) + ":");

		if (secEx == 1) {
			if (Math.log10(tmpsec) < 1)
				res += ("0" + Integer.toString(tmpsec));
			else
				res += (Integer.toString(tmpsec));
		} else {
			res += "00";
		}

		return res;
	}

	public static String vGen(int tsn) {
		double minv = Double.parseDouble(fio.tsRange[tsn][0]), maxv = Double
				.parseDouble(fio.tsRange[tsn][1]);
		return Double.toString((maxv - minv) * Math.random() + minv);
	}

	// ....................main function...............................//

	public static void rawDmTest() {
		// ......test query generation...........//

		String expt = "", expv = "";
		expt = tGen(1);
		expv = vGen(0);

		String tnear = "", tfar = "";
		while ((tnear = tGen(0)).compareTo(tfar = tGen(0)) <= 0) {
		}

		String vsmal = "", vbig = "";
		while ((vbig = vGen(0)).compareTo(vsmal = vGen(0)) <= 0) {
		}

		// ........................................//

		// .............RawData Model test..................//

		System.out.printf("%s  %s\n", expt, expv);

		// System.out.printf("value:   %s\n", dmodRaw_qtp(expt, "tempRaw"));

		// System.out.printf("%s  %s\n", tfar, tnear);
		// System.out.printf("Interval Points:   %d\n",
		// dmodRaw_qtInt(tfar, tnear, "tempRawM"));

		// System.out.printf("%s  %s\n", vsmal, vbig);
		// ArrayList<String> viseg = new ArrayList<String>();
		// dmodRaw_qvInt(vsmal, vbig, "tempRaw");
		// Iterator it = viseg.iterator();
		// while (it.hasNext()) {
		// System.out.printf("Interval: %s\n", it.next());
		// }

		// ArrayList<String> tpres = new ArrayList<String>();
		// dmodRaw_qvp(expv, "tempRaw");
		// System.out.printf("Time Points:\n");
		// it = tpres.iterator();
		// while (it.hasNext()) {
		// System.out.printf("%s\n", it.next());
		// }
		// System.out.printf("\n");

	}

	public static void dmodel1Test() {
		// ......test query generation...........//

		String expt = "", expv = "";
		// expt = tGen2(6);
		expv = vGen(0);

		String tnear = "", tfar = "";
		while ((tnear = tGen2(6)).compareTo(tfar = tGen2(6)) <= 0) {
		}

		String vsmal = "", vbig = "";
		while ((vbig = vGen(0)).compareTo(vsmal = vGen(0)) <= 0) {
		}

		// ........................................//

		// .............Data Model1 test..................//

		// expt="2012-05-31T18:03:52";

		// ...............tp test.............//
		int num = 50;
		// while (num > 0) {
		// expt = tGen2(6);
		// ArrayList<String> res = new ArrayList<String>();
		//
		// System.out.printf("%s  \n", expt);
		//
		// dmod1_qtpSeq(expt, "tempM1", res);
		// System.out.printf("%s  ", res.get(0));
		//
		// res.clear();
		// dmod1_idxQtp(expt, res);
		// System.out.printf("%s\n", res.get(0));
		//
		// num--;
		// }
		// .............ti test..................//
		// num = 15;
		// while (num > 0) {
		// expt = tGen2(6);
		// ArrayList<String> res = new ArrayList<String>();
		// tnear = "";
		// tfar = "";
		// while ((tnear = tGen2(6)).compareTo(tfar = tGen2(6)) <= 0) {
		// }
		// System.out.printf("%s  %s\n",tfar,tnear );
		// dmod1_qtIntSeq(tfar, tnear, "tempM1",res);
		// System.out.printf("%d\n", res.size());
		// res.clear();
		// dmod1_idxQti(tfar,tnear, res);
		// System.out.printf("%d\n", res.size());
		// num--;
		// }

		// .............vp test..................//
		// num = 20;
		// while (num > 0) {
		// expv = vGen(0);
		// expv="23.84519577756673 ";
		// ArrayList<String> res = new ArrayList<String>();
		//
		// System.out.printf("%s  \n", expv);
		//
		// dmod1_qvpSeq(expv, "tempM1", res);
		// System.out.printf("%d\n", res.size());
		//
		// res.clear();
		// dmod1_idxQvp(expv, res);
		// System.out.printf("%d\n", res.size());
		//
		// num--;
		// }

		// .............vi test..................//
		num = 10;
		while (num > 0) {

			while ((vbig = vGen(0)).compareTo(vsmal = vGen(0)) <= 0) {
			}

			ArrayList<String> res = new ArrayList<String>();

			// vsmal="19.983618712654987";
			// vbig=" 26.507202364664664";

			// vsmal="23.07903256800649"; vbig="25.91410197907109";
			// vsmal="19.48267881173787"; vbig="25.51256518709288";
			// vsmal="15.680412910095278"; vbig="26.9559748495069";

			// if(num==6)
			// {
			// vsmal="18.675886987960112";
			// vbig="25.876783813835544";
			// }
			// vsmal="15.026353406672937"; vbig="24.55758678316011;
			System.out.printf("%s   %s\n", vsmal, vbig);

			res.clear();
			// dmod1_qvIntSeq(vsmal, vbig, "tempM1", res);
			System.out.printf("%d\n", res.size());

			res.clear();
			dmod1_idxQvi(vsmal, vbig, res);
			System.out.printf("%d\n", res.size());

			num--;
		}

		// System.out.printf("%s  %s\n", tfar,tnear);
		// dmod1_qtIntSeq(tfar, tnear, "tempM1");

		// System.out.printf("%s\n", expv);
		// dmod1_qvpSeq(expv, "tempM1");
		//
		// System.out.printf("%s  %s\n", vsmal, vbig);
		// dmod1_qvIntSeq(vsmal, vbig, "tempM1");

	}

	public static void dmodel2Test() {

		// ......test query generation...........//

		String expt = "", expv = "";
		expt = tGen(1);
		expv = vGen(0);

		String tnear = "", tfar = "";
		while ((tnear = tGen(1)).compareTo(tfar = tGen(1)) <= 0) {
		}

		String vsmal = "", vbig = "";
		while ((vbig = vGen(0)).compareTo(vsmal = vGen(0)) <= 0) {
		}

		// .......unit test.....................//

		// System.out.printf("%s \n", expt);
		// dmod2_qtp2search(expt, "tempM2");

		// expv="26.268";
		// System.out.printf("%s \n", expv);
		// dmod2_qvp2search(expv, "tempM2");

		// 2012-05-31T19:52:46 2012-05-31T20:09:38
		// tfar = "2012-05-31T15:41:27";
		// tnear = "2012-05-31T16:13:18";

		// System.out.printf("Time interval: %s  %s\n", tfar, tnear);
		// dmod2_qtInt2Sch(tfar, tnear, "tempM2");
		//
		// vsmal="24.65709324409381";
		// vbig="25.65277533330979";

		System.out.printf("Value interval: %s  %s\n", vsmal, vbig);
		// dmod2_qvInt2Sch(vsmal, vbig, "tempM2");

	}

	public static void availExp() {

		// String expt = "", expv = "";
		// expt = tGen(0);
		// expv = vGen(0);
		//
		// // System.out.printf("%s\n",expt);
		//
		// int expnum = expn;
		// int[] sutCnt = new int[] { 0, 0, 0, 0, 0, 0, 0, 0 };
		// int suIdx = 0;
		//
		// while (expnum > 0) {
		// expt = tGen(1);
		// // System.out.printf("%s\n",expt);
		//
		// // ....time point....................//
		// String tmp = "";
		// // if ((dmodRaw_qtp(expt, "tempRawM")) != null)
		// // sutCnt[0]++;
		// //
		// // if (dmod1_qtpSeq(expt, "tempM1") == 1)
		// // sutCnt[1]++;
		// //
		// // if (dmod2_qtp2search(expt, "tempM2") == 1)
		// // sutCnt[2]++;
		//
		// // ....value point....................//
		// expv = vGen(0);
		// System.out.printf("%s\n", expv);
		//
		// // if ((dmodRaw_qvp(expv, "tempRawM")) == 1)
		// // sutCnt[3]++;
		// // // if (dmod1_qvpSeq(expv, "tempM1") == 1)
		// // // sutCnt[4]++;
		// // //if (dmod2_qvp2search(expv, "tempM2") == 1)
		// // sutCnt[5]++;
		// // else {
		// // expv = vGen(0);
		// }
		//
		// expnum--;
		// }
		// // System.out.printf("Time point: %f  %f   %f\n", (double) sutCnt[0]
		// // / (expn), (double) sutCnt[1] / (expn), (double) sutCnt[2]
		// // / (expn));
		// System.out.printf("Value point: %f  %f   %f\n", (double) sutCnt[3]
		// / (expn), (double) sutCnt[4] / (expn), (double) sutCnt[5]
		// / (expn));

	}

	public static String tGen2(int tsno) {

		// int even=fio.rawn/expn;
		// int inter=(int) (Math.random() * even);

		int t = (int) (Math.random() * fio.rawn);
		// t=expcur*even+t;

		int sec = (int) (Math.random() * 60);

		String tmp = fio.rawd[t][tsno].substring(0,
				fio.rawd[t][tsno].length() - 2);

		String tmp1 = "";
		if (sec < 10)
			tmp1 = "0" + Integer.toString(sec);
		else
			tmp1 = Integer.toString(sec);
		return tmp + tmp1;
	}

	public static String tGen3(int tsno, int expcur) {

		int even = fio.rawn / expn;
		// int inter=(int) (Math.random() * even);

		int t = (int) (Math.random() * even);
		t = (expcur - 1) * even + t;

		int sec = (int) (Math.random() * 60);

		String tmp = fio.rawd[t][tsno].substring(0,
				fio.rawd[t][tsno].length() - 2);

		String tmp1 = "";
		if (sec < 10)
			tmp1 = "0" + Integer.toString(sec);
		else
			tmp1 = Integer.toString(sec);
		return tmp + tmp1;
	}

	// ........................Model 2 no common path
	// optimization(NCPO).............................................//

	public static void dmod2_tIntSchProNCPO(String qt, String tabname, int sno,
			int bno, int[] resls) {

		String[] res = new String[3];
		String[] qual = { "tst", "ted" };

		int mid = 0, up = 1, dw = tsegn;

		while (up < dw - 1) {
			mid = up + (dw - up) / 2;

			String tmp = rkCon(mid);
			String row = "temp" + tmp;
			hb.get("tempM2", row, "attri", qual, res);

			if (qt.compareTo(res[0]) >= 0) {
				dw = mid;

				if (qt.compareTo(res[0]) >= 0 && qt.compareTo(res[1]) <= 0) {
					break;
				}

			} else {
				up = mid;
			}
		}
		int tar = 0;
		if (qt.compareTo(res[0]) >= 0 && qt.compareTo(res[1]) <= 0) {
			tar = mid;
			// tpres.add(res[0] + "," + res[1]);

		} else {
			if (dw == mid)
				tar = dw - 1;
			else
				tar = dw;
		}

		resls[0] = tar;

		return;
	}

	public static void dmod2_qtInt2SchNCPO(String qtl, String qtr,
			String tabname, ArrayList<String> tires) {

		int up = 1, dw = tsegn;
		int[] resls = { 0, 0 };
		int s = 0, e = 0;

		dmod2_tIntSchProNCPO(qtl, "tempM2", up, dw, resls);
		s = resls[0];
		dmod2_tIntSchProNCPO(qtr, "tempM2", up, dw, resls);
		e = resls[0];

		if (s > e) {
			s = s ^ e;
			e = s ^ e;
			s = s ^ e;
		}
		if (s == 0)
			s = e;

		tires.add(Integer.toString(s));
		tires.add(Integer.toString(e));
		// ............range scan.......................//
		// String a=tires.get(0),b=tires.get(tisz-1);

		// String tmpRes = "";
		// String[] res = new String[3];
		// String[] qual = new String[3];
		// qual[0] = "cof1";
		//
		// if (excMode == 1) {
		// for (int i = s; i <= e; i++) {
		//
		// tires.add("temp" + rkCon(i));
		// hb.get("tempM1", "temp" + rkCon(i), "model", qual, res);
		//
		// // System.out.printf("	%s", tires.get(i));
		// }
		// } else if (excMode == 0) {
		//
		// hb.scanIni("tempM2", "temp" + rkCon(s), "temp" + rkCon(e + 1));
		//
		// // String[] qualti = {"tst","ted"};
		//
		// hb.scanMNext();
		// while ((tmpRes = hb.scanM("model", qual, 1, res)) != "NO") {
		// // while ((tmpRes = hb.scanM("attri", qualti, 2, res)) != "NO")
		// // {
		// // System.out.print("	"+res[0]+","+res[1]);
		// hb.scanMNext();
		// }
		// }

		// hb.scanIni("tempM2", "temp" + rkCon(s), "temp" + rkCon(e + 1));
		//
		// String tmpRes = "";
		// String[] res = new String[3];
		// String[] qual = new String[3];
		// qual[0] = "cof1";
		//
		// // String[] qualti = {"tst","ted"};
		//
		// hb.scanMNext();
		// while ((tmpRes = hb.scanM("model", qual, 1, res)) != "NO") {
		// // while ((tmpRes = hb.scanM("attri", qualti, 2, res)) != "NO") {
		// // System.out.print("	"+res[0]+","+res[1]);
		// hb.scanMNext();
		// }
		// System.out.printf("   %d",e-s+1);
		return;
	}

	public static void dmod2_vp2SchProDwNCPO(String qv, String tabname,
			int[] bdres, int upper, int down) {

		// scanNextM(String cf, String[] qual, int qn, String[] res)

		String[] res = new String[3];
		String row = "";

		int upbd = 0, downbd = 0;
		int mid = 0, intres = down, up = upper, dw = down;// tsegn need to
															// change,
															// real number of
															// rows
															// in tempM2V
		// char sig = 0;

		while (up < dw - 1) {
			mid = up + (dw - up) / 2;

			String[] qual = { "no" };
			row = rkCon(mid);
			hb.get("tempMap", row, "attri", qual, res);

			String[] qual1 = { "vl", "vr" };
			row = "temp" + res[0];
			hb.get("tempM2", row, "attri", qual1, res);

			if (qv.compareTo(res[0]) >= 0) { // down direction
				up = mid;

				// if (qv.compareTo(res[0]) >= 0 && qv.compareTo(res[1]) <= 0
				// && sig == 0) {
				// intres = mid;
				// sig = 1;
				// }

			} else // if
			{
				dw = mid;
			}
		}
		if (qv.compareTo(res[0]) >= 0 && qv.compareTo(res[1]) <= 0)
			downbd = mid;
		else {
			// int tmp = 0;
			if (dw == mid)
				downbd = dw - 1;
			else
				downbd = dw;

			// String[] qual = { "no" };
			// row = rkCon(tmp);
			// hb.get("tempMap", row, "attri", qual, res);
			//
			// String[] qual1 = { "vl", "vr" };
			// row = "temp" + res[0];
			// hb.get("tempM2", row, "attri", qual1, res);

			// hb.get("tempM2", "temp" + Integer.toString(tmp), "attri", qual,
			// res);

			// if (qv.compareTo(res[0]) >= 0 && qv.compareTo(res[1]) <= 0) // my
			// lose some precision
			// downbd = tmp;
		}

		bdres[0] = downbd;

	}

	public static void dmod2_vp2SchProUpNCPO(String qv, String tabname,
			int[] bdres, int upper, int down) {

		// scanNextM(String cf, String[] qual, int qn, String[] res)

		String[] res = new String[3];
		String row = "";

		int upbd = 0, downbd = 0;
		int mid = 0, intres = down, up = upper, dw = down;// tsegn need to
															// change,
															// real number of
															// rows
															// in tempM2V

		// up direction
		// up = 1;
		// dw = intres;

		while (up < dw - 1) {
			// mid = (dw - up) >> 1;
			mid = up + (dw - up) / 2;
			// res[0] = "";
			// res[1] = "";

			String[] qual = { "no" };
			row = rkCon(mid);
			hb.get("tempMap", row, "attri", qual, res);

			String[] qual1 = { "vl", "vr" };
			row = "temp" + res[0];
			hb.get("tempM2", row, "attri", qual1, res);

			// if(res[0]==res[1])
			// {
			// mid=mid+2;
			// row = rkCon(mid);
			// hb.get("tempMap", row, "attri", qual, res);
			//
			// row = "temp" + res[0];
			// hb.get("tempM2", row, "attri", qual1, res);
			// }

			// if (qv.compareTo(res[1]) <= 0) { // may need improvement
			// if ( (qv.compareTo(res[0]) >= 0 && qv.compareTo(res[1]) <= 0) ||
			// (qv.compareTo(res[0]) <= 0))
			// {
			// dw = mid;
			// }
			// else if (qv.compareTo(res[1]) > 0)
			// {
			// up=mid;
			// }

			// if(qv.compareTo(res[0]) >= 0)

			if (qv.compareTo(res[1]) > 0) {
				up = mid;
			} else {
				dw = mid;
			}

		}

		if (qv.compareTo(res[0]) >= 0 && qv.compareTo(res[1]) <= 0)
			upbd = mid;
		else {
			// int tmp = 0;
			if (dw == mid)
				upbd = dw - 1;
			else
				upbd = dw;

			// String[] qual = { "no" };
			// row = rkCon(tmp);
			// hb.get("tempMap", row, "attri", qual, res);
			//
			// String[] qual1 = { "vl", "vr" };
			// row = "temp" + res[0];
			// hb.get("tempM2", row, "attri", qual1, res);

			// hb.get("tempM2", "temp" + Integer.toString(tmp), "attri", qual,
			// res);

			// if (qv.compareTo(res[0]) >= 0 && qv.compareTo(res[1]) <= 0)
			// upbd = tmp;
		}

		bdres[0] = upbd;

		// String row = "temp" + Integer.toString(tar);
		// return getVal("tempM2", row, "model", "cof1");
		// return tar;
	}

	public static void dmod2_qvp2searchNCPO(String qv, String tabname,
			ArrayList<String> vpres, int ceil, int bot) {// ceil: 1 bot:n
		// ArrayList<String> resls) {

		int[] bdres = { 0, 0, 0, 0 };

		int s = 0, e = 0;
		dmod2_vp2SchProDwNCPO(qv, tabname, bdres, ceil, bot);
		s = bdres[0];
		dmod2_vp2SchProUpNCPO(qv, tabname, bdres, ceil, bot);
		e = bdres[0];

		if (s > e) {
			s = s ^ e;
			e = s ^ e;
			s = s ^ e;
		}
		if (s == 0)
			s = e;

		vpres.add(Integer.toString(s));
		vpres.add(Integer.toString(e));

		// hb.scanIni("tempMap", rkCon(s), rkCon(e));
		//
		// String tmpRes = "";
		// String[] res = new String[3];
		// String[] qual = new String[3];
		// qual[0] = "no";
		//
		// hb.scanMNext();
		// while ((tmpRes = hb.scanM("attri", qual, 1, res)) != "NO") {
		// // (tmpRes = hb.scanM("model", qual, 1, res)) != "NO") {
		//
		// // ...test...//
		// // String rw = "temp" + res[0];
		// // String[] qualv = { "vl", "vr" };
		// // hb.get("tempM2", rw, "attri", qualv, res);
		// // System.out.printf("		%s %s",res[0],res[1]);
		// //
		// hb.scanMNext();
		// }
		// ...test...//
		// System.out.printf("		number: %d",e-s+1);

		return;
	}

	public static void dmod2_qvInt2SchNCPO(String qvl, String qvr,
			String tabname, ArrayList<String> vires, int ceil, int bot) {
		// String[] valres) {

		// int up = 0, dw = tsegn - 1;
		int up = ceil, dw = bot; // ordinal: from 1 to number of segments
		// ArrayList<Integer> resls = new ArrayList<Integer>();
		int[] resls = { 0, 0, 0 };

		int s = 0, e = 0;
		dmod2_vp2SchProDwNCPO(qvr, tabname, resls, ceil, bot);
		s = resls[0];
		dmod2_vp2SchProUpNCPO(qvl, tabname, resls, ceil, bot);
		e = resls[0];

		// dmod2_vIntSchProNoMono(qvl, qvr, tabname, up, dw, resls, 2);

		if (s > e) {
			s = s ^ e;
			e = s ^ e;
			s = s ^ e;
		}

		if (s == 0)
			s = e;

		vires.add(Integer.toString(s));
		vires.add(Integer.toString(e));

		// hb.scanIni("tempMap", rkCon(s), rkCon(e));
		//
		// String tmpRes = "", rw = "";
		// String[] res = new String[3];
		// String[] qual = new String[3];
		// qual[0] = "no";
		//
		// hb.scanMNext();
		// while ((tmpRes = hb.scanM("attri", qual, 1, res)) != "NO") {
		//
		// // ...test...//
		// rw = "temp" + res[0];
		// // String[] qualv = { "vl", "vr" };
		// // String[] qual = { "cof1" };
		// // hb.get("tempM2", rw, "model", qualm, res);
		// // System.out.printf("		%s %s",res[0],res[1]);
		//
		// hb.scanMNext();
		// }

		// for(int i=0;i<tseqn;i++)
		// {
		// System.out.printf("%s  \n", tseq[i]);
		// }
		//
		// // Arrays.sort(tseq);
		//
		// String[] qual = { "tst","ted" };
		// String row="";
		// for(int i=0;i<tseqn;i++)
		// {
		// row="temp"+tseq[i];
		// hb.get("tempM2", row, "attri", qual, res);
		// System.out.printf("%s   %s \n", res[0], res[1]);
		// }

		return;
	}

	// ...................................................................................//

	// ...........time range...............//

	public static void dmod2_compqNCPO(String st, String ed, String vs, String vb,
			String tabname, ArrayList<String> comRes) {
		
		ArrayList<String> res= new ArrayList<String>();
		
		dmod2_qtInt2SchNCPO(st, ed,"tempM2", res);
		
		int up=0,dw=0;
		
		// int num = resls.size();
		int s =Integer.parseInt(res.get(0)), e = Integer.parseInt(res.get(1));
		if (s > e) {
			s = s ^ e;
			e = s ^ e;
			s = s ^ e;
		}
		if (s == 0)
			s = e;

		up = 1;
		dw = mod2cnt;
		
		res.clear();
		dmod2_qvInt2SchNCPO(vs, vb, "tempM2", res, up,dw);

		int vals = Integer.parseInt(res.get(0)), vale = Integer.parseInt(res.get(1));

		if (vals > vale) {
			int tmp = 0;
			tmp = vals;
			vals = vale;
			vale = tmp;
		}

		if (vals == 0)
			vals = vale;

		// System.out.printf("   %d  %d",vresls[0],vresls[1]);

		hb.scanIni("tempMap", rkCon(vals), rkCon(vale + 1));
		String tmpRes = "";
		String[] qual = new String[3];
		String[] sres = new String[3];
		qual[0] = "no";

		hb.scanMNext();
		while ((tmpRes = hb.scanM("attri", qual, 1, sres)) != "NO") {
			//
			int tmp = Integer.valueOf(sres[0]);
			if (tmp >= s && tmp <= e) {

				comRes.add("temp" + sres[0]);
			}

			hb.scanMNext();
		}
	}

	public static String secGen() {
		int sec = (int) (Math.random() * 60);

		String tmp1 = "";
		if (sec < 10)
			tmp1 = "0" + Integer.toString(sec);
		else
			tmp1 = Integer.toString(sec);

		return tmp1;
	}

	public static void tiGen(int tsno, int expcur, String tint[], int total,
			double sel) {

		int even = fio.rawn / total;
		int intnum = (int) (fio.rawn * sel);
		// int inter=(int) (Math.random() * even);

		int t = (int) (Math.random() * even);
		t = (expcur - 1) * even + t;
		String tmp = fio.rawd[t][tsno].substring(0,
				fio.rawd[t][tsno].length() - 2);

		int edtmp = t - intnum;

		if (edtmp < 0)
			edtmp = 0;

		tint[0] = tmp + secGen();

		if (edtmp == 0) {
			tint[1] = fio.rawd[edtmp][tsno].substring(0,
					fio.rawd[edtmp][tsno].length() - 2)
					+ "00";
		} else {
			tint[1] = fio.rawd[edtmp][tsno].substring(0,
					fio.rawd[edtmp][tsno].length() - 2)
					+ secGen();
		}

	}

	// ............value experiments.................................//

	// count row number

	public static void valExpIni() {
		mod2cnt = dmod2_rwCnt("tempMap");

	}

	public static int dmod2_rwCnt(String tabname) {

		hb.scanGloIni(tabname);
		String tmpRes = "";
		hb.scanMNext();

		int cnt = 0;
		while ((tmpRes = hb.scanR()) != "NO") {
			cnt++;
			hb.scanMNext();
		}
		return cnt;
	}

	public static String vGen3(int tsn, int total, int cur) {
		double minv = Double.parseDouble(fio.tsRange[tsn][0]), maxv = Double
				.parseDouble(fio.tsRange[tsn][1]);

		double inter = (double) ((maxv - minv) / total);

		return Double
				.toString(inter * Math.random() + (cur - 1) * inter + minv);
	}

	// ........................Model 2 no common path
	// optimization(NCPO).............................................//

	// ...........key fetching phase......................//

	public static void excEngTLazyM1RawIdx(ArrayList<String> rkarr, String tabname) // one
																					// by
																					// one
	{
		int num = rkarr.size();

		String[] qual = new String[3], res = new String[3];
		qual[0] = "cof1";
		String tmpRes = "";

		for (int i = 0; i < num; i++) {
			hb.get(tabname, rkarr.get(i), "model", qual, res);
		}
		return;
	}

	public static void excEngTBatM1RawIdx(ArrayList<String> rkarr, String tabname)// batch,
																				// interval
																				// scanning
	{
		int num = rkarr.size();

		String[] qual = new String[3], res = new String[3];
		qual[0] = "cof1";
		String tmpRes = "";

		hb.scanIni(tabname, rkarr.get(0), rkarr.get(num - 1));
		hb.scanMNext();

		while ((tmpRes = hb.scanM("model", qual, 1, res)) != "NO") {
			hb.scanMNext();
		}

		return;
	}

	public static void excEngTLazyM2(ArrayList<String> rkbd, String tabname) {
		
		int st = Integer.parseInt(rkbd.get(0)), ed = Integer.parseInt(rkbd
				.get(1));

		String[] qual = new String[3], res = new String[3];
		qual[0] = "cof1";
		String tmpRes = "";

		for (int i = st; i <= ed + 1; i++) {
			hb.get(tabname, "temp" + rkCon(i), "model", qual, res);
		}
		return;
	}

	public static void excEngTBatM2(ArrayList<String> rkbd, String tabname) {
		
		int st = Integer.parseInt(rkbd.get(0)), ed = Integer.parseInt(rkbd
				.get(1));

		String[] qual = new String[3], res = new String[3];
		qual[0] = "cof1";
		String tmpRes = "";

		hb.scanIni(tabname, "temp" + rkCon(st), "temp" + rkCon(ed));
		hb.scanMNext();

		while ((tmpRes = hb.scanM("model", qual, 1, res)) != "NO") {
			hb.scanMNext();
		}

		return;
	}

	public static void excEngTCacheM2(ArrayList<String> rkbd, String tabname) {
		int st = Integer.parseInt(rkbd.get(0)), ed = Integer.parseInt(rkbd
				.get(1));

		String[] qual = new String[3], res = new String[3];
		qual[0] = "cof1";
		String tmpRes = "";
		String[] qualmap = { "no" };

		hb.scanIni(tabname, rkCon(st), rkCon(ed + 1));

		hb.scanMNext();

		for (int i = st; i <= ed; i++) {
			if (hb.cacheIsExist("temp" + rkCon(i)) == 1) {

			} else {
				hb.scanM("attri", qual, 1, res);
			}
			hb.scanMNext();
		}

		// hb.scanMNext();
		// while ((tmpRes = hb.scanM("attri", qual, 1, res)) != "NO") {
		//
		// // ...test...//
		// rw = "temp" + res[0];
		// // String[] qualv = { "vl", "vr" };
		// // String[] qual = { "cof1" };
		// hb.get("tempM2", rw, "model", qualm, res);
		// // System.out.printf("		%s %s",res[0],res[1]);
		//
		// hb.scanMNext();
	}

	
	
	//................value...................................................//
	public static void excEngValLazyM1RawIdx(ArrayList<String> rkarr,
			String tabname) {
		int num = rkarr.size();

		String[] qual = new String[3], res = new String[3];
		qual[0] = "cof1";
		String tmpRes = "";

		for (int i = 0; i < num; i++) {
			hb.get(tabname, rkarr.get(i), "model", qual, res);
		}
		return;
	}

	public static void excEngValLazyM2(ArrayList<String> rkarr, String tabname) {
		
		//int num = rkarr.size();
		int st = Integer.parseInt(rkarr.get(0)), ed = Integer.parseInt(rkarr
				.get(1));

		String[] qual = new String[3], res = new String[3];
		qual[0] = "no";
		String tmpRes = "";

		for (int i = st; i <= ed; i++) {
			hb.get("tempMap", rkarr.get(i), "attri", qual, res);
		}
		return;

	}
	

	
	
	
	public static void excEngValBatRaw(ArrayList<String> rkarr,
			String tabname, String[][] seg) {

		int cnt = 0;
		String tmpRes = "";
		String[] res = new String[3];
		String[] qual = new String[3];
		qual[0] = "val";
		while (seg[cnt][0].equals("-1") == false) {
			hb.scanIni("tempRaw", seg[cnt][0], seg[cnt][1]);
			hb.scanMNext();
			while ((tmpRes = hb.scanM("atrri", qual, 1, res)) != "NO") {
				hb.scanMNext();
			}
			cnt++;
		}
		return;
	}

	public static void excEngValBatM1(ArrayList<String> rkarr,
			String tabname, String[][] seg) {

		int cnt = 0;
		String tmpRes = "";
		String[] res = new String[3];
		String[] qual = new String[3];
		qual[0] = "cof1";
		while (seg[cnt][0].equals("-1") == false) {
			hb.scanIni("tempM1", seg[cnt][0], seg[cnt][1]);
			hb.scanMNext();
			while ((tmpRes = hb.scanM("model", qual, 1, res)) != "NO") {
				hb.scanMNext();
			}
			cnt++;
		}
		return;
	}

	public static int excEngConsecJudge(String s1, String s2) {

		int num = s1.length();
		for (int i = 0; i < num - 1; i++) {
			if (s1.charAt(i) != s1.charAt(i))
				return 0;
		}
		if ((s2.charAt(num - 1) - s1.charAt(num - 1)) == 1)
			return 1;
		else
			return 0;
	}

	public static void excEngValBatIdx(ArrayList<String> rkarr, String tabname,
			String[] rkey) {

		java.util.Arrays.sort(rkey);

		int num = rkarr.size();

		String st = new String(), ed = new String();
		st = rkey[0];

		String tmpRes = "";
		String[] res = new String[3];
		String[] qual = new String[3];
		qual[0] = "cof1";

		for (int i = 1; i < num; i++) {
			if (excEngConsecJudge(rkey[i - 1], rkey[i]) == 1) {
				ed = rkey[i];
			} else {
				hb.scanIni("tempM1", st, ed);
				hb.scanMNext();
				while ((tmpRes = hb.scanM("model", qual, 1, res)) != "NO") {

					hb.scanMNext();
				}

				st = rkey[i];
				ed = rkey[i];
			}
		}

		return;
	}

	public static void excEngValBatM2(ArrayList<String> rkarr, String tabname) {

		int s=Integer.parseInt(rkarr.get(0)), e=Integer.parseInt(rkarr.get(1));
		
		hb.scanIni("tempMap", rkCon(s), rkCon(e));
		
		 String tmpRes = "";
		 String[] res = new String[3];
		 String[] qual = new String[3];
		 qual[0] = "no";
		
		 hb.scanMNext();
		 while ((tmpRes = hb.scanM("attri", qual, 1, res)) != "NO") {
		 // (tmpRes = hb.scanM("model", qual, 1, res)) != "NO") {
		
		 // ...test...//
		  String rw = "temp" + res[0];
		  String[] qualv = { "vl", "vr" };
		  hb.get("tempM2", rw, "attri", qualv, res);
		  
		 // System.out.printf("		%s %s",res[0],res[1]);
		 //
		 hb.scanMNext();
		 }	
		return;
	}
	
	
	public static void excEngValBatM2Cache(ArrayList<String> rkarr, String tabname) {

		int s=Integer.parseInt(rkarr.get(0)), e=Integer.parseInt(rkarr.get(1));
		
		hb.scanIni("tempMap", rkCon(s), rkCon(e));
		
		 String tmpRes = "";
		 String[] res = new String[3];
		 String[] qual = new String[3];
		 qual[0] = "no";
		
		 hb.scanMNext();
		 
		 for (int i = s; i < e; i++) {
				if (hb.cacheIsExist(rkCon(i)) == 1) {

				} else {
					hb.scanM("attri", qual, 1, res);
				}
				hb.scanMNext();
			}
		return;
	}
	
	

	// ....................................................................................//

	public static void qvpRawM1() {

		String expv = "";
		// expt = tGen(0);
		// expv = vGen(0);

		// System.out.printf("%s\n",expt);

		int expnum = expn;
		long st = 0, ed = 0;
		long[] tsum = { 0, 0, 0, 0, 0, 0 };
		ArrayList<String> res = new ArrayList<String>();

		while (expnum > 0) {
			// expt = tGen(1);expnumexpnum

			expv = vGen3(0, expn, expnum);
			System.out.printf("%s\n", expv, res);

			// ....value point....................//

			res.clear();
			st = System.currentTimeMillis();
		//	dmodRaw_qvp(expv, "tempRaw", res);
			// dmodRaw_qtpSeq(expt, "tempM1", res);
			// System.out.printf("   %s", res.get(0));
			ed = System.currentTimeMillis();
			tsum[0] += ((ed - st));
			System.out.printf("   %d", (ed - st));

			res.clear();
			st = System.currentTimeMillis();
			// dmod1_qvpSeq(expv, "tempM1", res);
			// System.out.printf("   %s", res.get(0));
			ed = System.currentTimeMillis();
			tsum[1] += ((ed - st));
			System.out.printf("   %d", (ed - st));

			expnum--;
		}
		System.out.printf("\nTime point query: %f %f\n", (double) tsum[0]
				/ expn, (double) tsum[1] / expn);
	}

	public static void qvpM1Idx() {

		String expv = "";
		// expt = tGen(0);
		// expv = vGen(0);

		// System.out.printf("%s\n",expt);

		int expnum = expn;
		long st = 0, ed = 0;
		long[] tsum = { 0, 0, 0, 0, 0, 0 };
		ArrayList<String> res = new ArrayList<String>();

		dmod1_idx(3000);

		while (expnum > 0) {
			// expt = tGen(1);expnumexpnum

			expv = vGen3(0, expn, expnum);
			System.out.printf("%s\n", expv);

			// ....value point....................//

			res.clear();
			st = System.currentTimeMillis();
			// dmod1_qvpSeq(expv, "tempM1", res);
			// System.out.printf("   %s", res.get(0));
			ed = System.currentTimeMillis();
			tsum[0] += ((ed - st));
			System.out.printf("   %d", (ed - st));

			res.clear();
			st = System.currentTimeMillis();
			dmod1_idxQvp(expv, res);
			// dmodRaw_qtpSeq(expt, "tempM1", res);
			// System.out.printf("   %s", res.get(0));
			ed = System.currentTimeMillis();
			tsum[1] += ((ed - st));
			System.out.printf("   %d", (ed - st));

			expnum--;
		}
		System.out.printf("\nTime point query: %f %f\n", (double) tsum[0]
				/ expn, (double) tsum[1] / expn);
	}

	public static void qvpIdxNoIdx() {

		String expv = "";
		// expt = tGen(0);
		// expv = vGen(0);

		// System.out.printf("%s\n",expt);

		valExpIni();

		dmod1_idx(3000);

		int expnum = expn;
		long st = 0, ed = 0;
		long[] tsum = { 0, 0, 0, 0, 0, 0 };
		ArrayList<String> res = new ArrayList<String>();

		while (expnum > 0) {
			// expt = tGen(1);expnum

			expv = vGen3(0, expn, expnum);
			System.out.printf("%s\n", expv);

			// ....value point....................//

			res.clear();
			st = System.currentTimeMillis();
			dmod1_idxQvp(expv, res);
			// dmodRaw_qtpSeq(expt, "tempM1", res);
			// System.out.printf("   %s", res.get(0));
			ed = System.currentTimeMillis();
			tsum[0] += ((ed - st));
			System.out.printf("   %d", (ed - st));

			res.clear();
			st = System.currentTimeMillis();
			dmod2_qvp2search(expv, "tempM2", res, 1, mod2cnt);
			// System.out.printf("   %s", res.get(0));
			ed = System.currentTimeMillis();
			tsum[1] += ((ed - st));
			System.out.printf("   %d", (ed - st));

			expnum--;
		}
		System.out.printf("\nTime point query: %f %f\n", (double) tsum[0]
				/ expn, (double) tsum[1] / expn);
	}

	// .....value range.......//

	public static void viGen(int tsno, int cur, String vint[], int total,
			double sel) {

		double minv = Double.parseDouble(fio.tsRange[tsno][0]), maxv = Double
				.parseDouble(fio.tsRange[tsno][1]);

		double intern = (double) ((maxv - minv) / total);
		double selval = (double) ((maxv - minv) * sel);

		double smal = intern * Math.random() + (cur - 1) * intern + minv;
		double big = smal + selval;

		if (big > maxv)
			big = maxv;

		vint[0] = Double.toString(smal);
		vint[1] = Double.toString(big);
	}

	// .....................complete exp............................//

	public static void getPerTest() {

		long st = 0, ed = 0;
		long[] tsum = { 0, 0, 0, 0, 0, 0 };

		fio.segIni();
		tsegn = fio.seg2(0, segpre);
		int expnum = expn;

		while (expnum > 0) {

			int rno = (int) (Math.random() * tsegn);

			String row = "temp" + "," + fio.tst[rno] + "," + fio.ted[rno];

			st = System.nanoTime();
			hb.getTestIndiCol("tempM1", row);
			ed = System.nanoTime();
			tsum[0] += ((ed - st) / 1000000);
			System.out.printf("	%d	", tsum[0]);

			st = System.nanoTime();
			hb.getTestRow("tempM1", row);
			ed = System.nanoTime();
			tsum[1] += ((ed - st) / 1000000);
			System.out.printf("%d\n", tsum[1]);

			expnum--;
		}
		System.out.printf("\nTime point query: %f %f\n", (double) tsum[0]
				/ expn, (double) tsum[1] / expn);
		return;
	}

	public static void qtpAll() {

		String expt = "";

		int expnum = expn;
		long st = 0, ed = 0;
		long[] tsum = { 0, 0, 0, 0, 0, 0, 0, 0 };
		ArrayList<String> res = new ArrayList<String>();
		dmod1_idx(3000);

		while (expnum > 0) {

			expt = tGen3(6, expnum);
			// expt="2012-05-31T23:26:20";
			System.out.printf("\n%s", expt);

			res.clear();
			st = System.nanoTime();
			dmodRaw_qtp(expt, "tempRaw", res);
			// dmodRaw_qtpSeq(expt, "tempM1", res);
			// System.out.printf("   %s", res.get(0));
			ed = System.nanoTime();
			tsum[0] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);

			res.clear();
			st = System.nanoTime();
			dmod1_qtpSeq(expt, "tempM1", res);
			// System.out.printf("   %s", res.get(0));
			ed = System.nanoTime();
			tsum[1] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);

			res.clear();
			st = System.nanoTime();
			dmod1_idxQtp(expt, res);
			// System.out.printf("   %s", res.get(0));
			ed = System.nanoTime();
			// tsum[2] += ((ed - st)/1000000);
			tsum[2] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);

			res.clear();
			// hb.cacheIni();// cache initilization
			st = System.nanoTime();
			dmod2_qtp2search(expt, "tempM2", res);
			// System.out.printf("   %s", res.get(0));
			ed = System.nanoTime();
			tsum[3] += ((ed - st)/1000000.0);
			// tsum[3] += ((ed - st)/1000000);
			System.out.printf("   %d", (ed - st)/1000000.0);

//			res.clear();
//			hb.cacheIni();// cache initilization
//			st = System.currentTimeMillis();
//			dmod2_qtp2searchCache(expt, "tempM2", res);
//			// System.out.printf("   %s", res.get(0));
//			ed = System.currentTimeMillis();
//			tsum[4] += ((ed - st));
//			// tsum[3] += ((ed - st)/1000000);
//			System.out.printf("   %d", (ed - st));

			expnum--;
		}
		System.out.printf("\nTime point query: %f %f %f %f %f\n",
				(double) tsum[0] / expn, (double) tsum[1] / expn,
				(double) tsum[2] / expn, (double) tsum[3] / expn);
				//(double) tsum[4] / expn);
	}

	public static void qtiAll() {

		int expnum = expn;
		long st = 0, ed = 0;
		long[] tsum = { 0, 0, 0, 0, 0, 0, 0, 0 };
		ArrayList<String> res = new ArrayList<String>();
		String[] expt = new String[2];

		valExpIni();
	//	dmod1_idx(2000);

		while (expnum > 0) {
			// expt = tGen(1);

			tiGen(6, expnum, expt, expn, 0.01);

			// expt[0]="2012-05-31T21:12:36 "; expt[1]="2012-05-31T23:38:54";

			// 2012-05-31T21:12:36 2012-05-31T23:38:54

			System.out.printf("\n%s   %s", expt[0], expt[1]);

			res.clear();
			st = System.nanoTime();
			dmodRaw_qtInt(expt[0], expt[1], "tempRaw", res);
			// System.out.printf("   %s", res.get(0));
			ed = System.nanoTime();
			tsum[0] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);

			res.clear();
			st = System.nanoTime();
			dmod1_qtIntSeq(expt[0], expt[1], "tempM1", res);
			// System.out.printf("   %s", res.get(0));
			ed = System.nanoTime();
			tsum[1] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);

			res.clear();
			st = System.nanoTime();
			dmod1_idxQti(expt[0], expt[1], res);
			// dmod1_idxQtiIntScan(expt[0], expt[1], res);
			// System.out.printf("   %s", res.get(0));
			ed = System.nanoTime();
			tsum[2] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);

			res.clear();
			st = System.nanoTime();
			dmod2_qtInt2Sch(expt[0], expt[1], "tempM2", res);
			// System.out.printf("   %s", res.get(0));
			ed = System.nanoTime();
			tsum[3] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);

//			res.clear();
//			st = System.nanoTime();
//			dmod2_qtInt2SchCache(expt[0], expt[1], "tempM2", res);
//			// System.out.printf("   %s", res.get(0));
//			ed = System.nanoTime();
//			tsum[4] += ((ed - st));
//			System.out.printf("   %d", (ed - st));

			res.clear();
			st = System.nanoTime();
			dmod2_qtInt2SchNCPO(expt[0], expt[1], "tempM2", res);
			// System.out.printf("   %s", res.get(0));
			ed = System.nanoTime();
			tsum[4] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);

			expnum--;
		}
		System.out.printf("\nTime point query: %f %f %f %f %f\n",
				(double) tsum[0] / expn, (double) tsum[1] / expn,
				(double) tsum[2] / expn, (double) tsum[3] / expn,
				(double) tsum[4] / expn);

	}

	public static void qvpAll() {

		String expv = "";

		int expnum = expn;
		long st = 0, ed = 0;
		long[] tsum = { 0, 0, 0, 0, 0, 0, 0, 0 };
		ArrayList<String> res = new ArrayList<String>();
		String[][] seg= new String[200][3];
		

		valExpIni();
	//	dmod1_idx(1000);

		while (expnum > 0) {

			expv = vGen3(0, expn, expnum);
			System.out.printf("\n%s", expv);

			res.clear();
			st = System.nanoTime();
			dmodRaw_qvp(expv, "tempRaw", res,seg);
			// System.out.printf("   %s", res.get(0));
			ed = System.nanoTime();
			tsum[0] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);

			res.clear();
			st = System.nanoTime();
			// dmod1_qvpSeq(expv, "tempM1", res);
			// System.out.printf("   %s", res.get(0));
			ed = System.nanoTime();
			tsum[1] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);

			res.clear();
			st = System.nanoTime();
			dmod1_idxQvp(expv, res);
			// System.out.printf("   %s", res.get(0));
			ed = System.nanoTime();
			tsum[2] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);

			res.clear();
			st = System.nanoTime();
			dmod2_qvp2search(expv, "tempM2", res, 1, mod2cnt);
			// System.out.printf("   %s", res.get(0));
			ed = System.nanoTime();
			tsum[3] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);

//			res.clear();
//			st = System.currentTimeMillis();
//			dmod2_qvp2searchCache(expv, "tempM2", res, 1, mod2cnt);
//			// System.out.printf("   %s", res.get(0));
//			ed = System.currentTimeMillis();
//			tsum[4] += ((ed - st));
//			System.out.printf("   %d", (ed - st));

			res.clear();
			st = System.nanoTime();
			dmod2_qvp2searchNCPO(expv, "tempM2", res, 1, mod2cnt);
			// System.out.printf("   %s", res.get(0));
			ed = System.nanoTime();
			tsum[4] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);

			expnum--;
		}
		System.out.printf("\nTime point query: %f %f %f %f %f \n",
				(double) tsum[0] / expn, (double) tsum[1] / expn,
				(double) tsum[2] / expn, (double) tsum[3] / expn,
				(double) tsum[4] / expn);
	}

	public static void qviAll() {

		String[] expvi = new String[2];

		int expnum = expn;
		long st = 0, ed = 0;
		long[] tsum = { 0, 0, 0, 0, 0, 0 };
		ArrayList<String> res = new ArrayList<String>();
		String[][] seg= new String[200][3];
		
		
		valExpIni();
		//dmod1_idx(1000);

		double sel = 0.01;

	//	for (sel = 0.01; sel < 1; sel = sel + 0.07) {

			expnum = expn;

			while (expnum > 0) {

				viGen(0, expnum, expvi, expn, sel);

				// expvi[0]="14.354952204273609";
				// expvi[1]="14.582452204273608";

				// System.out.printf("\n%s %s", expvi[0], expvi[1]);

				// ....value range....................//

				 res.clear();
				 st = System.nanoTime();
				 dmodRaw_qvInt(expvi[0], expvi[1], "tempRaw",res,seg);
				 // dmodRaw_qtpSeq(expt, "tempM1", res);
				 // System.out.printf("   %s", res.get(0));
				 ed = System.nanoTime();
				 tsum[0] += ((ed - st)/1000000.0);
				 System.out.printf("   %d", (ed - st)/1000000.0);
				
				 res.clear();
				 st = System.nanoTime();
				 dmod1_qvIntSeq(expvi[0], expvi[1], "tempM1", res,seg);
				 // System.out.printf("   %s", res.get(0));
				 ed = System.nanoTime();
				 tsum[1] += ((ed - st)/1000000.0);
				 System.out.printf("   %d", (ed - st)/1000000.0);
				//
				res.clear();
				st = System.nanoTime();
				dmod1_idxQvi(expvi[0], expvi[1], res);
				// dmodRaw_qtpSeq(expt, "tempM1", res);
				// System.out.printf("   %s", res.get(0));
				ed = System.nanoTime();
				tsum[2] += ((ed - st)/1000000.0);
				System.out.printf("   %d", (ed - st)/1000000.0);
				//
				res.clear();
				st = System.nanoTime();
				dmod2_qvInt2Sch(expvi[0], expvi[1], "tempM2", res, 1, mod2cnt);
				// System.out.printf("   %s", res.get(0));
				ed = System.nanoTime();
				tsum[3] += ((ed - st)/1000000.0);
				System.out.printf("   %d", (ed - st)/1000000.0);

//				res.clear();
//				st = System.currentTimeMillis();
//				dmod2_qvInt2SchCache(expvi[0], expvi[1], "tempM2", res, 1,
//						mod2cnt);
//				// System.out.printf("   %s", res.get(0));
//				ed = System.currentTimeMillis();
//				tsum[4] += ((ed - st));

				res.clear();
				st = System.nanoTime();
				dmod2_qvInt2SchNCPO(expvi[0], expvi[1], "tempM2", res, 1,
						mod2cnt);
				// System.out.printf("   %s", res.get(0));
				ed = System.nanoTime();
				tsum[4] += ((ed - st)/1000000.0);
				System.out.printf("   %d", (ed - st)/1000000.0);

				expnum--;
			}
			System.out.printf("\nTime point query: %f %f %f %f %f\n",
					(double) tsum[0] / expn, (double) tsum[1] / expn,
					(double) tsum[2] / expn, (double) tsum[3] / expn,
					(double) tsum[4] / expn);
		}
	//}
	
	
	//................execution test.......................................//
	
	public static void qtiExe() {

		int expnum = expn;
		long st = 0, ed = 0;
		long[] tsum = { 0, 0, 0, 0, 0, 0, 0, 0 };
		ArrayList<String> res = new ArrayList<String>();
		String[] expt = new String[2];

		//dmod1_idx(2000);

		while (expnum > 0) {
			// expt = tGen(1);

			tiGen(6, expnum, expt, expn, 0.01);

			// expt[0]="2012-05-31T21:12:36 "; expt[1]="2012-05-31T23:38:54";

			// 2012-05-31T21:12:36 2012-05-31T23:38:54

			System.out.printf("\n%s   %s", expt[0], expt[1]);

			res.clear();	
			dmodRaw_qtInt(expt[0], expt[1], "tempRaw", res);	
			st = System.nanoTime();
			excEngTLazyM1RawIdx(res, "tempRaw");
			ed = System.nanoTime();
			tsum[0] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);
			
			st = System.nanoTime();
			excEngTBatM1RawIdx(res, "tempRaw");
			ed = System.nanoTime();
			tsum[1] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);
			
			res.clear();
			dmod1_qtIntSeq(expt[0], expt[1], "tempM1", res);
			st = System.nanoTime();
			excEngTLazyM1RawIdx(res, "tempM1");
			ed = System.nanoTime();
			tsum[2] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);
			
			st = System.nanoTime();
			excEngTBatM1RawIdx(res, "tempM1");
			ed = System.nanoTime();
			tsum[3] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);

			res.clear();
			dmod2_qtInt2Sch(expt[0], expt[1], "tempM2", res);
			st = System.nanoTime();
			excEngTLazyM2(res, "tempM2");
			ed = System.nanoTime();
			tsum[4] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);
			
			st = System.nanoTime();
			excEngTBatM2(res, "tempM2");
			ed = System.nanoTime();
			tsum[5] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);

			expnum--;
		}
		System.out.printf("\nTime point query: %f %f ; %f %f ; %f %f;\n",
				(double) tsum[0] / expn, (double) tsum[1] / expn,
				(double) tsum[2] / expn, (double) tsum[3] / expn,
				(double) tsum[4] / expn,(double) tsum[5] / expn);

	}
	
	public static void qvpExe() {
		
		String expv = "";

		int expnum = expn;
		long st = 0, ed = 0;
		long[] tsum = { 0, 0, 0, 0, 0, 0, 0, 0 };
		ArrayList<String> res = new ArrayList<String>();
		String[][] seg= new String[200][3];
		

		valExpIni();
		dmod1_idx(1000);

		while (expnum > 0) {

			expv = vGen3(0, expn, expnum);
			System.out.printf("\n%s", expv);

			res.clear();	
			dmodRaw_qvp(expv, "tempRaw", res,seg);	
			st = System.nanoTime();
			excEngValLazyM1RawIdx(res, "tempRaw");
			ed = System.nanoTime();
			tsum[0] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);
			
			st = System.nanoTime();
			excEngValBatRaw(res, "tempRaw",seg);
			ed = System.nanoTime();
			tsum[1] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);
			
			
			res.clear();
			dmod1_qvpSeq(expv, "tempM1", res,seg);
			st = System.nanoTime();
			excEngValLazyM1RawIdx(res, "tempM1");
			ed = System.nanoTime();
			tsum[2] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);
			
			st = System.nanoTime();
			excEngValBatM1(res, "tempM1",seg);
			ed = System.nanoTime();
			tsum[3] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);

		
			res.clear();
			dmod2_qvp2search(expv, "tempM2", res, 1, mod2cnt);
			st = System.nanoTime();
			excEngValLazyM2(res, "tempM2");
			ed = System.nanoTime();
			tsum[4] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);
			
			st = System.nanoTime();
			excEngValBatM2(res, "tempM2");
			ed = System.nanoTime();
			tsum[5] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);
			
			
			
//			dmodRaw_qvp(expv, "tempRaw", res,seg);
//			
//			dmod1_qvpSeq(expv, "tempM1", res,seg);
//			
//			dmod2_qvp2search(expv, "tempM2", res, 1, mod2cnt);
			

			expnum--;
		}
		System.out.printf("\nTime point query: %f %f ; %f %f ; %f %f;\n",
				(double) tsum[0] / expn, (double) tsum[1] / expn,
				(double) tsum[2] / expn, (double) tsum[3] / expn,
				(double) tsum[4] / expn,(double) tsum[5] / expn);
		
	 return;
	}
	
public static void qviExe() {


		String[] expvi = new String[2];
		int expnum = expn;
		long st = 0, ed = 0;
		long[] tsum = { 0, 0, 0, 0, 0, 0, 0, 0 };
		ArrayList<String> res = new ArrayList<String>();
		String[][] seg= new String[200][3];
		

		valExpIni();
		dmod1_idx(1000);
		double sel=0.02;

		while (expnum > 0) {

			viGen(0, expnum, expvi, expn, sel);


			res.clear();	
			dmodRaw_qvInt(expvi[0], expvi[1], "tempRaw",res,seg);
			st = System.nanoTime();
			excEngValLazyM1RawIdx(res, "tempRaw");
			ed = System.nanoTime();
			tsum[0] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);
			
			st = System.nanoTime();
			excEngValBatRaw(res, "tempRaw",seg);
			ed = System.nanoTime();
			tsum[1] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);
			
			
			res.clear();
			dmod1_qvIntSeq(expvi[0], expvi[1], "tempM1", res,seg);
			st = System.nanoTime();
			excEngValLazyM1RawIdx(res, "tempM1");
			ed = System.nanoTime();
			tsum[2] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);
			
			st = System.nanoTime();
			excEngValBatM1(res, "tempM1",seg);
			ed = System.nanoTime();
			tsum[3] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);

		
			res.clear();
			dmod2_qvInt2Sch(expvi[0], expvi[1], "tempM2", res, 1, mod2cnt);
			st = System.nanoTime();
			excEngValLazyM2(res, "tempM2");
			ed = System.nanoTime();
			tsum[4] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);
			
			st = System.nanoTime();
			excEngValBatM2(res, "tempM2");
			ed = System.nanoTime();
			tsum[5] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);
			
			
			
//			dmodRaw_qvp(expv, "tempRaw", res,seg);
//			
//			dmod1_qvpSeq(expv, "tempM1", res,seg);
//			
//			dmod2_qvp2search(expv, "tempM2", res, 1, mod2cnt);
			

			expnum--;
		}
		System.out.printf("\nTime point query: %f %f ; %f %f ; %f %f;\n",
				(double) tsum[0] / expn, (double) tsum[1] / expn,
				(double) tsum[2] / expn, (double) tsum[3] / expn,
				(double) tsum[4] / expn,(double) tsum[5] / expn);
		
	 return;
	}

	// ............composite experiments.................................//
	public static void compqAll() {

		String[] expvi = new String[2];
		String[] expti = new String[2];

		int expnum = expn;
		long st = 0, ed = 0;
		long[] tsum = { 0, 0, 0, 0, 0, 0 };
		ArrayList<String> res = new ArrayList<String>();

		valExpIni();
		dmod1_idx(1000);

		// for(double sel=0.01;sel<=0.8;sel+=0.01)

		while (expnum > 0) {

			viGen(0, expnum, expvi, expn, 0.01);
			tiGen(6, expnum, expti, expn, 0.01);

			System.out.printf("\n%s  %s  %s  %s", expvi[0], expvi[1], expti[0],
					expti[1]);

			 res.clear();
			 st = System.nanoTime();
			 dmodRaw_compq(expti[0], expti[1], expvi[0], expvi[1], "tempRaw",
			 res);
			 // dmodRaw_qtpSeq(expt, "tempM1", res);
			 // System.out.printf("   %s", res.get(0));
			 ed = System.nanoTime();
			 tsum[0] += ((ed - st)/1000000.0);
			 System.out.printf("   %d", (ed - st)/1000000.0);
			
			 res.clear();
			 st = System.nanoTime();
			 dmod1_compq(expti[0], expti[1], expvi[0], expvi[1], "tempM1",
			 res);
			 // System.out.printf("   %s", res.get(0));
			 ed = System.nanoTime();
			 tsum[1] += ((ed - st)/1000000.0);
			 System.out.printf("   %d", (ed - st)/1000000.0);

			res.clear();
			st = System.nanoTime();
			dmod1_idxCompq(expti[0], expti[1], expvi[0], expvi[1], "tempM1");
			// System.out.printf("   %s", res.get(0));
			ed = System.nanoTime();
			tsum[2] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);
			//
			res.clear();
			st = System.nanoTime();
			dmod2_compq(expti[0], expti[1], expvi[0], expvi[1], "tempM2", res);
			// System.out.printf("   %s", res.get(0));
			ed = System.nanoTime();
			tsum[3] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);
			
			res.clear();
			st = System.nanoTime();
			dmod2_compqNCPO(expti[0], expti[1], expvi[0], expvi[1], "tempM2", res);
			// System.out.printf("   %s", res.get(0));
			ed = System.nanoTime();
			tsum[4] += ((ed - st)/1000000.0);
			System.out.printf("   %d", (ed - st)/1000000.0);

			expnum--;
		}
		System.out.printf("\nTime point query: %f %f %f %f\n", (double) tsum[0]
				/ expn, (double) tsum[1] / expn, (double) tsum[2] / expn,
				(double) tsum[3] / expn ,(double) tsum[4] / expn);
	}

	// .....................join exp............................//

	public static void joinAll() {

		String expv = "";

		int expnum = expn;
		long st = 0, ed = 0;
		long[] tsum = { 0, 0, 0, 0, 0, 0 };
		ArrayList<String> res = new ArrayList<String>();

		while (expnum > 0) {

			// res.clear();
			// st = System.currentTimeMillis();
			// dmodRaw_join(String tab1, String tab2,res); //
			// // System.out.printf("   %s", res.get(0));
			// ed = System.currentTimeMillis();
			// tsum[0] += ((ed - st));
			// System.out.printf("   %d", (ed - st));
			//
			// res.clear();
			// st = System.currentTimeMillis();
			// dmod1_join(String tab1, String tab2,res);//
			// // System.out.printf("   %s", res.get(0));
			// ed = System.currentTimeMillis();
			// tsum[1] += ((ed - st));
			// System.out.printf("   %d", (ed - st));
			//
			// res.clear();
			// st = System.currentTimeMillis();
			// dmod2_join(String tab1, String tab2,res);
			// // System.out.printf("   %s", res.get(0));
			// ed = System.currentTimeMillis();
			// tsum[2] += ((ed - st));
			// System.out.printf("   %d", (ed - st));

			expnum--;
		}
		System.out.printf("\nTime point query: %f %f %f\n", (double) tsum[0]
				/ expn, (double) tsum[1] / expn, (double) tsum[2] / expn);
	}

	public static void comCheck() {
		String expt = "";
		expt = tGen2(6);
		System.out.printf("%s\n", expt);
		ArrayList<String> tmp = new ArrayList<String>();
		dmodRaw_qtp(expt, "tempRaw", tmp);
		System.out.printf("fdafaf\n");
	}

	public static void modCons() {
		if (dmodRaw() == 1)
			System.out.println("raw model done \n");
		else
			System.out.println("problem model 1 \n");

		if (dmod1() == 1)
			System.out.println("model 1 done \n");
		else
			System.out.println("problem model 1 \n");

		if (dmod2() == 1)
			System.out.println("model 2 done \n");
		else
			System.out.println("problem model 2 \n");
	}

	public static void main(String[] args) {

		//fio = new fileIO();
		hb = new HBaseOp();
		
		olfio = new onlineFileIO();
		
		System.out.print("Everything is find.");
		
//		String a="100", b="90";
//		
//		if(a.compareTo(b)>=0) System.out.print("correct"); else System.out.print("Wrong");
		
//		olfio.ImportFolder("/home/guo/workspace/dataset3", "2010-04-16",0);

//		if (upload() == 1)
//			System.out.println("load done \n");
//		else
//			System.out.println("problem \n");

		// .........Raw Data Model.......................//

	//	modCons();

		// ....communication check......//

		// comCheck();

		// // .............Experiment.......................//

		// getPerTest();

		// qtpAll();
		// qtiAll();

		// qvpAll();
		// qviAll();

		// compqAll();

		// availExp();

		return;
	}
}
