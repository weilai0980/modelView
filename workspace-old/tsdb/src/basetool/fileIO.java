package basetool;

import basetool.segm;

import java.io.*;
import java.util.*;


public class fileIO {

	public static String[][] rawd = new String[50000][20];
	public static int rawdCol;

	public static String[][] tsRange = new String[30][5];// min 0, max 1

	public static int[] ed = new int[40000];
	public static int[] st = new int[40000];

	public static String[] tst = new String[40000];
	public static String[] ted = new String[40000];

	public static double[] vl = new double[40000];
	public static double[] vr = new double[40000];
	public static double[] modCoe = new double[40000];

	public static String lastT;
	public static String oldT;

	public int loadcnt;
	public static int rawn;
	public int segn;
	// public segm[] segs = new segm[10000];
	public segm tseg;
	public String[] rawid = { "temp", "hum", "co", "co2", "co24", "no23", "ts" };

	public static HBaseOp fhb = new HBaseOp();

	
	public String[] month = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", 
			"Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
	
	
	
	public fileIO() {
		rawd = new String[50000][20];
		rawdCol = 0;

		// segs = new segm[10000];

		// tst = new int[1000];
		// ted = new int[1000];

		st = new int[20000];
		ed = new int[20000];
		vl = new double[20000];
		vr = new double[20000];
		modCoe = new double[20000];

		tst = new String[20000];
		ted = new String[20000];

		tsRange = new String[30][5];

		rawn = segn = 0;
		// tmpr=new String[10];
		loadcnt = 0;

	}

	public void segIni() {
		/*
		 * for (int i = 0; i < segn; ++i) { segs[i].st = segs[i].ed = 0;
		 * segs[i].vl = segs[i].vr = 0.0; segs[i].modCoe = 0.0; }
		 */
		segn = 0;
	}

	public String getrow(int cnt, int tsno) {

		return rawd[cnt][tsno];
	}

	public String getColVal(int cnt, int tsno) {

		return rawd[cnt][tsno];
	}

	public static String timePro(String str) {
		int i = 0;
		for (i = 0; i < str.length(); ++i) {
			if (str.charAt(i) == '.')
				break;
		}

		return str.substring(0, i);
	}

	public static int loadfile() { // return number of time series

		// System.out.println("connected \n");

		// temperature,humidity,co,co_2,co2_4,no2_3,timed
		try {
			// CsvReader products = new CsvReader("data.txt");
			// FileReader fr=new FileReader("data.txt");
			// FileReader fr=new FileReader("tf1.txt");

			// File csv=new File("data.csv");

			// System.out.println("connected \n");

			// File tsFp = new File("td2.txt");
			File tsFp = new File("15kd.txt");

			BufferedReader br = new BufferedReader(new FileReader(tsFp));

			// System.out.println("connected \n");

			String line = "";
			rawn = 0;

			// System.out.println("connected \n");

			int i = 0;
			int tmp = 0, ini = 1, isnull = 0;
			while ((line = br.readLine()) != null) {

				StringTokenizer st = new StringTokenizer(line, ",");
				tmp = 0;
				while (st.hasMoreTokens()) {

					rawd[rawn][tmp++] = st.nextToken();

					if (rawd[rawn][tmp - 1].equals("null")) {
						isnull = 1;
						break;
					}
					if (ini == 1) {

						tsRange[tmp - 1][0] = rawd[rawn][tmp - 1];
						tsRange[tmp - 1][1] = rawd[rawn][tmp - 1];

					} else {

						if (rawd[rawn][tmp - 1].compareTo(tsRange[tmp - 1][0]) <= 0)
							tsRange[tmp - 1][0] = rawd[rawn][tmp - 1];
						else if (rawd[rawn][tmp - 1]
								.compareTo(tsRange[tmp - 1][1]) >= 0)
							tsRange[tmp - 1][1] = rawd[rawn][tmp - 1];
					}
				}
				ini = 0;
				if (isnull == 1)
					isnull = 0;
				else {
					rawd[rawn][tmp - 1] = timePro(rawd[rawn][tmp - 1]);
					rawn++;
				}
			}
			rawdCol = tmp;

			lastT = rawd[0][tmp - 1];
			oldT = rawd[rawn - 1][tmp - 1];

			return tmp - 1;

		} catch (IOException ioe) {

			System.out.println("no file connection");
			return -1;
		}

	}

	public double dataAna(String str) {
		double val = Double.parseDouble(str);
		return val;
	}

	public int seg2(int tsn, double er) {

		int i = 0, beg = 0, tmpsegn = 0;
		double cmx = dataAna(rawd[0][tsn]), cmi = dataAna(rawd[0][tsn]), segsum = dataAna(rawd[0][tsn]), tmp = 0.0;
		double bkmx = 0.0, bkmi = 0.0;

		for (i = 1; i < rawn; i++) {

			if (rawd[i][tsn].equals("null") == true) {
				i++;
				continue;
			}

			tmp = Double.parseDouble(rawd[i][tsn]);

			segsum += tmp;

			int sig1 = 0, sig2 = 0;
			if (tmp > cmx) {
				bkmx = cmx;
				cmx = tmp;
				sig1 = 1;
			} else {
				if (tmp < cmi) {
					bkmi = cmi;
					cmi = tmp;
					sig2 = 1;
				}
			}

			double terr = 0.0;
			terr = er * (double) (segsum) / (i - beg + 1);

			if (Math.abs(cmx - cmi) > 2 * terr) {

				if (sig1 == 1)
					vr[tmpsegn] = bkmx;
				else
					vr[tmpsegn] = cmx;

				if (sig2 == 1)
					vl[tmpsegn] = bkmi;
				else
					vl[tmpsegn] = cmi;

				st[tmpsegn] = i - 1;
				ed[tmpsegn] = beg;

				tst[tmpsegn] = (rawd[i - 1][rawdCol - 1]);

				ted[tmpsegn] = (rawd[beg][rawdCol - 1]);

				modCoe[tmpsegn++] = (double) (segsum - tmp) / (i - beg);
				// System.out.printf("%d %d %f %f %f\n", beg, i - 1,
				// vl[tmpsegn - 1], vr[tmpsegn - 1], modCoe[tmpsegn - 1]);

				bkmi = Double.parseDouble(rawd[i - 1][tsn]);
				bkmx = Double.parseDouble(rawd[i - 1][tsn]);
				beg = i - 1;
				cmx = Double.parseDouble(rawd[i - 1][tsn]);
				cmi = Double.parseDouble(rawd[i - 1][tsn]);
				segsum = Double.parseDouble(rawd[i - 1][tsn]);// +Double.parseDouble(rawd[i][tsn]);
				// ++i;
			}

			// sig1 = sig2 = 0;
		}

		vl[tmpsegn] = cmi;
		vr[tmpsegn] = cmx;
		st[tmpsegn] = rawn - 1;
		ed[tmpsegn] = beg;
		tst[tmpsegn] = (rawd[rawn - 1][rawdCol - 1]);
		ted[tmpsegn] = (rawd[beg][rawdCol - 1]);
		modCoe[tmpsegn++] = (double) segsum / (rawn - beg);

		// System.out.printf("%d %d %f %f %f\n", beg, i - 1, cmi, cmx,
		// modCoe[tmpsegn - 1]);

		// System.out.printf("%d %d %lf\n",segs[tmpsegn-1].st,segs[tmpsegn-1].ed,segs[tmpsegn-1].modCoe);

		// beg = i;
		// cmx = Double.parseDouble(rawd[i][tsn]);
		// cmi = Double.parseDouble(rawd[i][tsn]);
		// segsum = 0.0;

		segn = tmpsegn;
		return tmpsegn;

	}

	public void rawCon()
	{
	   return;	
	}
	public void m1Con()
	{
		return;
	}
	public void m2Con()
	{
		return;
	}
	
	
	private static void getFiles(File folder, List<File> list, String filter) {
		folder.setReadOnly();
		File[] files = folder.listFiles();
		for(int j = 0; j < files.length; j++) {
			if(files[j].isDirectory()) {
				getFiles(files[j], list, filter);
			} else {
				// Get only "*-gsensor*" files
				if (files[j].getName().contains(filter)) {
					list.add(files[j]);
				}
			}
		}
	}

	public void ImportFolder (String foldername, String filter) {

		File folder = new File(foldername);
		List<File> list = new ArrayList<File>();
		getFiles(folder, list, filter);
		
		onlinModIni();
		
		for (File f : list) {
			//doHBaseImport(f);
			
			onlineModLoad(2,0.01,f);
		}
		
	}

	public void onlinModIni()
	{
		fhb.creTab("tempRaw");
		fhb.creTab("tempM1");
		fhb.creTab("tempM2");
		return;
	}
	
	public void onlineTimePro(String unix)
	{
		
	}
	public void onlineModLoad(int tsno, double er,File file ) {
		String[][] rawdata = new String[3][10];
		int rds = 0;

		double cmx = 0.0, cmi = 0.0, segsum = 0.0;
		double bkmx = 0.0, bkmi = 0.0;
		int segpcnt = 0, segncnt = 0;
		double vrol = 0.0, vlol = 0.0, modCoeOl = 0.0;
		String tedol = new String(), tstol = new String();

		String begstamp = new String();
		int bitnum = (int) Math.log10(10000);

		try {

			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			//BufferedReader br = new BufferedReader(new FileReader(tsFp));

			String line = "";
			int tmp = 0, ini = 1, isnull = 0;
			while ((line = br.readLine()) != null) {

				StringTokenizer st = new StringTokenizer(line, ",");
				tmp = 0;
				while (st.hasMoreTokens()) {

					rawdata[rds][tmp++] = st.nextToken();

					if (rawdata[tmp - 1].equals("null")) {
						isnull = 1;
						break;
					}

					if (ini == 1) {

						tsRange[tmp - 1][0] = rawdata[rds][tmp - 1];
						tsRange[tmp - 1][1] = rawdata[rds][tmp - 1];

						cmx = Double.parseDouble(rawdata[rds][tmp - 1]);
						cmi = Double.parseDouble(rawdata[rds][tmp - 1]);

					} else {

						if (rawdata[rds][tmp - 1]
								.compareTo(tsRange[tmp - 1][0]) <= 0)
							tsRange[tmp - 1][0] = rawdata[rds][tmp - 1];
						else if (rawdata[rds][tmp - 1]
								.compareTo(tsRange[tmp - 1][1]) >= 0)
							tsRange[tmp - 1][1] = rawdata[rds][tmp - 1];
					}

				}
				ini = 0;
				if (isnull == 1)
					isnull = 0;
				else {
					rawdata[rds][tmp - 1] = timePro(rawdata[rds][tmp - 1]);
					rawdCol = tmp;

					if (fhb.put("tempRaw", rawid[tsno] + ":"
							+ rawdata[rds][rawdCol - 1], "attri", "val",
							rawdata[rds][tsno]) == 0)
						System.out.println("raw data failure");

					// .........................segmentation.............................//

					if (rawdata[tsno].equals("null") == true) {
						continue;
					}

					double valtmp = Double.parseDouble(rawdata[rds][tsno]);

					segsum += valtmp;

					int sig1 = 0, sig2 = 0;
					if (valtmp > cmx) {
						bkmx = cmx;
						cmx = valtmp;
						sig1 = 1;
					} else {
						if (valtmp < cmi) {
							bkmi = cmi;
							cmi = valtmp;
							sig2 = 1;
						}
					}

					segpcnt++;
					double terr = 0.0;
					terr = er * (double) (segsum) / segpcnt;

					if (Math.abs(cmx - cmi) > 2 * terr) {

						if (sig1 == 1)
							vrol = bkmx;
						else
							vrol = cmx;

						if (sig2 == 1)
							vlol = bkmi;
						else
							vlol = cmi;

						segncnt++;
						tstol = (rawdata[1 - rds][rawdCol - 1]);
						tedol = begstamp;
						modCoeOl = (double) (segsum - tmp) / (segpcnt - 1);

						segpcnt = 1;
						bkmi = Double.parseDouble(rawdata[1 - rds][tsno]);
						bkmx = Double.parseDouble(rawdata[1 - rds][tsno]);
						begstamp = rawdata[rds][rawdCol - 1];
						// beg = i-1;
						cmx = Double.parseDouble(rawdata[1 - rds][tsno]);
						cmi = Double.parseDouble(rawdata[1 - rds][tsno]);
						segsum = Double.parseDouble(rawdata[rds][tsno]);

						// ...............data
						// model1...........................//

						String row = rawid[tsno] + "," + tstol + "," + tedol;

						if (fhb.put("tempM1", row, "attri", "vl",
								Double.toString(vlol)) == 0)
							System.out.println("data mode1 failure");
						if (fhb.put("tempM1", row, "attri", "vr",
								Double.toString(vrol)) == 0)
							System.out.println("data model1 failure");

						if (fhb.put("tempM1", row, "model", "cof1",
								Double.toString(modCoeOl)) == 0)
							System.out.println("data model1 failure");

						// ..............data
						// model2.............................//

						int bni = (int) Math.log10(segncnt);
						String rownum = "";

						if (bni < bitnum) {
							for (int k = 0; k < (bitnum - bni); ++k)
								rownum += "0";
						}
						rownum += Integer.toString(segncnt);

						if (fhb.put("tempM2", rawid[tsno] + rownum, "attri",
								"vl", Double.toString(vlol)) == 0)
							System.out.println("data model1 failure");
						if (fhb.put("tempM2", rawid[tsno] + rownum, "attri",
								"vr", Double.toString(vrol)) == 0)
							System.out.println("data model1 failure");

						if (fhb.put("tempM2", rawid[tsno] + rownum, "attri",
								"tst", tstol) == 0)
							System.out.println("data model1 failure");
						if (fhb.put("tempM2", rawid[tsno] + rownum, "attri",
								"ted", tedol) == 0)
							System.out.println("data model1 failure");

						if (fhb.put("tempM2", rawid[tsno] + rownum, "model",
								"cof1", Double.toString(modCoeOl)) == 0)
							System.out.println("data model1 failure");

						if (fhb.put("tempM2V", Double.toString(vlol) + ','
								+ Double.toString(vrol), "attri", "no", rownum) == 0)
							System.out.println("data model1 failure");

						// ......................................................//

					}
					// .......................................................//
					rds = 1 - rds;
				}
			}
			segn = segncnt;

		} catch (IOException ioe) {

			System.out.println("no file connection");
		}

		return;
	}

	public void onlineMod2(int tsno) {
		String tabName = (rawid[tsno] + "Map");
		fhb.creTab(tabName);

		fhb.scanGloIni(rawid[tsno] + "M2V");
		String tmpRes = "";
		int cnt = 1;
		int bitnum = (int) Math.log10(segn);
		String rownum = new String();

		while ((tmpRes = fhb.scanNext("attri", "no")) != "NO") {

			int bncnt = (int) Math.log10(cnt);
			rownum = "";

			if (bncnt < bitnum) {
				for (int k = 0; k < (bitnum - bncnt); ++k)
					rownum += "0";
			}
			rownum += Integer.toString(cnt++);

			if (fhb.put(tabName, rownum, "attri", "no", tmpRes) == 0)
				System.out.println("Data Model2 Failure.");
		}
	}

}
