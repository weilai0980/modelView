import basetool.*;

import java.io.*;
import java.util.*;

public class QueryManager {

	public int rawdCol;

	public Integer[][] tsRange = new Integer[30][5];// min 0, max 1

	public int rawn;
	public int segn;
	public String[] rawid = { "temp", "hum", "co", "co2", "co24", "no23", "ts" };

	public HBaseOp fhb = new HBaseOp();
	public HBaseOp fhb2 = new HBaseOp();

	public static int loadini, isend;

	// segmentation state record//
	public static int precmx, precmi, prebkmx, prebkmi;
	public static double presegsum;
	public static int presegpcnt;
	public static String preBegStmp;

	public static String ministamp;
	public static String maxstamp;
	// ...........................//

	// .......kvi component........//
	public kviManager kviTemp;// =new kviManager("tempIdxmod");
	public kviManager kviVal;// =new kviManager("valIdxmod");

	public String prekey = "abcdefghijklmnopqrstuvwxyz";

	// ......query generation.....//
	public double sttime, edtime;
	public double maxval, minval;

	public double errpre;

	public double qrysel = 1;

	// .....comparison approach....//
	public filterHbase filterHb;
	public filterScan scanfilter;
	public MRScan mrscanner;
	public rawQuery rawscanner;

	public MRtest1 mrtest;

	// ......time counter....//
	long stcnt, edcnt;

	// ............test.............//
//	public double[] trangExp = {
//
//	1.272754426E9, 1.274610298E9, 1.269541917E9, 1.271397789E9, 1.294482299E9,
//			1.296338171E9, 1.27681552E9, 1.278671392E9, 1.288319901E9,
//			1.290175773E9, 1.286986854E9, 1.290698598E9, 1.269702524E9,
//			1.273414268E9, 1.273230674E9, 1.276942418E9, 1.299643229E9,
//			1.2999708E9, 1.284199935E9, 1.287911679E9,
//
//			1.280828795E9, 1.282684667E9, 1.299608151E9, 1.2999708E9,
//			1.280829164E9, 1.282685036E9, 1.291578928E9, 1.2934348E9,
//			1.270801648E9, 1.27265752E9, 1.277277216E9, 1.28098896E9,
//			1.285692654E9, 1.289404398E9, 1.27872375E9, 1.282435494E9,
//			1.293177345E9, 1.296889089E9, 1.284753467E9, 1.288465211E9 };
	public int trangExpCnt = 0;

	// .................................//

	public QueryManager(boolean format) throws IOException {
		rawn = 0;
		segn = 0;

		precmx = precmi = 0;
		prebkmx = prebkmi = 0;
		presegsum = 0.0;
		presegpcnt = 0;
		preBegStmp = new String();

		loadini = 1;
		isend = 0;

		errpre = 0.1;

		sttime = edtime = 0.0;
		maxval = minval = 0.0;

		if (format == true) {

			// fhb.formatHbase();

			kviTemp = new kviManager("tempIdxmodCol", true, 0);

			// // test
			// String str = kviTemp.idxtabname;

			kviVal = new kviManager("valIdxmodCol", true, 1);
			// test
			// String str1 = kviTemp.idxtabname;

		} else {
			kviTemp = new kviManager("tempIdxmodCol", false, 0);

			// // test
			// String str = kviTemp.idxtabname;

			kviVal = new kviManager("valIdxmodCol", false, 1);

		}

		scanfilter = new filterScan();
		mrscanner = new MRScan(0);
		rawscanner = new rawQuery();
		mrtest = new MRtest1(0);
		filterHb = new filterHbase(0);

		// filter=new filterHbase(0);
		ministamp = new String();
		maxstamp = new String();

		stcnt = 0;
		edcnt = 0;

		//
	}

	void startCnt() {
		stcnt = System.nanoTime();
		return;
	}

	double stopCnt() {
		edcnt = System.nanoTime();
		System.out.printf("%f  \n", (edcnt - stcnt) / 1000000000.0);
		return (edcnt - stcnt) / 1000000000.0;
	}

	private static void getFiles(File folder, List<File> list, String filter) {
		folder.setReadOnly();
		File[] files = folder.listFiles();
		for (int j = 0; j < files.length; j++) {
			if (files[j].isDirectory()) {
				getFiles(files[j], list, filter);
			} else {
				// Get only "*-gsensor*" files
				if (files[j].getName().contains(filter)) {
					list.add(files[j]);
				}
			}
		}
	}

	public void ImportFolder(String foldername, String filter, int model)
			throws IOException { // 0:
		// raw,
		// 1:
		// model1,
		// 2:
		// model2

		File folder = new File(foldername);
		List<File> list = new ArrayList<File>();
		getFiles(folder, list, filter);

		Collections.sort(list);

		File curfile;
		long filelen = 0;
		int num = list.size();

		if (model == 0) {

			double[] timeCnt = { 0.0, 0.0 };

			fhb.creTab("tempRawTemp");
			fhb.iniTabOperation("tempRawTemp");
			// fhb.iniTabOperation("tempRaw_guo");

			fhb2.creTab("tempRawVal");
			fhb2.iniTabOperation("tempRawVal");
			// fhb2.iniTabOperation("tempRaw_val");

			loadini = 1;
			for (int i = 0; i < num; i++) {
				onlineModRLoad(1, list.get(i), timeCnt);

				textInsertionPer(timeCnt, "insertionRaw");
				System.out.print(list.get(i) + "\n");
			}
		}

		else if (model == 5) {
			loadini = 1;
			// num=2;
			// onlinModIni();
			fhb.delTab("tempTest");
			fhb.creTab("tempTest");
			fhb.iniTabOperation("tempTest");

			for (int i = 0; i < num; i++) {

				// .....file check.......//
				curfile = list.get(i);
				filelen = curfile.length();
				if (filelen == 0)
					continue;
				// .....................//

				if (i == (num - 1))
					isend = 1;
				onlineMod1Load(1, errpre, list.get(i));
				System.out.print(list.get(i) + "\n");
			}

		} else if (model == 6) {
			loadini = 1;
			// num=2;
			// onlinModIni();
			fhb.delTab("tempTest");
			fhb.creTab("tempTest");
			fhb.iniTabOperation("tempTest");

			for (int i = 0; i < num; i++) {

				// .....file check.......//
				curfile = list.get(i);
				filelen = curfile.length();
				if (filelen == 0)
					continue;
				// .....................//

				if (i == (num - 1))
					isend = 1;
				onlineModRLoad_Single("tempTest", 1, list.get(i));
				System.out.print(list.get(i) + "\n");
			}

		} else if (model == 1) {
			loadini = 1;
			// num=2;
			// onlinModIni();
			double[] timeCnt = { 0.0, 0.0 };
			fhb.delTab("tempM1_guo");
			fhb.creTab("tempM1_guo");
			fhb.iniTabOperation("tempM1_guo");

			for (int i = 0; i < num; i++) {

				// .....file check.......//
				curfile = list.get(i);
				filelen = curfile.length();
				if (filelen == 0)
					continue;
				// .....................//

				if (i == (num - 1))
					isend = 1;
				onlineMod1Load(1, errpre, list.get(i), timeCnt);
				textInsertionPer(timeCnt);
				System.out.print(list.get(i) + "\n");
			}
			// try {
			// FileWriter fstream = new FileWriter("conf.txt", true);
			// BufferedWriter out = new BufferedWriter(fstream);
			// String str = "model number," + Integer.toString(segn);
			// out.write(str);
			// out.write("\n");
			// str = "maximumTime," + maxstamp;
			// out.write(str);
			// out.write("\n");
			// str = "minimumTime," + ministamp;
			// out.write(str);
			// out.write("\n");
			// str = "minival," + Integer.toString(tsRange[1][0]);
			// out.write(str);
			// out.write("\n");
			// str = "maxval," + Integer.toString(tsRange[1][1]);
			// out.write(str);
			// //
			// out.close();
			// } catch (IOException ioe) {
			//
			// System.out.println("no file connection");
			// }
		}

		else if (model == 4) {// kvi uploading
			loadini = 1;
			// onlinModIni();

			double[] idxTcnt = new double[3];
			double[] idxVcnt = new double[3];

			for (int i = 0; i < num; i++) {
				if (i == (num - 1))
					isend = 1;

				// .....file check.......//
				curfile = list.get(i);
				filelen = curfile.length();
				if (filelen == 0)
					continue;
				// .....................//

				onlineKviLoad1(1, errpre, list.get(i), idxTcnt, idxVcnt);

				// .........insertion performance record........//
				textInsertionPer(idxTcnt, idxVcnt);

				System.out.print(list.get(i) + "\n");
			}
			try {
				FileWriter fstream = new FileWriter("confQuery.txt", true);// 1st
																			// line:
																			// time
				// 2nd line: value
				BufferedWriter out = new BufferedWriter(fstream);
				String str;
				// String str = "model number," + Integer.toString(segn);
				// out.write(str);
				// out.write("\n");
				str = ministamp;
				out.write(str);
				out.write(",");
				str = maxstamp;
				out.write(str);
				out.write("\n");
				str = Integer.toString(tsRange[1][0]);
				out.write(str);
				out.write(",");
				str = Integer.toString(tsRange[1][1]);
				out.write(str);
				//
				out.close();
			} catch (IOException ioe) {

				System.out.println("no file connection");
			}
		}
	}

	public void onlinModIni() {
		fhb.formatHbase();
		return;
	}

	public String onlineTimePro(String unix) {

		String unixstr = new String();
		unixstr = unix.substring(0, unix.length() - 2);
		long unixTime = Long.parseLong(unixstr);

		long timestamp = unixTime * 1000; // msec
		java.util.Date d = new java.util.Date(timestamp);

		String str = new String();
		// str = Integer.toString(d.getYear() + 1900) + "-";
		// str += (d.getMonth() + "-");
		// str += (d.getDate());
		// str += ("T" + d.getHours());
		// str += (":" + d.getMinutes());
		// str += (":" + d.getSeconds());

		return str;
	}

	public String valReg(String regval) {
		long tmp = (long) (Double.parseDouble(regval));
		int num = 0;
		while (tmp != 0) {
			tmp = tmp / 10;
			num++;
		}
		if (num == 0)
			num = 1;
		return prekey.charAt(num - 1) + "," + regval;
	}

	public void onlineModRLoad(int tsno, File file, double iotempCnt[])
			throws IOException {
		String[][] rawdata = new String[3][10];
		int rds = 0;
		long sttime = 0, edtime = 0;
		double rcnt = 0.0;
		sttime = System.nanoTime();

		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);

		String line = "";
		int tmp = 0, ini = 1, isnull = 0;
		line = br.readLine();

		while ((line = br.readLine()) != null) {

			StringTokenizer st = new StringTokenizer(line, ",");
			tmp = 0;
			while (st.hasMoreTokens()) {
				rawdata[rds][tmp++] = st.nextToken();

				if (rawdata[rds][tmp - 1].equals("null")) {
					isnull = 1;
					break;
				}
				if (ini == 0) {
					if (rawdata[rds][0].compareTo(rawdata[1 - rds][0]) == 0) {
						isnull = 1;
						break;
					}
				}

				// .......modifcation.............//
				if (tmp == 2)
					break;
				// ...............................//
			}
			ini = 0;
			if (isnull == 1) {
				isnull = 0;
			} else {
				rawdCol = tmp;

				rcnt++;
				if (fhb.put("tempRaw_guo", rawid[0] + ":" + rawdata[rds][0],
						"attri", "val", rawdata[rds][tsno]) == 0)
					System.out.println("raw data failure");

				if (fhb2.put("tempRaw_val", rawid[0] + ":"
						+ valReg(rawdata[rds][tsno]) + ":" + rawdata[rds][0],
						"attri", "temp", rawdata[rds][0]) == 0)
					System.out.println("raw data failure");
			}
			// .......................................................//
			rds = 1 - rds;
		}

		edtime = System.nanoTime();
		iotempCnt[0] = (edtime - sttime) / 1000000000.0 / rcnt;
		return;
	}

	public void onlineModRLoad_Single(String tabn, int tsno, File file) {
		String[][] rawdata = new String[3][10];
		int rds = 0;

		try {

			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			// BufferedReader br = new BufferedReader(new FileReader(tsFp));

			String line = "";
			int tmp = 0, ini = 1, isnull = 0;
			line = br.readLine();

			while ((line = br.readLine()) != null) {

				StringTokenizer st = new StringTokenizer(line, ",");
				tmp = 0;
				while (st.hasMoreTokens()) {
					rawdata[rds][tmp++] = st.nextToken();
					if (tmp == 3)
						break;
					if (rawdata[rds][tmp - 1].equals("null")) {
						isnull = 1;
						break;
					}
					if (ini == 0) {
						if (rawdata[rds][0].compareTo(rawdata[1 - rds][0]) == 0) {
							isnull = 1;
							break;
						}
					}
				}
				ini = 0;
				if (isnull == 1) {
					isnull = 0;
				} else {
					rawdCol = tmp;

					if (fhb.put(tabn, rawid[0] + ":" + rawdata[rds][0],
							"attri", "val", rawdata[rds][tsno]) == 0)
						System.out.println("raw data failure");

					if (fhb.put(tabn, rawid[0] + ":" + rawdata[rds][0],
							"attri", "val1", rawdata[rds][tsno]) == 0)
						System.out.println("raw data failure");

					if (fhb.put(tabn, rawid[0] + ":" + rawdata[rds][0],
							"attri", "val2", rawdata[rds][tsno]) == 0)
						System.out.println("raw data failure");
					// if (fhb2.put("tempRaw_val", rawid[0] + ":"
					// + valReg(rawdata[rds][tsno]) + ":"
					// + rawdata[rds][0], "attri", "temp", rawdata[rds][0]) ==
					// 0)
					// System.out.println("raw data failure");
				}
				// .......................................................//
				rds = 1 - rds;
			}

		} catch (IOException ioe) {

			System.out.println("no file connection");
		}
		return;
	}

	public void onlineMod1Load(int tsno, double er, File file) {
		String[][] rawdata = new String[3][10];
		int rds = 0;

		int cmx = 0, cmi = 0;
		double segsum = 0.0;// cmx = 0.0, cmi = 0.0, segsum = 0.0;
		int bkmx = 0, bkmi = 0;
		// double bkmx = 0.0, bkmi = 0.0;
		int segpcnt = 0, segncnt = 0;
		double vrol = 0.0, vlol = 0.0, modCoeOl = 0.0;
		String tedol = new String(), tstol = new String();

		String begstamp = new String();
		String ministamp = new String();

		int tempfileini = 0;

		try {

			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String line = "";
			int tmp = 0, ini = 1, isnull = 0;
			line = br.readLine();

			while ((line = br.readLine()) != null) {

				StringTokenizer st = new StringTokenizer(line, ",");
				tmp = 0;
				while (st.hasMoreTokens()) {

					rawdata[rds][tmp++] = st.nextToken();

					if (tmp == 3)
						break;
					if (rawdata[rds][tmp - 1].equals("null")) {
						isnull = 1;
						break;
					}
					if (ini == 1) {

						if (loadini == 1) {
							if (tmp != 1) {
								tsRange[tmp - 1][0] = Integer
										.parseInt(rawdata[rds][tmp - 1]);
								tsRange[tmp - 1][1] = Integer
										.parseInt(rawdata[rds][tmp - 1]);
							}
							if ((tmp - 1) == tsno) {
								cmx = Integer.parseInt(rawdata[rds][tsno]);
								cmi = Integer.parseInt(rawdata[rds][tsno]);
							}
							begstamp = rawdata[rds][0];
							ministamp = rawdata[rds][0];

						} else {

							cmx = precmx;
							cmi = precmi;

							segsum = presegsum;
							segpcnt = presegpcnt;
							begstamp = preBegStmp;

							tempfileini = 1;
						}

					} else {

						if (rawdata[rds][0].compareTo(rawdata[1 - rds][0]) == 0) {
							isnull = 1;
							break;
						}
						if ((tmp - 1) == tsno) {
							int curval = Integer
									.parseInt(rawdata[rds][tmp - 1]);
							if (curval < tsRange[tmp - 1][0])
								tsRange[tmp - 1][0] = curval;
							else if (curval >= tsRange[tmp - 1][1])
								tsRange[tmp - 1][1] = curval;
						}
					}
				}
				ini = 0;
				loadini = 0;

				if (isnull == 1) {
					isnull = 0;
				} else {
					// rawdata[rds][0] = onlineTimePro(rawdata[rds][0]);
					rawdCol = tmp;

					// .........................segmentation.............................//
					if (rawdata[tsno].equals("null") == true) {
						continue;
					}
					int valtmp = Integer.parseInt(rawdata[rds][tsno]);

					segsum += valtmp;
					segpcnt++;

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

					if (tempfileini == 1) {
						tempfileini = 0;
						rds = 1 - rds;
						continue;
					}

					double terr = 0.0;
					terr = er * (double) (segsum) / segpcnt;

					if (Math.abs(cmx - cmi) > Math.abs(3 * terr)) {

						if (sig1 == 1)
							vrol = bkmx;
						else
							vrol = cmx;

						if (sig2 == 1)
							vlol = bkmi;
						else
							vlol = cmi;

						segncnt++;
						tstol = begstamp;
						tedol = (rawdata[1 - rds][0]);
						modCoeOl = (double) (segsum - valtmp) / (segpcnt - 1);

						segpcnt = 2;
						begstamp = rawdata[1 - rds][0];
						bkmi = Integer.parseInt(rawdata[1 - rds][tsno]);
						bkmx = Integer.parseInt(rawdata[1 - rds][tsno]);
						cmx = Integer.parseInt(rawdata[rds][tsno]);
						cmi = Integer.parseInt(rawdata[rds][tsno]);
						segsum = Double.parseDouble(rawdata[rds][tsno])
								+ Double.parseDouble(rawdata[1 - rds][tsno]);

						// ...............data
						// model1...........................//

						String row = rawid[0] + "," + tstol + "," + tedol;

						if (fhb.put("tempM1_guo", row, "attri", "vl",
								Double.toString(vlol)) == 0)
							System.out.println("data mode1 failure");
						if (fhb.put("tempM1_guo", row, "attri", "vr",
								Double.toString(vrol)) == 0)
							System.out.println("data model1 failure");

						// if (fhb.put("tempM1", row, "attri", "st", tstol) ==
						// 0)
						// System.out.println("data mode1 failure");
						// if (fhb.put("tempM1", row, "attri", "ed", tedol) ==
						// 0)
						// System.out.println("data model1 failure");

						if (fhb.put("tempM1_guo", row, "model", "cof1",
								Double.toString(modCoeOl)) == 0)
							System.out.println("data model1 failure");

						// ......................................................//
					}
					rds = 1 - rds;
				}
			}

			if (isend == 1) {
				segncnt++;
				tstol = begstamp;
				tedol = (rawdata[1 - rds][0]);
				modCoeOl = (double) (segsum) / (segpcnt - 1);

				// ...............data
				// model1...........................//

				String row = rawid[0] + "," + tstol + "," + tedol;

				if (fhb.put("tempM1_guo", row, "attri", "vl",
						Double.toString(vlol)) == 0)
					System.out.println("data mode1 failure");
				if (fhb.put("tempM1_guo", row, "attri", "vr",
						Double.toString(vrol)) == 0)
					System.out.println("data model1 failure");

				// if (fhb.put("tempM1", row, "attri", "st", tstol) == 0)
				// System.out.println("data mode1 failure");
				// if (fhb.put("tempM1", row, "attri", "ed", tedol) == 0)
				// System.out.println("data model1 failure");

				if (fhb.put("tempM1_guo", row, "model", "cof1",
						Double.toString(modCoeOl)) == 0)
					System.out.println("data model1 failure");

				// ......................................................//
			}
			segn += segncnt;
			precmx = cmx;
			precmi = cmi;
			presegsum = segsum;
			presegpcnt = segpcnt;
			preBegStmp = begstamp;

		} catch (IOException ioe) {

			System.out.println("no file connection");
		}

		return;
	}

	public void onlineMod1LoadText(int tsno, double er, File file)
			throws IOException {
		String[][] rawdata = new String[3][10];
		int rds = 0;

		int cmx = 0, cmi = 0;
		double segsum = 0.0;// cmx = 0.0, cmi = 0.0, segsum = 0.0;
		int bkmx = 0, bkmi = 0;
		// double bkmx = 0.0, bkmi = 0.0;
		int segpcnt = 0, segncnt = 0;
		double vrol = 0.0, vlol = 0.0, modCoeOl = 0.0;
		String tedol = new String(), tstol = new String();
		String row = new String();

		String begstamp = new String();
		String ministamp = new String();

		int tempfileini = 0;

		FileWriter fstream;

		fstream = new FileWriter("./result/rawMod.txt", true);
		BufferedWriter out = new BufferedWriter(fstream);

		try {

			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String line = "";
			int tmp = 0, ini = 1, isnull = 0;
			line = br.readLine();

			while ((line = br.readLine()) != null) {

				StringTokenizer st = new StringTokenizer(line, ",");
				tmp = 0;
				while (st.hasMoreTokens()) {

					rawdata[rds][tmp++] = st.nextToken();

					if (tmp == 3)
						break;
					if (rawdata[rds][tmp - 1].equals("null")) {
						isnull = 1;
						break;
					}
					if (ini == 1) {

						if (loadini == 1) {
							if (tmp != 1) {
								tsRange[tmp - 1][0] = Integer
										.parseInt(rawdata[rds][tmp - 1]);
								tsRange[tmp - 1][1] = Integer
										.parseInt(rawdata[rds][tmp - 1]);
							}
							if ((tmp - 1) == tsno) {
								cmx = Integer.parseInt(rawdata[rds][tsno]);
								cmi = Integer.parseInt(rawdata[rds][tsno]);
							}
							begstamp = rawdata[rds][0];
							ministamp = rawdata[rds][0];

						} else {

							cmx = precmx;
							cmi = precmi;

							segsum = presegsum;
							segpcnt = presegpcnt;
							begstamp = preBegStmp;

							tempfileini = 1;
						}

					} else {

						if (rawdata[rds][0].compareTo(rawdata[1 - rds][0]) == 0) {
							isnull = 1;
							break;
						}
						if ((tmp - 1) == tsno) {
							int curval = Integer
									.parseInt(rawdata[rds][tmp - 1]);
							if (curval < tsRange[tmp - 1][0])
								tsRange[tmp - 1][0] = curval;
							else if (curval >= tsRange[tmp - 1][1])
								tsRange[tmp - 1][1] = curval;
						}
					}
				}
				ini = 0;
				loadini = 0;

				if (isnull == 1) {
					isnull = 0;
				} else {
					// rawdata[rds][0] = onlineTimePro(rawdata[rds][0]);
					rawdCol = tmp;

					// .........................segmentation.............................//
					if (rawdata[tsno].equals("null") == true) {
						continue;
					}
					int valtmp = Integer.parseInt(rawdata[rds][tsno]);

					segsum += valtmp;
					segpcnt++;

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

					if (tempfileini == 1) {
						tempfileini = 0;
						rds = 1 - rds;
						continue;
					}

					double terr = 0.0;
					terr = er * (double) (segsum) / segpcnt;

					if (Math.abs(cmx - cmi) > Math.abs(3 * terr)) {

						if (sig1 == 1)
							vrol = bkmx;
						else
							vrol = cmx;

						if (sig2 == 1)
							vlol = bkmi;
						else
							vlol = cmi;

						segncnt++;
						tstol = begstamp;
						tedol = (rawdata[1 - rds][0]);
						modCoeOl = (double) (segsum - valtmp) / (segpcnt - 1);

						segpcnt = 2;
						begstamp = rawdata[1 - rds][0];
						bkmi = Integer.parseInt(rawdata[1 - rds][tsno]);
						bkmx = Integer.parseInt(rawdata[1 - rds][tsno]);
						cmx = Integer.parseInt(rawdata[rds][tsno]);
						cmi = Integer.parseInt(rawdata[rds][tsno]);
						segsum = Double.parseDouble(rawdata[rds][tsno])
								+ Double.parseDouble(rawdata[1 - rds][tsno]);

						// ...............data
						// model1...........................//

						row = tstol + "," + tedol + "," + vlol + "," + vrol
								+ "," + modCoeOl + "," + "\n";
						out.write(row);

						// ......................................................//
					}
					rds = 1 - rds;
				}
			}

			if (isend == 1) {
				segncnt++;
				tstol = begstamp;
				tedol = (rawdata[1 - rds][0]);
				modCoeOl = (double) (segsum) / (segpcnt - 1);

				// ...............data
				// model1...........................//
				row = tstol + "," + tedol + "," + vlol + "," + vrol + ","
						+ modCoeOl + "," + "\n";

				out.write(row);

				// ......................................................//
			}
			segn += segncnt;
			precmx = cmx;
			precmi = cmi;
			presegsum = segsum;
			presegpcnt = segpcnt;
			preBegStmp = begstamp;

			out.close();

		} catch (IOException ioe) {

			System.out.println("no file connection");
		}

		return;
	}

	public void onlineMod1Load(int tsno, double er, File file,
			double iotempCnt[]) {
		String[][] rawdata = new String[3][10];
		int rds = 0;

		int cmx = 0, cmi = 0;
		double segsum = 0.0;// cmx = 0.0, cmi = 0.0, segsum = 0.0;
		int bkmx = 0, bkmi = 0;
		// double bkmx = 0.0, bkmi = 0.0;
		int segpcnt = 0, segncnt = 0;
		double vrol = 0.0, vlol = 0.0, modCoeOl = 0.0;
		String tedol = new String(), tstol = new String();

		String begstamp = new String();
		String ministamp = new String();

		int tempfileini = 0;

		// ....time cnt...//
		double sttime = System.nanoTime();
		double edtime = 0.0;

		try {

			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String line = "";
			int tmp = 0, ini = 1, isnull = 0;
			line = br.readLine();

			while ((line = br.readLine()) != null) {

				StringTokenizer st = new StringTokenizer(line, ",");
				tmp = 0;
				while (st.hasMoreTokens()) {

					rawdata[rds][tmp++] = st.nextToken();

					if (tmp == 3)
						break;
					if (rawdata[rds][tmp - 1].equals("null")) {
						isnull = 1;
						break;
					}
					if (ini == 1) {

						if (loadini == 1) {
							if (tmp != 1) {
								tsRange[tmp - 1][0] = Integer
										.parseInt(rawdata[rds][tmp - 1]);
								tsRange[tmp - 1][1] = Integer
										.parseInt(rawdata[rds][tmp - 1]);
							}
							if ((tmp - 1) == tsno) {
								cmx = Integer.parseInt(rawdata[rds][tsno]);
								cmi = Integer.parseInt(rawdata[rds][tsno]);
							}
							begstamp = rawdata[rds][0];
							ministamp = rawdata[rds][0];

						} else {

							cmx = precmx;
							cmi = precmi;

							segsum = presegsum;
							segpcnt = presegpcnt;
							begstamp = preBegStmp;

							tempfileini = 1;
						}

					} else {

						if (rawdata[rds][0].compareTo(rawdata[1 - rds][0]) == 0) {
							isnull = 1;
							break;
						}
						if ((tmp - 1) == tsno) {
							int curval = Integer
									.parseInt(rawdata[rds][tmp - 1]);
							if (curval < tsRange[tmp - 1][0])
								tsRange[tmp - 1][0] = curval;
							else if (curval >= tsRange[tmp - 1][1])
								tsRange[tmp - 1][1] = curval;
						}
					}
				}
				ini = 0;
				loadini = 0;

				if (isnull == 1) {
					isnull = 0;
				} else {
					// rawdata[rds][0] = onlineTimePro(rawdata[rds][0]);
					rawdCol = tmp;

					// .........................segmentation.............................//
					if (rawdata[tsno].equals("null") == true) {
						continue;
					}
					int valtmp = Integer.parseInt(rawdata[rds][tsno]);

					segsum += valtmp;
					segpcnt++;

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

					if (tempfileini == 1) {
						tempfileini = 0;
						rds = 1 - rds;
						continue;
					}

					double terr = 0.0;
					terr = er * (double) (segsum) / segpcnt;

					if (Math.abs(cmx - cmi) > Math.abs(3 * terr)) {

						if (sig1 == 1)
							vrol = bkmx;
						else
							vrol = cmx;

						if (sig2 == 1)
							vlol = bkmi;
						else
							vlol = cmi;

						segncnt++;
						tstol = begstamp;
						tedol = (rawdata[1 - rds][0]);
						modCoeOl = (double) (segsum - valtmp) / (segpcnt - 1);

						segpcnt = 2;
						begstamp = rawdata[1 - rds][0];
						bkmi = Integer.parseInt(rawdata[1 - rds][tsno]);
						bkmx = Integer.parseInt(rawdata[1 - rds][tsno]);
						cmx = Integer.parseInt(rawdata[rds][tsno]);
						cmi = Integer.parseInt(rawdata[rds][tsno]);
						segsum = Double.parseDouble(rawdata[rds][tsno])
								+ Double.parseDouble(rawdata[1 - rds][tsno]);

						// ...............data
						// model1...........................//

						String row = rawid[0] + "," + tstol + "," + tedol;
						if (fhb.put("tempM1_guo", row, "attri", "vl",
								Double.toString(vlol)) == 0)
							System.out.println("data mode1 failure");
						if (fhb.put("tempM1_guo", row, "attri", "vr",
								Double.toString(vrol)) == 0)
							System.out.println("data model1 failure");
						if (fhb.put("tempM1_guo", row, "attri", "st", tstol) == 0)
							System.out.println("data mode1 failure");
						if (fhb.put("tempM1_guo", row, "attri", "ed", tedol) == 0)
							System.out.println("data model1 failure");
						if (fhb.put("tempM1_guo", row, "model", "cof1",
								Double.toString(modCoeOl)) == 0)
							System.out.println("data model1 failure");

						// ......................................................//
					}
					rds = 1 - rds;
				}
			}

			if (isend == 1) {
				segncnt++;
				tstol = begstamp;
				tedol = (rawdata[1 - rds][0]);
				modCoeOl = (double) (segsum) / (segpcnt - 1);

				// ...............data
				// model1...........................//

				String row = rawid[0] + "," + tstol + "," + tedol;
				if (fhb.put("tempM1_guo", row, "attri", "vl",
						Double.toString(vlol)) == 0)
					System.out.println("data mode1 failure");
				if (fhb.put("tempM1_guo", row, "attri", "vr",
						Double.toString(vrol)) == 0)
					System.out.println("data model1 failure");
				if (fhb.put("tempM1_guo", row, "attri", "st", tstol) == 0)
					System.out.println("data mode1 failure");
				if (fhb.put("tempM1_guo", row, "attri", "ed", tedol) == 0)
					System.out.println("data model1 failure");
				if (fhb.put("tempM1_guo", row, "model", "cof1",
						Double.toString(modCoeOl)) == 0)
					System.out.println("data model1 failure");

				// ......................................................//
			}
			segn += segncnt;
			precmx = cmx;
			precmi = cmi;
			presegsum = segsum;
			presegpcnt = segpcnt;
			preBegStmp = begstamp;

			edtime = System.nanoTime();
			iotempCnt[0] = (edtime - sttime) / 1000000000.0 / (double) segncnt;

		} catch (IOException ioe) {

			System.out.println("no file connection");
		}

		return;
	}

	public void onlineKviLoad(int tsno, double er, File file, double inTemp[],
			double inVal[]) {// inEffi[0][0]: temporal index in memory time;
		String[][] rawdata = new String[3][10];
		int rds = 0;

		int cmx = 0, cmi = 0;
		double segsum = 0.0;// cmx = 0.0, cmi = 0.0, segsum = 0.0;
		int bkmx = 0, bkmi = 0;
		// double bkmx = 0.0, bkmi = 0.0;
		int segpcnt = 0, segncnt = 0;
		double vrol = 0.0, vlol = 0.0, modCoeOl = 0.0;
		String tedol = new String(), tstol = new String();

		String begstamp = new String();
		double[] modinfor = new double[5];

		int tempfileini = 0;

		// ......insertion performance.......//
		double[] tempT = { 0.0, 0.0 }, valT = { 0.0, 0.0 };
		double[] tmpT = { 0.0, 0.0 };

		// ...unit test..//
		long numcnt = 0;

		String[] vqual = { "vl", "vr" };
		String[] tqual = { "st", "ed" };

		// ..............//

		try {

			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String line = "";
			int tmp = 0, ini = 1, isnull = 0;
			line = br.readLine();

			while ((line = br.readLine()) != null) {

				// ...unit test..//
				numcnt++;
				// .............//

				StringTokenizer st = new StringTokenizer(line, ",");
				tmp = 0;
				while (st.hasMoreTokens()) {

					rawdata[rds][tmp++] = st.nextToken();

					if (tmp == 3)
						break;
					if (rawdata[rds][tmp - 1].equals("null")
							|| rawdata[rds][tmp - 1].length() > 14) {
						isnull = 1;
						break;
					}
					if (ini == 1) {

						if (loadini == 1) {
							if (tmp != 1) {
								tsRange[tmp - 1][0] = Integer
										.parseInt(rawdata[rds][tmp - 1]);
								tsRange[tmp - 1][1] = Integer
										.parseInt(rawdata[rds][tmp - 1]);
							}
							if ((tmp - 1) == tsno) {
								cmx = Integer.parseInt(rawdata[rds][tsno]);
								cmi = Integer.parseInt(rawdata[rds][tsno]);
							}
							begstamp = rawdata[rds][0];
							ministamp = rawdata[rds][0];

						} else {

							cmx = precmx;
							cmi = precmi;

							segsum = presegsum;
							segpcnt = presegpcnt;
							begstamp = preBegStmp;

							tempfileini = 1;
						}

					} else {

						if (rawdata[rds][0].compareTo(rawdata[1 - rds][0]) == 0) {
							isnull = 1;
							break;
						}
						if ((tmp - 1) == tsno) {
							int curval = Integer
									.parseInt(rawdata[rds][tmp - 1]);
							if (curval < tsRange[tmp - 1][0])
								tsRange[tmp - 1][0] = curval;
							else if (curval >= tsRange[tmp - 1][1])
								tsRange[tmp - 1][1] = curval;
						}
					}
				}
				ini = 0;
				loadini = 0;

				if (isnull == 1) {
					isnull = 0;
				} else {
					// rawdata[rds][0] = onlineTimePro(rawdata[rds][0]);
					rawdCol = tmp;

					// .........................segmentation.............................//
					if (rawdata[tsno].equals("null") == true) {
						continue;
					}
					int valtmp = Integer.parseInt(rawdata[rds][tsno]);
					maxstamp = rawdata[rds][0];

					segsum += valtmp;
					segpcnt++;

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

					if (tempfileini == 1) {
						tempfileini = 0;
						rds = 1 - rds;
						continue;
					}

					double terr = 0.0;
					terr = er * (double) (segsum) / segpcnt;

					if (Math.abs(cmx - cmi) > Math.abs(3 * terr)) {

						if (sig1 == 1)
							vrol = bkmx;
						else
							vrol = cmx;

						if (sig2 == 1)
							vlol = bkmi;
						else
							vlol = cmi;

						segncnt++;
						tstol = begstamp;
						tedol = (rawdata[1 - rds][0]);
						modCoeOl = (double) (segsum - valtmp) / (segpcnt - 1);

						segpcnt = 2;
						begstamp = rawdata[1 - rds][0];
						bkmi = Integer.parseInt(rawdata[1 - rds][tsno]);
						bkmx = Integer.parseInt(rawdata[1 - rds][tsno]);
						cmx = Integer.parseInt(rawdata[rds][tsno]);
						cmi = Integer.parseInt(rawdata[rds][tsno]);
						segsum = Double.parseDouble(rawdata[rds][tsno])
								+ Double.parseDouble(rawdata[1 - rds][tsno]);

						// ...............data
						// model1...........................//

						// (double l, double r, double modinfo[], int order,
						// double assocl, double assocr)
						modinfor[0] = modCoeOl;

						kviTemp.kvi_insert(Double.parseDouble(tstol),
								Double.parseDouble(tedol), modinfor, 0, vlol,
								vrol, tmpT, tqual, vqual);

						tempT[0] += tmpT[0];
						tempT[1] += tmpT[1];

						kviVal.kvi_insert(vlol, vrol, modinfor, 0,
								Double.parseDouble(tstol),
								Double.parseDouble(tedol), tmpT, vqual, tqual);

						valT[0] += tmpT[0];
						valT[1] += tmpT[1];
					}
					rds = 1 - rds;
				}
			}

			if (isend == 1) {
				segncnt++;
				tstol = begstamp;
				tedol = (rawdata[1 - rds][0]);
				modCoeOl = (double) (segsum) / (segpcnt - 1);
				modinfor[0] = modCoeOl;

				kviTemp.kvi_insert(Double.parseDouble(tstol),
						Double.parseDouble(tedol), modinfor, 0, vlol, vrol,
						tmpT, tqual, vqual);

				tempT[0] += tmpT[0];
				tempT[1] += tmpT[1];

				kviVal.kvi_insert(vlol, vrol, modinfor, 0,
						Double.parseDouble(tstol), Double.parseDouble(tedol),
						tmpT, vqual, tqual);

				valT[0] += tmpT[0];
				valT[1] += tmpT[1];

				// String row = rawid[0] + "," + tstol + "," + tedol;
				//
				// if (fhb.put("tempM1", row, "attri", "vl",
				// Double.toString(vlol)) == 0)
				// System.out.println("data mode1 failure");
				// if (fhb.put("tempM1", row, "attri", "vr",
				// Double.toString(vrol)) == 0)
				// System.out.println("data model1 failure");
				//
				// if (fhb.put("tempM1", row, "model", "cof1",
				// Double.toString(modCoeOl)) == 0)
				// System.out.println("data model1 failure");

				// ......................................................//
			}
			// if (numcnt != 0) {
			segn += segncnt;

			precmx = cmx;
			precmi = cmi;
			presegsum = segsum;
			presegpcnt = segpcnt;
			preBegStmp = begstamp;

			inTemp[0] = tempT[0] / segncnt;
			inTemp[1] = tempT[1] / segncnt;

			inVal[0] = valT[0] / segncnt;
			inVal[1] = valT[1] / segncnt;
			// }

		} catch (IOException ioe) {

			System.out.println("no file connection");
		}

		return;
	}

	public void onlineKviLoad1(int tsno, double er, File file, double inTemp[],
			double inVal[]) {// inEffi[0][0]: temporal index in memory time;
		String[][] rawdata = new String[3][10];
		int rds = 0;

		int cmx = 0, cmi = 0;
		double segsum = 0.0;// cmx = 0.0, cmi = 0.0, segsum = 0.0;
		int bkmx = 0, bkmi = 0;
		// double bkmx = 0.0, bkmi = 0.0;
		int segpcnt = 0, segncnt = 0;
		double vrol = 0.0, vlol = 0.0, modCoeOl = 0.0;
		String tedol = new String(), tstol = new String();

		String begstamp = new String();
		double[] modinfor = new double[5];

		int tempfileini = 0;

		// ......insertion performance.......//
		double[] tempT = { 0.0, 0.0 }, valT = { 0.0, 0.0 };
		double[] tmpT = { 0.0, 0.0 };

		// ...unit test..//
		long numcnt = 0;

		String[] vqual = { "vl", "vr" };
		String[] tqual = { "st", "ed" };

		// ..............//

		try {

			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String line = "";
			int tmp = 0, ini = 1, isnull = 0;
			line = br.readLine();

			while ((line = br.readLine()) != null) {

				// ...unit test..//
				numcnt++;
				// .............//

				StringTokenizer st = new StringTokenizer(line, ",");
				tmp = 0;
				while (st.hasMoreTokens()) {

					rawdata[rds][tmp++] = st.nextToken();

					if (rawdata[rds][tmp - 1].equals("null")
							|| rawdata[rds][tmp - 1].length() > 14) {
						isnull = 1;
						break;
					}
					if (ini == 1) {

						if (loadini == 1) {
							if (tmp != 1) {
								tsRange[tmp - 1][0] = Integer
										.parseInt(rawdata[rds][tmp - 1]);
								tsRange[tmp - 1][1] = Integer
										.parseInt(rawdata[rds][tmp - 1]);
							}
							if ((tmp - 1) == tsno) {
								cmx = Integer.parseInt(rawdata[rds][tsno]);
								cmi = Integer.parseInt(rawdata[rds][tsno]);
							}
							begstamp = rawdata[rds][0];
							ministamp = rawdata[rds][0];

						} else {

							cmx = precmx;
							cmi = precmi;

							segsum = presegsum;
							segpcnt = presegpcnt;
							begstamp = preBegStmp;

							tempfileini = 1;
						}

					} else {

						if (rawdata[rds][0].compareTo(rawdata[1 - rds][0]) == 0) {
							isnull = 1;
							break;
						}
						if ((tmp - 1) == tsno) {
							int curval = Integer
									.parseInt(rawdata[rds][tmp - 1]);
							if (curval < tsRange[tmp - 1][0])
								tsRange[tmp - 1][0] = curval;
							else if (curval >= tsRange[tmp - 1][1])
								tsRange[tmp - 1][1] = curval;
						}
					}

					if (tmp == 2)// modification
						break;
				}
				ini = 0;
				loadini = 0;

				if (isnull == 1) {
					isnull = 0;
				} else {
					// rawdata[rds][0] = onlineTimePro(rawdata[rds][0]);
					rawdCol = tmp;

					// .........................segmentation.............................//
					if (rawdata[tsno].equals("null") == true) {
						continue;
					}
					int valtmp = Integer.parseInt(rawdata[rds][tsno]);
					maxstamp = rawdata[rds][0];

					segsum += valtmp;
					segpcnt++;

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

					if (tempfileini == 1) {
						tempfileini = 0;
						rds = 1 - rds;
						continue;
					}

					double terr = 0.0;
					terr = er * (double) (segsum) / segpcnt;

					if (Math.abs(cmx - cmi) > Math.abs(3 * terr)) {

						if (sig1 == 1)
							vrol = bkmx;
						else
							vrol = cmx;

						if (sig2 == 1)
							vlol = bkmi;
						else
							vlol = cmi;

						segncnt++;
						tstol = begstamp;
						tedol = (rawdata[1 - rds][0]);
						modCoeOl = (double) (segsum - valtmp) / (segpcnt - 1);

						segpcnt = 2;
						begstamp = rawdata[1 - rds][0];
						bkmi = Integer.parseInt(rawdata[1 - rds][tsno]);
						bkmx = Integer.parseInt(rawdata[1 - rds][tsno]);
						cmx = Integer.parseInt(rawdata[rds][tsno]);
						cmi = Integer.parseInt(rawdata[rds][tsno]);
						segsum = Double.parseDouble(rawdata[rds][tsno])
								+ Double.parseDouble(rawdata[1 - rds][tsno]);

						// ...............data
						// model1...........................//

						// (double l, double r, double modinfo[], int order,
						// double assocl, double assocr)
						modinfor[0] = modCoeOl;

						kviTemp.kvi_insert(Double.parseDouble(tstol),
								Double.parseDouble(tedol), modinfor, 0, vlol,
								vrol, tmpT, tqual, vqual);

						tempT[0] += tmpT[0];
						tempT[1] += tmpT[1];

						kviVal.kvi_insert(vlol, vrol, modinfor, 0,
								Double.parseDouble(tstol),
								Double.parseDouble(tedol), tmpT, vqual, tqual);

						valT[0] += tmpT[0];
						valT[1] += tmpT[1];
					}
					rds = 1 - rds;
				}
			}

			if (isend == 1) {
				segncnt++;
				tstol = begstamp;
				tedol = (rawdata[1 - rds][0]);
				modCoeOl = (double) (segsum) / (segpcnt - 1);
				modinfor[0] = modCoeOl;

				kviTemp.kvi_insert(Double.parseDouble(tstol),
						Double.parseDouble(tedol), modinfor, 0, vlol, vrol,
						tmpT, tqual, vqual);

				tempT[0] += tmpT[0];
				tempT[1] += tmpT[1];

				kviVal.kvi_insert(vlol, vrol, modinfor, 0,
						Double.parseDouble(tstol), Double.parseDouble(tedol),
						tmpT, vqual, tqual);

				valT[0] += tmpT[0];
				valT[1] += tmpT[1];

				// String row = rawid[0] + "," + tstol + "," + tedol;
				//
				// if (fhb.put("tempM1", row, "attri", "vl",
				// Double.toString(vlol)) == 0)
				// System.out.println("data mode1 failure");
				// if (fhb.put("tempM1", row, "attri", "vr",
				// Double.toString(vrol)) == 0)
				// System.out.println("data model1 failure");
				//
				// if (fhb.put("tempM1", row, "model", "cof1",
				// Double.toString(modCoeOl)) == 0)
				// System.out.println("data model1 failure");

				// ......................................................//
			}
			// if (numcnt != 0) {
			segn += segncnt;

			precmx = cmx;
			precmi = cmi;
			presegsum = segsum;
			presegpcnt = segpcnt;
			preBegStmp = begstamp;

			inTemp[0] = tempT[0] / segncnt;
			inTemp[1] = tempT[1] / segncnt;

			inVal[0] = valT[0] / segncnt;
			inVal[1] = valT[1] / segncnt;
			// }

		} catch (IOException ioe) {

			System.out.println("no file connection");
		}

		return;
	}

	public String valfill(int val, int posmax, int negmin) {
		String str = Integer.toString(val), res = "";
		int bitnum = 0, bitval = 0;
		if (val >= 0) {
			bitnum = (int) Math.log10(posmax);
			bitval = (int) Math.log10(val);
		} else {
			bitnum = (int) Math.log10(-1 * negmin);
			bitval = (int) Math.log10(-1 * val);
			str = str.substring(1, str.length());
			res += "-";
		}

		if (bitval < bitnum) {
			for (int i = 0; i < bitnum - bitval; i++)
				res += "0";
		}
		// while (bitval < bitnum) {
		// res += "0";
		// bitval++;
		// }
		res += str;
		return res;
	}

	public void onlineMod2Load(int tsno, double er, File file) {

		String[][] rawdata = new String[3][10];
		int rds = 0;

		int cmx = 0, cmi = 0;
		double segsum = 0.0;// cmx = 0.0, cmi = 0.0, segsum = 0.0;
		int bkmx = 0, bkmi = 0;
		// double bkmx = 0.0, bkmi = 0.0;
		int segpcnt = 0, segncnt = 0;
		double vrol = 0.0, vlol = 0.0, modCoeOl = 0.0;
		String tedol = new String(), tstol = new String();

		String begstamp = new String();
		// String ministamp = new String();

		String tmpa = new String(), tmpb = new String();

		int tempfileini = 0;

		try {

			FileReader confr = new FileReader("conf.txt");
			BufferedReader confbr = new BufferedReader(confr);
			String confstr = confbr.readLine();
			StringTokenizer confst = new StringTokenizer(confstr, ",");
			confst.nextToken();
			int tmpsegn = Integer.parseInt(confst.nextToken());
			int bitnum = (int) Math.log10(tmpsegn);

			confstr = confbr.readLine();
			confst = new StringTokenizer(confstr, ",");
			confst.nextToken();
			int valmin = Integer.parseInt(confst.nextToken());

			confstr = confbr.readLine();
			confst = new StringTokenizer(confstr, ",");
			confst.nextToken();
			int valmax = Integer.parseInt(confst.nextToken());

			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String line = "";
			int tmp = 0, ini = 1, isnull = 0;
			line = br.readLine();
			StringTokenizer st; // = new StringTokenizer(line, ",");

			while ((line = br.readLine()) != null) {

				// StringTokenizer st = new StringTokenizer(line, ",");
				st = new StringTokenizer(line, ",");

				tmp = 0;
				while (st.hasMoreTokens()) {

					rawdata[rds][tmp++] = st.nextToken();

					if (tmp == 3)
						break;
					if (rawdata[rds][tmp - 1].equals("null")) {
						isnull = 1;
						break;
					}
					if (ini == 1) {

						if (loadini == 1) {
							if (tmp != 1) {
								tsRange[tmp - 1][0] = Integer
										.parseInt(rawdata[rds][tmp - 1]);
								tsRange[tmp - 1][1] = Integer
										.parseInt(rawdata[rds][tmp - 1]);
							}
							if ((tmp - 1) == tsno) {
								cmx = Integer.parseInt(rawdata[rds][tsno]);
								cmi = Integer.parseInt(rawdata[rds][tsno]);
							}
							begstamp = rawdata[rds][0];
							ministamp = rawdata[rds][0];

						} else {

							cmx = precmx;
							cmi = precmi;

							segsum = presegsum;
							segpcnt = presegpcnt;
							begstamp = preBegStmp;

							tempfileini = 1;
						}

					} else {

						if (rawdata[rds][0].compareTo(rawdata[1 - rds][0]) == 0) {
							isnull = 1;
							break;
						}
						if ((tmp - 1) == tsno) {
							int curval = Integer
									.parseInt(rawdata[rds][tmp - 1]);
							if (curval < tsRange[tmp - 1][0])
								tsRange[tmp - 1][0] = curval;
							else if (curval >= tsRange[tmp - 1][1])
								tsRange[tmp - 1][1] = curval;
						}
					}
				}
				ini = 0;
				loadini = 0;

				if (isnull == 1) {
					isnull = 0;
				} else {
					// rawdata[rds][0] = onlineTimePro(rawdata[rds][0]);
					rawdCol = tmp;

					// .........................segmentation.............................//
					if (rawdata[tsno].equals("null") == true) {
						continue;
					}
					int valtmp = Integer.parseInt(rawdata[rds][tsno]);
					maxstamp = rawdata[rds][0];

					segsum += valtmp;
					segpcnt++;

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

					if (tempfileini == 1) {
						tempfileini = 0;
						rds = 1 - rds;
						continue;
					}

					double terr = 0.0;
					terr = er * (double) (segsum) / segpcnt;

					if (Math.abs(cmx - cmi) > Math.abs(2 * terr)) {

						if (sig1 == 1)
							vrol = bkmx;
						else
							vrol = cmx;

						if (sig2 == 1)
							vlol = bkmi;
						else
							vlol = cmi;

						segncnt++;
						tstol = begstamp;
						tedol = (rawdata[1 - rds][0]);
						modCoeOl = (double) (segsum - valtmp) / (segpcnt - 1);

						segpcnt = 2;
						begstamp = rawdata[1 - rds][0];
						bkmi = Integer.parseInt(rawdata[1 - rds][tsno]);
						bkmx = Integer.parseInt(rawdata[1 - rds][tsno]);
						cmx = Integer.parseInt(rawdata[rds][tsno]);
						cmi = Integer.parseInt(rawdata[rds][tsno]);
						segsum = Double.parseDouble(rawdata[rds][tsno])
								+ Double.parseDouble(rawdata[1 - rds][tsno]);

						// ...............data
						// model2...........................//

						int bni = (int) Math.log10(segn + segncnt);
						String rownum = "";

						if (bni < bitnum) {
							for (int k = 0; k < (bitnum - bni); ++k)
								rownum += "0";
						}
						rownum += Integer.toString(segn + segncnt);

						if (fhb.put("tempM2", rawid[0] + rownum, "attri", "vl",
								Double.toString(vlol)) == 0)
							System.out.println("data model1 failure");
						if (fhb.put("tempM2", rawid[0] + rownum, "attri", "vr",
								Double.toString(vrol)) == 0)
							System.out.println("data model1 failure");

						if (fhb.put("tempM2", rawid[0] + rownum, "attri",
								"tst", tstol) == 0)
							System.out.println("data model1 failure");
						if (fhb.put("tempM2", rawid[0] + rownum, "attri",
								"ted", tedol) == 0)
							System.out.println("data model1 failure");

						if (fhb.put("tempM2", rawid[0] + rownum, "model",
								"cof1", Double.toString(modCoeOl)) == 0)
							System.out.println("data model1 failure");

						tmpa = valfill((int) vlol, valmax, valmin);
						tmpb = valfill((int) vrol, valmax, valmin);
						// System.out.println(tmpa+tmpb+"\n");
						//
						if (fhb.put("tempM2V", tmpa + ',' + tmpb, "attri",
								"no", rownum) == 0)
							System.out.println("data model1 failure");

						// ......................................................//
					}
					rds = 1 - rds;
				}
			}

			if (isend == 1) {
				segncnt++;
				tstol = begstamp;
				tedol = (rawdata[1 - rds][0]);
				modCoeOl = (double) (segsum) / (segpcnt - 1);

				// ...............data
				// model2...........................//

				int bni = (int) Math.log10(segncnt + segn);
				String rownum = "";

				if (bni < bitnum) {
					for (int k = 0; k < (bitnum - bni); ++k)
						rownum += "0";
				}
				rownum += Integer.toString(segn + segncnt);

				if (fhb.put("tempM2", rawid[0] + rownum, "attri", "vl",
						Double.toString(vlol)) == 0)
					System.out.println("data model1 failure");
				if (fhb.put("tempM2", rawid[0] + rownum, "attri", "vr",
						Double.toString(vrol)) == 0)
					System.out.println("data model1 failure");

				if (fhb.put("tempM2", rawid[0] + rownum, "attri", "tst", tstol) == 0)
					System.out.println("data model1 failure");
				if (fhb.put("tempM2", rawid[0] + rownum, "attri", "ted", tedol) == 0)
					System.out.println("data model1 failure");

				if (fhb.put("tempM2", rawid[0] + rownum, "model", "cof1",
						Double.toString(modCoeOl)) == 0)
					System.out.println("data model1 failure");

				// if (fhb.put("tempM2V",valfill((int)vlol,valmax,valmin) + ','+
				// valfill((int)vrol,valmax,valmin),
				// "attri", "no", rownum) == 0)
				// System.out.println("data model1 failure");

				// ......................................................//
			}
			segn += segncnt;
			precmx = cmx;
			precmi = cmi;
			presegsum = segsum;
			presegpcnt = segpcnt;
			preBegStmp = begstamp;

		} catch (IOException ioe) {

			System.out.println("no file connection");
		}

		return;
	}

	public void onlineMod2Comple(int tsno) {

		tsno = tsno - 1;

		String tabName = (rawid[tsno] + "Map");
		fhb.creTab(tabName);

		fhb.scanGloIni(rawid[tsno] + "M2V");
		String tmpRes = "";
		int cnt = 1;

		String rownum = new String();

		int rowcnt = 0;

		while ((tmpRes = fhb.scanNext("attri", "no")) != "NO") {
			rowcnt++;
		}

		fhb.scanGloIni(rawid[tsno] + "M2V");
		int bitnum = (int) Math.log10(rowcnt);

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

	public void qryGen_paraload() throws IOException {
		try {
			double[] p = new double[3];
			FileReader confr;
			confr = new FileReader("./confQuery.txt");
			BufferedReader confbr = new BufferedReader(confr);
			String confstr = confbr.readLine();
			StringTokenizer confst = new StringTokenizer(confstr, ",");
			// confst.nextToken();

			p[0] = Double.parseDouble(confst.nextToken());
			p[1] = Double.parseDouble(confst.nextToken());

			sttime = p[0];
			edtime = p[1];

			// .......unit test......//

			// System.out.print(Double.toString(p[0]) + ","
			// + Double.toString(p[1])+"\n");
			// ......................//

			confstr = confbr.readLine();
			confst = new StringTokenizer(confstr, ",");
			// confst.nextToken();

			p[0] = Double.parseDouble(confst.nextToken());
			p[1] = Double.parseDouble(confst.nextToken());

			minval = p[0];
			maxval = p[1];

			// .......unit test......//

			// System.out.print(Double.toString(p[0]) + ","
			// + Double.toString(p[1]));
			// ......................//

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return;
	}

	public void qryGen_tempRange(double inter[]) {// inter[0]:left

		double inteRang = (edtime - sttime);
		double gap = (edtime - sttime) * qrysel;
		long leftbd = 0, rightbd = 0;
		leftbd = (long) (sttime + Math.random() * inteRang);
		rightbd = (long) (leftbd + gap);
		if (rightbd > edtime) {
			rightbd = (long) edtime;
		}
		inter[0] = leftbd;
		inter[1] = rightbd;

		// // ........test..............//
		// inter[0] = trangExp[trangExpCnt];
		// inter[1] = trangExp[trangExpCnt + 1];
		// trangExpCnt += 2;

		return;
	}

	public void qryGen_tempPoint(double inter[]) {// inter[0]:left

		double inteRang = edtime - sttime;
		long pnt = 0;
		pnt = (long) (sttime + Math.random() * inteRang);
		if (pnt > edtime) {
			pnt = (long) edtime;
		}
		inter[0] = pnt;
		return;
	}

	public void qryGen_valRange(double inter[]) {// inter[0]:left

		// double inteRang = maxval - minval;

		double inteRang = (maxval - minval);
		double gap = (maxval - minval) * qrysel;

		double leftbd = 0, rightbd = 0;
		// leftbd = minval + Math.random() * inteRang;
		leftbd = minval + Math.random() * inteRang;

		// .......modification..................//
		// leftbd = 0.0 + Math.random() * inteRang;

		rightbd = leftbd + gap;
		if (rightbd > maxval) {
			rightbd = maxval;
		}
		if (rightbd < 0) {
			rightbd = 0;
		}

		inter[0] = leftbd;
		inter[1] = rightbd;

		return;
	}

	public void qryGen_valPoint(double inter[]) {// inter[0]:left

		// double inteRang = maxval - minval;
		double inteRang = maxval;
		double pnt = 0.0;
		// pnt = (minval + Math.random() * inteRang);
		pnt = (0 + Math.random() * inteRang);

		if (pnt < 0) {
			pnt = 0;
		}
		inter[0] = pnt;
		return;
	}

	public void textClear()// 0: sroot 1:sdel
	// 1st line: for temporal index 2nd line: value index
	{
		FileWriter fstream;
		try {
			fstream = new FileWriter("./conf.txt", false);
			BufferedWriter out = new BufferedWriter(fstream);
			String str = "";
			out.write(str);
			// out.write("\n");
			out.close();

			fstream = new FileWriter("./confQuery.txt", false);
			out = new BufferedWriter(fstream);
			out.write(str);
			// out.write("\n");
			out.close();

			fstream = new FileWriter("./insertion.txt", false);
			out = new BufferedWriter(fstream);
			out.write(str);
			// out.write("\n");
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return;
	}

	public void textInsertionPer(double tempidx[], double validx[])// 0: sroot
																	// 1:sdel
	// 1st line: for temporal index 2nd line: value index
	{
		FileWriter fstream;
		try {
			fstream = new FileWriter("./insertion.txt", true);
			BufferedWriter out = new BufferedWriter(fstream);
			String str = Double.toString(tempidx[0]) + ","
					+ Double.toString(tempidx[1]) + ","
					+ Double.toString(validx[0]) + ","
					+ Double.toString(validx[1]);
			// out.append(str);
			out.write(str);
			out.write("\n");
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return;
	}

	public void textInsertionPer(double tempcnt[])// for simple model view
	// 1:sdel
	// 1st line: for temporal index 2nd line: value index
	{
		FileWriter fstream;
		try {
			fstream = new FileWriter("./insertionMod1.txt", true);
			BufferedWriter out = new BufferedWriter(fstream);
			String str = Double.toString(tempcnt[0]);
			// out.append(str);
			out.write(str);
			out.write("\n");
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return;
	}

	public void textInsertionPer(double tempcnt[], String filename)// for simple
																	// model
																	// view
	// 1:sdel
	// 1st line: for temporal index 2nd line: value index
	{
		FileWriter fstream;
		try {
			fstream = new FileWriter("./" + filename, true);
			BufferedWriter out = new BufferedWriter(fstream);
			String str = Double.toString(tempcnt[0]);
			// out.append(str);
			out.write(str);
			out.write("\n");
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return;
	}

	public void textRecord(int cnt, double paraval[])// 0: sroot 1:sdel
	// 1st line: for temporal index 2nd line: value index
	{
		FileWriter fstream;
		try {
			fstream = new FileWriter("./conf.txt", true);
			BufferedWriter out = new BufferedWriter(fstream);
			String str = Double.toString(paraval[0]) + ","
					+ Double.toString(paraval[1]);
			// out.append(str);
			out.write(str);
			out.write("\n");
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return;
	}

	public void paramRecord() {
		double[] para = new double[20];
		int num = 0;
		num = kviTemp.paraOut(para);
		textRecord(num, para);
		para = new double[20];
		num = kviVal.paraOut(para);
		textRecord(num, para);
		return;
	}

	public void paramLoad() throws IOException {

		try {
			double[] p = new double[3];
			FileReader confr;
			confr = new FileReader("./conf.txt");
			BufferedReader confbr = new BufferedReader(confr);
			String confstr = confbr.readLine();
			StringTokenizer confst = new StringTokenizer(confstr, ",");
			// confst.nextToken();

			p[0] = Double.parseDouble(confst.nextToken());
			p[1] = Double.parseDouble(confst.nextToken());

			kviTemp.paraSetup(2, p);

			// .......unit test......//

			// System.out.print(Double.toString(p[0]) + ","
			// + Double.toString(p[1]));
			// ......................//

			confstr = confbr.readLine();
			confst = new StringTokenizer(confstr, ",");
			// confst.nextToken();

			p[0] = Double.parseDouble(confst.nextToken());
			p[1] = Double.parseDouble(confst.nextToken());

			kviVal.paraSetup(2, p);

			// .......unit test......//
			//
			// System.out.print(Double.toString(p[0]) + ","
			// + Double.toString(p[1])+"\n");
			// ......................//

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return;
	}

	public void textRecordExp_clear(String filename) {
		FileWriter fstream;
		try {
			fstream = new FileWriter(filename, false);
			BufferedWriter out = new BufferedWriter(fstream);
			String str = "";
			out.write(str);
			// out.write("\n");
			out.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void textRecordExp(String filename, double timetup[][],
			String range[], int numway, double l, double r) {
		FileWriter fstream;
		try {
			fstream = new FileWriter(filename, true);
			BufferedWriter out = new BufferedWriter(fstream);
			String str = "";
			for (int i = 0; i < numway; ++i) {
				if (i == 0) {
					str = (Double.toString(timetup[i][0]) + ","
							+ Double.toString(timetup[i][1]) + ','
							+ Double.toString(timetup[i][2]) + ","
							+ Double.toString(timetup[i][3]) + ","
							+ Double.toString(timetup[i][4]) + ","
							+ Double.toString(timetup[i][5]) + ",");
					// + range[0]
					// + ',' + range[1] + ',');
				} else {

					str += (Double.toString(timetup[i][0]) + ","
							+ Double.toString(timetup[i][1]) + ',');
				}
			}

			// ....modification.............//
			str += (Double.toString(timetup[0][6]) + ",");
			str += (Double.toString(timetup[0][7]) + ",");
			// .............................//

			str += (Double.toString(l) + ",");
			str += (Double.toString(r) + ",");
			out.write(str);
			out.write("\n");
			out.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	public void textRecordExpKVIMRK(String filename, double timetup[][],
			String range[], int numway, double l, double r) {
		FileWriter fstream;
		try {
			fstream = new FileWriter(filename, true);
			BufferedWriter out = new BufferedWriter(fstream);
			String str = "";
			for (int i = 0; i < numway; ++i) {
			
					str  = str+ ( (Double.toString(timetup[i][0]) + ","
							+ Double.toString(timetup[i][1]) + ','
							+ Double.toString(timetup[i][2]) + ","
							+ Double.toString(timetup[i][3]) + ","
							+ Double.toString(timetup[i][4]) + ","
							+ Double.toString(timetup[i][5]) + ","));
					str += (Double.toString(timetup[i][6]) + ",");
					str += (Double.toString(timetup[i][7]) + ",");
			}

		

			str += (Double.toString(l) + ",");
			str += (Double.toString(r) + ",");
			out.write(str);
			out.write("\n");
			out.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void textRecordExpKviTest(String filename, double timetup[][],
			int numway, double l, double r) {
		FileWriter fstream;

		// ....test..........//
		System.out.printf("%d\n", numway);

		try {
			fstream = new FileWriter(filename, true);
			BufferedWriter out = new BufferedWriter(fstream);
			String str = "";
			for (int i = 0; i < numway; ++i) {

				str += (Double.toString(timetup[i][0]) + ","
						+ Double.toString(timetup[i][1]) + ','
						+ Double.toString(timetup[i][2]) + ","
						+ Double.toString(timetup[i][3]) + ","
						+ Double.toString(timetup[i][4]) + ","
						+ Double.toString(timetup[i][5]) + ",");

				str += (Double.toString(timetup[i][6]) + ",");
				str += (Double.toString(timetup[i][7]) + ",");

				// out.write(str);
				// out.write("\n");

			}

			str += (Double.toString(l) + ",");
			str += (Double.toString(r) + ",");
			out.write(str);
			out.write("\n");
			out.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void textRecordExpKviTest(String filename, double timetup[][],
			int numway, double l, double r, String rkbd) {
		FileWriter fstream;

		// ....test..........//
		// System.out.printf("%d\n", numway);

		try {
			fstream = new FileWriter(filename, true);
			BufferedWriter out = new BufferedWriter(fstream);
			String str = "";
			for (int i = 0; i < numway; ++i) {

				if (i == 0)
				{
					str += (Double.toString(timetup[i][0])
							+ ","
							 + Double.toString(timetup[i][1]) + ','
							 + Double.toString(timetup[i][2]) + ","
							 + Double.toString(timetup[i][3]) + ","
							+ Double.toString(timetup[i][4]) + ","
							+ Double.toString(timetup[i][5]) + ",");

					str += (Double.toString(timetup[i][6]) + ",");
					str += (Double.toString(timetup[i][7]) + ",");

				} else {
					str += (Double.toString(timetup[i][0]) + ",");
					str += (Double.toString(timetup[i][1]) + ",");
				}
				// out.write(str);
				// out.write("\n");

			}

			// ........modification......................//
			// str += (Double.toString(timetup[1][0]) + ",");
			// str += (Double.toString(timetup[1][1]) + ",");
			// str += (Double.toString(timetup[2][0]) + ",");
			// str += (Double.toString(timetup[2][1]) + ",");

			// ............................................//

			str += (Double.toString(l) + ",");
			str += (Double.toString(r) + ",");
			
			//......for test.....................//
            //	str += rkbd;
			
			out.write(str);
			out.write("\n");
			out.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void textRecordExp(String filename, double timetup[][], int numway) {
		FileWriter fstream;
		try {
			fstream = new FileWriter(filename, true);
			BufferedWriter out = new BufferedWriter(fstream);
			String str = "";
			for (int i = 0; i < numway; ++i) {

				str += (Double.toString(timetup[i][0]) + ","
						+ Double.toString(timetup[i][1]) + ',');
			}
			out.write(str);
			out.write("\n");
			out.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void textRecordExp(String filename, double timetup[][],
			String range[], int numway, double l) {
		FileWriter fstream;
		try {
			fstream = new FileWriter(filename, true);
			BufferedWriter out = new BufferedWriter(fstream);
			String str = "";
			for (int i = 0; i < numway; ++i) {
				if (i == 0) {
					str = (Double.toString(timetup[i][0]) + ","
							+ Double.toString(timetup[i][1]) + ',');
				} else {

					str += (Double.toString(timetup[i][0]) + ","
							+ Double.toString(timetup[i][1]) + ',');
				}
			}
			str += (Double.toString(l) + ",");
			out.write(str);
			out.write("\n");
			out.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void textRecordExp(String filename, double timecnt[], int num,
			double val) {
		FileWriter fstream;
		try {
			fstream = new FileWriter(filename, true);
			BufferedWriter out = new BufferedWriter(fstream);
			String str = "";
			for (int i = 0; i < num; ++i) {
				str += (Double.toString(timecnt[i]) + ",");
			}
			str += (Double.toString(val) + ",");
			// str+=(Double.toHexString(r)+",");
			out.write(str);
			out.write("\n");
			out.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public String prefixCon(long regval) {
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

	public void kvitiExp(int num) throws Exception {
		double[] inte = new double[10];
		comCheck();
		double[] tmpstat = new double[15];
		double[][] stat = new double[15][10];
		String[] tuprange = new String[15];

		int tnum = 0;
		double qualnum = 0.0;

		textRecordExp_clear("./result/kvitiExpcacheup.txt");

		String[] qual = { "st", "ed" };
		qrysel = 0.0;
		kviTemp.kviCol.stedPathRecord();

		for (int i = 0; i < num; ++i) {

			if (i % 10 == 0) {
				qrysel = qrysel + 0.05;
			}

			qryGen_tempRange(inte);
			tnum = 0;

			System.out.printf("%f    %f\n", inte[0], inte[1]);

			// kviTemp.kvi_rangeSearch(inte[0], inte[1], qual,0);
			// kviTemp.kvi_metricOutput(tmpstat, tuprange);
			// stat[tnum][0] = tmpstat[0] - 1.0; // tdur
			// stat[tnum][1] = tmpstat[1];// inmem
			// stat[tnum][2] = tmpstat[2];// randacc
			// stat[tnum][3] = tmpstat[3];// mracc
			// stat[tnum][4] = tmpstat[4];// # tuples
			// stat[tnum][5] = tmpstat[5];// search level
			// stat[tnum][6] = tmpstat[6];// # map output
			// stat[tnum++][7] = tmpstat[7];// #map
			// System.out.printf("%f  \n", kviTemp.kvi_timeCnt());
			
//			kviTemp.kvi_rangeSearchTrivial(inte[0], inte[1], qual, 0);

			kviTemp.kvi_rangeSearchTest(inte[0], inte[1], qual, 0);
			kviTemp.kvi_metricOutput(tmpstat, tuprange);
			stat[tnum][0] = tmpstat[0] - 1.0; // tdur
			stat[tnum][1] = tmpstat[1];// inmem
			stat[tnum][2] = tmpstat[2];// randacc
			stat[tnum][3] = tmpstat[3];// mracc
			stat[tnum][4] = tmpstat[4];// # map input tuples
			stat[tnum][5] = tmpstat[5];// search level
			stat[tnum][6] = tmpstat[6];// # map output
			stat[tnum++][7] = tmpstat[7];// #map
			System.out.printf("%f  \n", kviTemp.kvi_timeCnt());

//			MRScan.gTabNum = 0;
//			startCnt();
//			qualnum = mrscanner.jobTempRange(inte[0], inte[1], "tempIdxmodCol");
//			stat[tnum][0] = stopCnt();
//			stat[tnum++][1] = 100;
//
//			stat[tnum][0] = kviMRproCol.numMapIn;
//			stat[tnum++][1] = qualnum;

			textRecordExpKviTest("./result/kvitiExpcacheup.txt", stat, tnum, inte[0],
					inte[1], kviTemp.strk + "," + kviTemp.edrk);
		}

		return;
	}

	public void kviviExp(int num) throws Exception {
		double[] inte = new double[10];
		comCheck();
		double[] tmpstat = new double[15];
		double[][] stat = new double[15][10];
		String[] tuprange = new String[15];

		int tnum = 0;
		// double qualnum = 0.0;

		textRecordExp_clear("./result/kviviExp.txt");

		String[] qual = { "vl", "vr" };
		qrysel = 0.1;
		kviVal.kviCol.vlvrPathRecord();

		for (int i = 0; i < num; ++i) {

			if (i % 10 == 0) {
				qrysel = qrysel + 0.1;
			}

			qryGen_valRange(inte);
			tnum = 0;

			System.out.printf("%f    %f\n", inte[0], inte[1]);

			// kviVal.kvi_rangeSearch(inte[0], inte[1], qual, 1);
			// kviVal.kvi_metricOutput(tmpstat, tuprange);
			// stat[tnum][0] = tmpstat[0]; // tdur
			// stat[tnum][1] = tmpstat[1];// inmem
			// stat[tnum][2] = tmpstat[2];// randacc
			// stat[tnum][3] = tmpstat[3];// mracc
			// stat[tnum][4] = tmpstat[4];// # tuples
			// stat[tnum++][5] = tmpstat[5];// search level
			// stat[tnum][6] = tmpstat[6];// # map output
			// stat[tnum++][7] = tmpstat[7];// #map
			// System.out.printf("%f  \n", kviVal.kvi_timeCnt());

			kviVal.kvi_rangeSearchTest(inte[0], inte[1], qual, 1);
			kviVal.kvi_metricOutput(tmpstat, tuprange);
			stat[tnum][0] = tmpstat[0] - 1.0; // tdur
			stat[tnum][1] = tmpstat[1];// inmem
			stat[tnum][2] = tmpstat[2];// randacc
			stat[tnum][3] = tmpstat[3];// mracc
			stat[tnum][4] = tmpstat[4];// # tuples
			stat[tnum][5] = tmpstat[5];// search level
			stat[tnum][6] = tmpstat[6];// # map output
			stat[tnum++][7] = tmpstat[7];// #map
			System.out.printf("%f  \n", kviVal.kvi_timeCnt());

			textRecordExpKviTest("./result/kviviExp.txt", stat, tnum, inte[0],
					inte[1], kviVal.strk + "," + kviVal.edrk);
		}

		return;
	}

	public void tiExp(int num) throws Exception {
		double[] inte = new double[10];
		comCheck();
		double[] tmpstat = new double[15];
		double[][] stat = new double[15][10];
		String[] tuprange = new String[15];

		int tnum = 0;
		double qualnum = 0.0;

		textRecordExp_clear("./result/tiExpMapT.txt");

		String[] qual = { "st", "ed" };
		qrysel = 0.1;

		for (int i = 0; i < num; ++i) {

			if (i % 10 == 0) {
				qrysel = qrysel + 0.1;
			}

			qryGen_tempRange(inte);
			tnum = 0;

			System.out.printf("%f    %f\n", inte[0], inte[1]);

			 kviTemp.kvi_rangeSearch(inte[0], inte[1], qual,0);
			// kviTemp.kvi_metricOutput(tmpstat, tuprange);
			// stat[tnum][0] = tmpstat[0] - 1.0; // tdur
			// stat[tnum][1] = tmpstat[1];// inmem
			// stat[tnum][2] = tmpstat[2];// randacc
			// stat[tnum][3] = tmpstat[3];// mracc
			// stat[tnum][4] = tmpstat[4];// # tuples
			// stat[tnum][5] = tmpstat[5];// search level
			// stat[tnum][6] = tmpstat[6];// # map output
			// stat[tnum++][7] = tmpstat[7];// #map

			System.out.printf("%f  \n", kviTemp.kvi_timeCnt());

			// kviTemp.conve_kvi_rangeSearchConsec(inte[0], inte[1], qual,0);
			// kviTemp.kvi_metricOutput(tmpstat);
			stat[tnum][0] = tmpstat[0];
			stat[tnum++][1] = tmpstat[1];
			System.out.printf("%f  \n", kviTemp.kvi_timeCnt());

			// MRScan.gTabNum = 0;
			// startCnt();
			qualnum = mrscanner.jobTempRange(inte[0], inte[1], "tempIdxmodCol");
			stat[tnum][0] = stopCnt();
			stat[tnum++][1] = 100;
			//
			startCnt();
			// stat[tnum][1] = filterHb.rangeQuery("tempIdxmodCol", inte[0],
			// inte[1], "attri", qual);
			stat[tnum++][0] = stopCnt();

			startCnt();
			// stat[tnum][1] = rawscanner.rangeQryTemp(inte[0], inte[1],
			// "tempRaw_guo");
			stat[tnum++][0] = stopCnt();

			stat[tnum][0] = kviMRproCol.numMapIn;
			stat[tnum++][1] = qualnum;

			// for no-gridding process
			// kviTemp.kvi_rangeSearchNoGrid(inte[0], inte[1], qual);
			// kviTemp.kvi_metricOutput(tmpstat, tuprange);
			stat[tnum++][0] = tmpstat[0] - 1.0; // tdur
			System.out.printf("%f  \n", kviTemp.kvi_timeCnt());

			textRecordExp("./result/tiExpMapT.txt", stat, tuprange, tnum,
					inte[0], inte[1]);
		}

		return;
	}

	public void tpExp(int num) throws Exception {
		double[] inte = new double[10];
		comCheck();

		double[] tmpstat = new double[15];
		double[][] stat = new double[15][10];
		String[] tuprange = new String[15];

		int tnum = 0;
		double qualnum = 0.0;
		String[] qual = { "st", "ed" };

		textRecordExp_clear("./result/tpExpRaw1.txt");
		fhb.creTab("tempM1_guoRes");

		for (int i = 0; i < num; ++i) {
			qryGen_tempPoint(inte);
			tnum = 0;

			System.out.printf("%f    \n", inte[0]);
		
		
			kviTemp.kvi_pointSearch(inte[0], qual, 0);
			kviTemp.kvi_metricOutput(tmpstat);
			stat[tnum][0] = tmpstat[0]; 
			stat[tnum++][1] = tmpstat[1]; // tdur

//			kviTemp.kvi_pointSearch(inte[0], qual, 0);
//			// kviTemp.kvi_metricOutput(tmpstat, tuprange);
//			stat[tnum][0] = tmpstat[0]; // tdur
//			stat[tnum++][1] = tmpstat[4];// # tuples
//
//			System.out.printf("%f  \n", kviTemp.kvi_timeCnt());
//
//			startCnt();
//			// qualnum = mrscanner.jobTempPoint(inte[0], "tempIdxmodCol");
//			stat[tnum][0] = stopCnt();
//			stat[tnum++][1] = 100;
//
//			startCnt();
//			// stat[tnum][1] = filterHb.pointQuery("tempIdxmodCol", inte[0],
//			// "attri", qual);
//			stat[tnum++][0] = stopCnt();
//
		

//			stat[tnum][0] = kviMRproCol.numMapIn;// from kvi
//			stat[tnum++][1] = qualnum;// from pure mapreduce
			
			startCnt();
			stat[tnum][1] = rawscanner.pointQryTemp(inte[0], "tempRaw_guo");
			stat[tnum++][0] = stopCnt();

			textRecordExp("./result/tpExpRaw1.txt", stat, tuprange, tnum,
					inte[0]);
		}

		return;
	}

	public void viExp(int num) throws Exception {
		double[] inte = new double[10];

		comCheck();
		double[] tmpstat = new double[15];
		double[][] stat = new double[15][10];
		String[] tuprange = new String[15];

		int tnum = 0;
		double qualnum = 0.0;
		String[] qual = { "vl", "vr" };

		textRecordExp_clear("./result/viExpMrkHs2.txt");

		qrysel = 0.0;
		for (int i = 0; i < num; ++i) {

			if (i % 10 == 0) {
				qrysel = qrysel + 0.05;
			}

			qryGen_valRange(inte);
			tnum = 0;

			System.out.printf("%f    %f\n", inte[0], inte[1]);
			
			
//			kviVal.kvi_rangeSearch(inte[0], inte[1], qual, 1);
//			kviVal.kvi_metricOutput(tmpstat, tuprange);
//			stat[tnum][0] = tmpstat[0]; // tdur
//			stat[tnum][1] = tmpstat[1];// inmem
//			stat[tnum][2] = tmpstat[2];// randacc
//			stat[tnum][3] = tmpstat[3];// mracc
//			stat[tnum][4] = tmpstat[4];// # tuples
//			stat[tnum][5] = tmpstat[5];// search level
//			
//			stat[tnum][6] = tmpstat[6];// map out
//			stat[tnum++][7] = tmpstat[7];// map cnt
//			System.out.printf("KVI %f  \n", kviVal.kvi_timeCnt());
			

			kviVal.kvi_rangeSearchMrkvi(inte[0], inte[1], qual, 1);
			kviVal.kvi_metricOutput(tmpstat, tuprange);
			stat[tnum][0] = tmpstat[0]; // tdur
			stat[tnum][1] = tmpstat[1];// inmem
			stat[tnum][2] = tmpstat[2];// randacc
			stat[tnum][3] = tmpstat[3];// mracc
			stat[tnum][4] = tmpstat[4];// # tuples
			stat[tnum][5] = tmpstat[5];// search level
			
			stat[tnum][6] = tmpstat[6];// map out
			stat[tnum++][7] = tmpstat[7];// map cnt
			
			System.out.printf("MRK %f  \n", kviVal.kvi_timeCnt());

//			kviVal.conve_kvi_rangeSearchConsec(inte[0], inte[1], qual, 1);
//			kviVal.kvi_metricOutput(tmpstat);
//			stat[tnum][0] = tmpstat[0];
//			stat[tnum++][1] = tmpstat[1];
//			System.out.printf("%f  \n", kviVal.kvi_timeCnt());
//
//			startCnt();
//			qualnum = mrscanner.jobValRange(inte[0], inte[1], "valIdxmodCol");
//			stat[tnum][0] = stopCnt();
//			stat[tnum++][1] = 100;
//
//			startCnt();
//			// stat[tnum][1] = filterHb.rangeQuery("valIdxmodCol", inte[0],
//			// inte[1], "attri", qual);
//			stat[tnum++][0] = stopCnt();
//
//			startCnt();
//			// stat[tnum][1] = rawscanner.rangeQryVal(inte[0], inte[1],
//			// "tempRaw_val");
//			stat[tnum++][0] = stopCnt();
//
//			stat[tnum][0] = kviMRproCol.numMapIn;// from kvi
//			stat[tnum++][1] = qualnum;// from pure mapreduce
//
//			// for no-gridding process
//			// kviVal.kvi_rangeSearchNoGrid(inte[0], inte[1], qual);
//			// kviVal.kvi_metricOutput(tmpstat, tuprange);
//			stat[tnum++][0] = tmpstat[0]; // tdur
//			System.out.printf("%f  \n", kviVal.kvi_timeCnt());

//			textRecordExp("./result/viExpMrkHs.txt", stat, tuprange, tnum,
//					inte[0], inte[1]);
			
			textRecordExpKVIMRK("./result/viExpMrkHs2.txt", stat, tuprange, tnum,
					inte[0], inte[1]);
		}

		return;
	}

	public void vpExp(int num) throws Exception {
		double[] inte = new double[10];
		comCheck();

		double[] tmpstat = new double[15];
		double[][] stat = new double[15][10];
		String[] tuprange = new String[15];

		int tnum = 0;
		double qualnum = 0.0;

		textRecordExp_clear("./result/vpExpMrk.txt");
		// fhb.creTab("tempM1_guoRes");
		String[] qual = { "vl", "vr" };

		for (int i = 0; i < num; ++i) {
			qryGen_valPoint(inte);
			tnum = 0;

			System.out.printf("%f    \n", inte[0]);
			
			
			kviVal.kvi_pointSearch_mr(inte[0], qual, 1);
			kviVal.kvi_metricOutput(tmpstat);
			stat[tnum++][0] = tmpstat[0]; // tdur

			// kviVal.kvi_pointSearch(inte[0], qual,1);
			// kviVal.kvi_metricOutput(tmpstat, tuprange);
//			stat[tnum][0] = tmpstat[0]; // tdur
			// stat[tnum][1] = tmpstat[1];// inmem
			// stat[tnum][2] = tmpstat[2];// randacc
			// stat[tnum][3] = tmpstat[3];// mracc
//			stat[tnum++][1] = tmpstat[4];// # tuples
			// stat[tnum++][5]=tmpstat[5];//search level
			System.out.printf("%f  \n", kviVal.kvi_timeCnt());

//			startCnt();
			// qualnum = mrscanner.jobValPoint(inte[0], "valIdxmodCol");
			// stat[tnum][0] = stopCnt();
//			stat[tnum++][1] = 100;

//			startCnt();
			// stat[tnum][1] = filterHb.pointQuery("valIdxmodCol", inte[0],
			// "attri", qual);
//			stat[tnum++][0] = stopCnt();

//			startCnt();
//			stat[tnum][1] = rawscanner.pointQryVal(inte[0], "tempRaw_val");
//			stat[tnum++][0] = stopCnt();

//			stat[tnum][0] = kviMRproCol.numMapIn;// from kvi
//			stat[tnum++][1] = qualnum;// from pure mapreduce

			textRecordExp("./result/vpExpMrk.txt", stat, tuprange, tnum,
					inte[0]);
		}
		return;
	}

	public void valScanExpGener()// for both value point and range query
	{
		double[] inte = new double[10];
		comCheck();
		double[][] stat = new double[15][10];
		int tnum = 0;

		textRecordExp_clear("./result/generalScanExp.txt");
		qryGen_valPoint(inte);

		startCnt();
		stat[tnum][1] = scanfilter.pointQryVal(inte[0], "tempM1_guo");
		stat[tnum++][0] = stopCnt();

		// startCnt();
		// stat[tnum][1] = rawscanner.pointQryVal(inte[0], "tempRaw_guo");
		// stat[tnum++][0] = stopCnt();

		textRecordExp("./result/generalScanExp.txt", stat, tnum);

		return;
	}

	public void tempScanExpGener()// for both value point and range query
	{
		double[] inte = new double[10];
		comCheck();
		double[][] stat = new double[15][10];
		int tnum = 0;

		qrysel = 0.0;

		for (int i = 0, num = 20; i < num; ++i) {

			if (i % 5 == 0) {
				qrysel = qrysel + 0.05;
			}

			qryGen_tempRange(inte);
			tnum = 0;

			System.out.printf("%f    %f\n", inte[0], inte[1]);

			startCnt();
			stat[tnum][1] = scanfilter.rangeQryTemp(inte[0], inte[1],
					"tempM1_guo");
			stat[tnum++][0] = stopCnt();

			startCnt();
			// stat[tnum][1] = rawscanner.rangeQryTemp(inte[0], inte[1],
			// "tempRaw_guo");
			stat[tnum++][0] = stopCnt();

			startCnt();
			stat[tnum][1] = scanfilter.pointQryTemp(inte[0], "tempM1_guo");
			stat[tnum++][0] = stopCnt();

			startCnt();
			stat[tnum][1] = rawscanner.pointQryTemp(inte[0], "tempRaw_guo");
			stat[tnum++][0] = stopCnt();

			// textRecordExp_clear("./result/generalScanExp.txt");
			textRecordExp("./result/generalScanExp.txt", stat, tnum);
		}

		return;
	}

	public void geneicScanExp() {

		textRecordExp_clear("./result/generalScanExp.txt");
		// valScanExpGener();
		tempScanExpGener();
		return;
	}

	public void testExp(int num) throws Exception {
		double[] inte = new double[10];
		comCheck();

		double[] tmpstat = new double[15];
		double[][] stat = new double[15][10];
		String[] tuprange = new String[15];

		int tnum = 0;

		textRecordExp_clear("./result/testExp.txt");
		fhb.creTab("tempTestRes");

		for (int i = 0; i < num; ++i) {
			qryGen_valPoint(inte);
			tnum = 0;

			System.out.printf("%f    \n", inte[0]);

			startCnt();
			mrtest.jobValPoint(inte[0], "tempM1_guo");
			stat[tnum][0] = stopCnt();
			stat[tnum++][1] = 100;
		}

		return;
	}

	public void testExpOpt(int num) throws Exception {

		// double[] inte = new double[10];
		// comCheck();
		// double[] tmpstat = new double[15];
		// double[][] stat = new double[15][10];
		// String[] tuprange = new String[15];
		//
		// int tnum = 0;
		//
		// kviMRpro mrtest = new kviMRpro(0, "tempIdxmod");
		// String[] qual = { "st", "ed" };
		//
		// fhb.creTab("tempM1_guoRes");
		// qrysel = 0.0;
		//
		// long st = 0, ed = 0;
		//
		// for (int i = 0; i < num; ++i) {
		//
		// if (i % 5 == 0) {
		// qrysel = qrysel + 0.1;
		// }
		//
		// qryGen_tempRange(inte);
		// tnum = 0;
		// System.out.printf("%f    %f\n", inte[0], inte[1]);
		//
		// kviTemp.kvi_rangeSearch(inte[0], inte[1], qual);
		// kviTemp.kvi_metricOutput(tmpstat, tuprange);
		// System.out.printf("%f  \n", kviTemp.kvi_timeCnt());
		// stat[tnum++][0] = tmpstat[0]; // tdur
		//
		// st = (long) inte[0];
		// ed = (long) inte[1];
		// startCnt();
		// mrtest.jobIntervalEvalQuery(inte[0], inte[1], "tempIdxmod",
		// prefixCon(st) + ",0", prefixCon(ed + 1) + ",0");
		// stat[tnum++][0] = stopCnt();
		//
		// textRecordExp("./result/opttest.txt", stat, tnum);
		//
		// }
		//
		// return;

	}

	public void testRawMod() throws Exception {

		File folder = new File("/home/guo/workspace/dataset4");
		List<File> list = new ArrayList<File>();
		getFiles(folder, list, "xa");

		Collections.sort(list);
		int num = list.size();
		num = 1;
		loadini = 1;
		File curfile;
		long filelen = 0;

		for (int i = 0; i < num; i++) {

			// .....file check.......//
			curfile = list.get(i);
			filelen = curfile.length();
			if (filelen == 0)
				continue;
			// .....................//

			if (i == (num - 1))
				isend = 1;
			onlineMod1LoadText(1, errpre, list.get(i));
			System.out.print(list.get(i) + "\n");
		}
		return;

	}

	public void comCheck() {
		fhb.get("tempIdxmod", "j.100334", "model", "coef1");
	}

	public static void main(String[] args) throws Exception {

		String sign;
		int expnum = 0;
		sign = args[0];
		// sign="expti";

		if (sign.equals("l0") == true) {// kvi

			QueryManager qrypro = new QueryManager(true);
			qrypro.textClear();
			qrypro.ImportFolder("/home/guo/workspace/dataset4", "xa", 4);
			qrypro.paramRecord();

		} else if (sign.equals("l1") == true)// tempM1
		{
			QueryManager qrypro = new QueryManager(false);
			qrypro.ImportFolder("/home/guo/workspace/dataset4", "xa", 1);
		} else if (sign.equals("l2") == true)// raw data
		{
			QueryManager qrypro = new QueryManager(false);
			qrypro.ImportFolder("/home/guo/workspace/dataset4", "xa", 0);
		}

		else if (sign.equals("expkviti") == true) {
			expnum = Integer.parseInt(args[1]);
			// expnum=1;

			QueryManager qrypro = new QueryManager(false);
			try {
				qrypro.paramLoad();
				qrypro.qryGen_paraload();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			qrypro.kvitiExp(expnum);

		} else if (sign.equals("expkvivi") == true) {
			expnum = Integer.parseInt(args[1]);
			// expnum=1;

			QueryManager qrypro = new QueryManager(false);
			try {
				qrypro.paramLoad();
				qrypro.qryGen_paraload();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			qrypro.kviviExp(expnum);

		}

		else if (sign.equals("expti") == true) {
			expnum = Integer.parseInt(args[1]);
			// expnum=1;

			QueryManager qrypro = new QueryManager(false);
			try {
				qrypro.paramLoad();
				qrypro.qryGen_paraload();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			qrypro.tiExp(expnum);

		} else if (sign.equals("exptp") == true) {
			expnum = Integer.parseInt(args[1]);

			QueryManager qrypro = new QueryManager(false);
			try {
				qrypro.paramLoad();
				qrypro.qryGen_paraload();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			qrypro.tpExp(expnum);

		} else if (sign.equals("expvi") == true) {
			expnum = Integer.parseInt(args[1]);

			QueryManager qrypro = new QueryManager(false);
			try {
				qrypro.paramLoad();
				qrypro.qryGen_paraload();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			qrypro.viExp(expnum);

		} else if (sign.equals("expvp") == true) {
			expnum = Integer.parseInt(args[1]);

			QueryManager qrypro = new QueryManager(false);
			try {
				qrypro.paramLoad();
				qrypro.qryGen_paraload();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			qrypro.vpExp(expnum);

		} else if (sign.equals("expgen") == true) {
			// expnum = Integer.parseInt(args[1]);
			QueryManager qrypro = new QueryManager(false);
			try {
				qrypro.paramLoad();
				qrypro.qryGen_paraload();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			qrypro.geneicScanExp();

		} else if (sign.equals("exptest") == true) {
			expnum = Integer.parseInt(args[1]);

			QueryManager qrypro = new QueryManager(false);
			// try {
			// qrypro.paramLoad();
			// qrypro.qryGen_paraload();
			// } catch (IOException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }

			// qrypro.testExp(expnum);
			// qrypro.testExpOpt(expnum);
			qrypro.testRawMod();

		} else if (sign.equals("c0") == true) {
			QueryManager qrypro = new QueryManager(true);
			qrypro.textClear();
			qrypro.ImportFolder("/home/guo/data/accel_data_yan/userAdd/bin",
					"gsensor", 4);
			qrypro.paramRecord();

		} else if (sign.equals("c1") == true) {// temp1
			QueryManager qrypro = new QueryManager(false);
			qrypro.ImportFolder("/home/guo/data/accel_data_yan/user2",
					"gsensor", 1);
		} else if (sign.equals("c2") == true) {// raw
			QueryManager qrypro = new QueryManager(false);
			qrypro.ImportFolder("/home/guo/data/accel_data_yan/userAdd/set5",
					"gsensor", 0);

		} else if (sign.equals("c5") == true) {// raw
			QueryManager qrypro = new QueryManager(false);
			qrypro.ImportFolder("/home/guo/data/accel_data_yan/user2/",
					"2010-03-20-gsensor", 5);

		} else if (sign.equals("c6") == true) {// raw
			QueryManager qrypro = new QueryManager(false);
			qrypro.ImportFolder("/home/guo/data/accel_data_yan/user3/",
					"gsensor", 6);

		}
		System.out.printf("the number of tuples in index table: %f\n",
				MRScan.gTabNum);
		return;
	}

}
