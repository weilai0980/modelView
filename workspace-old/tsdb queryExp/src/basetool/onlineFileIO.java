package basetool;

import basetool.segm;

import java.io.*;
import java.util.*;

public class onlineFileIO {

	public static int rawdCol;

	public static Integer[][] tsRange = new Integer[30][5];// min 0, max 1

	public static int rawn;
	public int segn;
	public String[] rawid = { "temp", "hum", "co", "co2", "co24", "no23", "ts" };

	public static HBaseOp fhb = new HBaseOp();
	public static int loadini, isend;

	// segmentation state record//
	public static int precmx, precmi, prebkmx, prebkmi;
	public static double presegsum;
	public static int presegpcnt;
	public static String preBegStmp;
	
	public static String ministamp;
	public static String maxstamp;
	// ...........................//

	public double errpre;

	public onlineFileIO() {
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
		
		ministamp = new String();
		maxstamp = new String();
	}

	public void rawCon() {
		return;
	}

	public void m1Con() {
		return;
	}

	public void m2Con() {
		return;
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

	public void ImportFolder(String foldername, String filter, int model) { // 0:
																			// raw,
																			// 1:
																			// model1,
																			// 2:
																			// model2

		File folder = new File(foldername);
		List<File> list = new ArrayList<File>();
		getFiles(folder, list, filter);

		Collections.sort(list);

		int num = list.size();

		// for(int i=0;i<num;i++)
		// System.out.print(list.get(i)+"\n");

		if (model == 0) {
			onlinModIni();
			fhb.creTab("tempRaw");
//			loadini = 1;
//			for (int i = 0; i < num; i++) {
//				onlineModRLoad(1, list.get(i));
//
//				System.out.print(list.get(i) + "\n");
//			}
		} else if (model == 1) {
			loadini = 1;
			// num=2;
			onlinModIni();
			fhb.creTab("tempM1");

			for (int i = 0; i < num; i++) {
				if (i == (num - 1))
					isend = 1;
				onlineMod1Load(1, errpre, list.get(i));
				System.out.print(list.get(i) + "\n");
			}
			try {
				FileWriter fstream = new FileWriter("conf.txt", true);
				BufferedWriter out = new BufferedWriter(fstream);
				String str = "model number," + Integer.toString(segn);
				out.write(str);
				out.write("\n");
				 str = "maximumTime," + maxstamp;
				 out.write(str);
				 out.write("\n");
				 str = "minimumTime," + ministamp;
				 out.write(str);
				 out.write("\n");
			     str = "minival," + Integer.toString(tsRange[1][0]);
				 out.write(str);
				 out.write("\n");
				 str = "maxval," + Integer.toString(tsRange[1][1]);
				 out.write(str);
				//
				out.close();
			} catch (IOException ioe) {

				System.out.println("no file connection");
			}
		} else if (model == 2) {
			loadini = 1;
			// onlinModIni();
			//fhb.formatHbaseM2();
			fhb.creTab("tempM2");
			fhb.creTab("tempM2V");
			for (int i = 0; i < num; i++) {
				if (i == (num - 1))
					isend = 1;
				onlineMod2Load(1, errpre, list.get(i));
				System.out.print(list.get(i) + "    " + Integer.toString(segn)
						+ "\n");
			}
			
			onlineMod2Comple(1);
		}
		else if(model ==3)
		{
			onlineMod2Comple(1);
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
		str = Integer.toString(d.getYear() + 1900) + "-";
		str += (d.getMonth() + "-");
		str += (d.getDate());
		str += ("T" + d.getHours());
		str += (":" + d.getMinutes());
		str += (":" + d.getSeconds());

		return str;
	}

	public void onlineModRLoad(int tsno, File file) {
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
					// rawdata[rds][0] = onlineTimePro(rawdata[rds][0]);
					rawdCol = tmp;
					if (fhb.put("tempRaw", rawid[0] + ":" + rawdata[rds][0],
							"attri", "val", rawdata[rds][tsno]) == 0)
						System.out.println("raw data failure");
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
						// model1...........................//

						String row = rawid[0] + "," + tstol + "," + tedol;

						if (fhb.put("tempM1", row, "attri", "vl",
								Double.toString(vlol)) == 0)
							System.out.println("data mode1 failure");
						if (fhb.put("tempM1", row, "attri", "vr",
								Double.toString(vrol)) == 0)
							System.out.println("data model1 failure");

						if (fhb.put("tempM1", row, "model", "cof1",
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

				if (fhb.put("tempM1", row, "attri", "vl", Double.toString(vlol)) == 0)
					System.out.println("data mode1 failure");
				if (fhb.put("tempM1", row, "attri", "vr", Double.toString(vrol)) == 0)
					System.out.println("data model1 failure");

				if (fhb.put("tempM1", row, "model", "cof1",
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

	public String valfill(int val, int posmax, int negmin) {
		String str = Integer.toString(val), res = "";
		int bitnum = 0, bitval = 0;
		if (val >= 0) {
			bitnum = (int) Math.log10(posmax);
			bitval = (int) Math.log10(val);
		} else {
			bitnum = (int) Math.log10(-1 * negmin);
			bitval = (int) Math.log10(-1 * val);
			str=str.substring(1,str.length());
			res += "-";
		}
		
		if(bitval<bitnum)
		{
			for(int i=0;i<bitnum-bitval;i++)
				res += "0";
		}
//		while (bitval < bitnum) {
//			res += "0";
//			bitval++;
//		}
        res+=str;
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
		//String ministamp = new String();
		
		String tmpa=new String(), tmpb=new String();

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
			int valmin=Integer.parseInt(confst.nextToken());
			
			confstr = confbr.readLine();
			confst = new StringTokenizer(confstr, ",");
			confst.nextToken();
			int valmax=Integer.parseInt(confst.nextToken());
			
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String line = "";
			int tmp = 0, ini = 1, isnull = 0;
			line = br.readLine();
			StringTokenizer st; //= new StringTokenizer(line, ",");

			while ((line = br.readLine()) != null) {

				//StringTokenizer st = new StringTokenizer(line, ",");
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
						
						 tmpa=valfill((int)vlol,valmax,valmin);
						 tmpb=valfill((int)vrol,valmax,valmin);
//						System.out.println(tmpa+tmpb+"\n");
//						
						if (fhb.put("tempM2V",tmpa + ','+ tmpb,
								"attri", "no", rownum) == 0)
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

//				if (fhb.put("tempM2V",valfill((int)vlol,valmax,valmin) + ','+ valfill((int)vrol,valmax,valmin),
//						"attri", "no", rownum) == 0)
//					System.out.println("data model1 failure");

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

}
