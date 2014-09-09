package basetool;

import java.io.*;
import java.util.StringTokenizer;

public class segIdxDis {

	public int segn;
	public int blocksize;

	public void segIdxDis(int blksize) {
		segn = 0;
		blocksize = blksize;
	}

	public void segload(String key, String addr) {

		String tuple = key + "," + addr;// key is staring and ending point
										// combined, seperated by ','
		try {
			// Create file
			FileWriter fstream = new FileWriter("segtree.txt", true);
			BufferedWriter out = new BufferedWriter(fstream);
			out.write(tuple);
			// Close the output stream
			out.close();
		} catch (Exception e) {// Catch exception if any
			System.err.println("Error: " + e.getMessage());
		}
		segn++;
	}

	public void creation() {
		try {
			FileReader fr = new FileReader("segtree.txt");
			BufferedReader br = new BufferedReader(fr);

			FileWriter fstream = new FileWriter("segtree.txt", true);
			BufferedWriter out = new BufferedWriter(fstream);

			int idxi = 0, si = 0, ini = 0;
			int stno = 0;
			String stkey = new String(), curkey = new String(), wrstr = new String();

			StringTokenizer st = new StringTokenizer(br.readLine(), ",");
			stkey = st.nextToken();
			si++;
			idxi++;

			while (si <= segn) {

				st = new StringTokenizer(br.readLine(), ",");
				curkey = st.nextToken();
				idxi++;
				si++;

				if (ini == 1) {
					stkey = curkey;
					stno = si - 1;

					ini = 0;
				}
				if (idxi == blocksize) {

					StringTokenizer tmpsep = new StringTokenizer(stkey, ",");
					String leftb = tmpsep.nextToken();
					tmpsep = new StringTokenizer(curkey, ",");
					tmpsep.nextToken();
					String rightb = tmpsep.nextToken();

					wrstr = leftb + "," + rightb + "," + Integer.toString(stno)
							+ "," + Integer.toString(si - 1);
					idxi = 0;
					ini = 1;
					out.write(wrstr);
				}

			}

			StringTokenizer tmpsep = new StringTokenizer(stkey, ",");
			String leftb = tmpsep.nextToken();
			tmpsep = new StringTokenizer(curkey, ",");
			tmpsep.nextToken();
			String rightb = tmpsep.nextToken();

			wrstr = leftb + "," + rightb + "," + Integer.toString(stno) + ","
					+ Integer.toString(si - 1);
			idxi = 0;
			ini = 1;
			out.write(wrstr);

			String line = new String();
			while ((line = br.readLine()) != null) {

				st = new StringTokenizer(line, ",");
				curkey = st.nextToken();
				idxi++;
				si++;

				if (ini == 1) {
					stkey = curkey;
					stno = si - 1;

					ini = 0;
				}
				if (idxi == blocksize) {

					tmpsep = new StringTokenizer(stkey, ",");
					leftb = tmpsep.nextToken();
					tmpsep = new StringTokenizer(curkey, ",");
					rightb = tmpsep.nextToken();

					wrstr = leftb + "," + rightb + "," + Integer.toString(stno)
							+ "," + Integer.toString(si - 1);
					idxi = 0;
					ini = 1;
					out.write(wrstr);
				}

			}

			if (idxi >= 2) {
				tmpsep = new StringTokenizer(stkey, ",");

				leftb = tmpsep.nextToken();
				tmpsep = new StringTokenizer(curkey, ",");
				tmpsep.nextToken();
				rightb = tmpsep.nextToken();

				wrstr = leftb + "," + rightb + "," + Integer.toString(stno)
						+ "," + Integer.toString(si - 1);
				idxi = 0;
				ini = 1;
				out.write(wrstr);
			}
			fr.close();
			fstream.close();
		} catch (Exception e) {// Catch exception if any
			System.err.println("Error: " + e.getMessage());
		}

		return;
	}

	public String findSingKey(String key) {
		return "";
	}

}
