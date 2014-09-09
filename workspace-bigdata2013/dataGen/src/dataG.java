import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class dataG {

	public static int MINVAL = 0;
	public static int MAXVAL = 680;
	public static long TUPINFILE = 1000000;
	public static int FILENUM = 50;
	public static int MODLEN = 20;

	public dataG() {

	}

//	public static void read() throws IOException {
//		FileReader fr = new FileReader("2-gsensor.txt");
//		BufferedReader br = new BufferedReader(fr);
//		String line = "";
//		line = br.readLine();
//
//		while ((line = br.readLine()) != null) {
//
//		}
//		System.out.printf("reading done\n");
//	}

	public static void main(String[] args) throws Exception {

		int filenum = 1;
		double tupnum = 0.0;
		double tempst = 1597971100.0;
		//1299970801.0;
		//1397970899.0,90,7,152
		//1397970900.0;
		//1447970951.0; 
		//1497971001
		//1547971051.0;
	
		double preval = 0.0;
		String wstr = "";

		FileWriter fstream;
		BufferedWriter out;

		int val1 = 0, val2 = 0, val3 = 0;

		while (filenum <= FILENUM) {
			filenum++;
			tupnum = 0.0;

			try {

				fstream = new FileWriter(Integer.toString(filenum)
						+ "-gsensor.txt", true);
				out = new BufferedWriter(fstream);
				wstr = "time,x,y,z\n";
				out.write(wstr);

				while (tupnum <= TUPINFILE) {

					if (tupnum % MODLEN != 0) {
						wstr = Long.toString((long) tempst) + ".0";
						val1 = (int) preval + (int) (Math.random() * 30);
						val2 = (int) (20 * Math.random());
						val3 = (int) (200 * Math.random());
						wstr = wstr + "," + Integer.toString(val1) + ","
								+ Integer.toString(val2) + ","
								+ Integer.toString(val3) + "\n";

						tupnum++;
						tempst++;
						out.write(wstr);
					} else {
						wstr = Long.toString((long) tempst) + ".0";
						val1 = MINVAL
								+ (int) (Math.random() * (MAXVAL - MINVAL));
						preval = val1;

						val2 = (int) (20 * Math.random());
						val3 = (int) (200 * Math.random());
						wstr = wstr + "," + Integer.toString(val1) + ","
								+ Integer.toString(val2) + ","
								+ Integer.toString(val3) + "\n";

						tupnum++;
						tempst++;
						out.write(wstr);
					}
				}
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		// read();

		return;
	}

}
