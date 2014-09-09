package basetool;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseOp {

	public static HBaseConfiguration config = new HBaseConfiguration();

	public ResultScanner rs;
	public static Result scanr;

	public ResultScanner[] mulrs = new ResultScanner[3];
	public static Result[] mulscanr = new Result[3];

	public static int cacheSize = 200;
	public static Result[] cache = new Result[cacheSize];
	public static String[] cacheRkey = new String[cacheSize];
	public static Integer[] cacheSig = new Integer[cacheSize];

	public HBaseAdmin admin;

	void HBaseOp() {

		// config
		rs = null;
		scanr = null;
		cacheSize = 200;

		try {
			// Create a HBaseAdmin
			// HBaseAdmin admin = new HBaseAdmin(config);
			// Create table
			admin = new HBaseAdmin(config);

		} catch (IOException e) {

			System.out.println("IOError: cannot hbase admin.");
			e.printStackTrace();
		}

		for (int i = 0; i < mulrs.length; i++) {
			mulrs[i] = null;
			mulscanr[i] = null;
		}
	}

	// public static void main(String[] args){

	public void creTab(String tabname) {

		// Try to create a Table with 2 column family (Title, Author)
		HTableDescriptor descriptor = new HTableDescriptor(tabname);
		descriptor.addFamily(new HColumnDescriptor("attri"));
		descriptor.addFamily(new HColumnDescriptor("model"));

		try {
			// Create a HBaseAdmin
			HBaseAdmin admin = new HBaseAdmin(config);
			// Create table
			admin.createTable(descriptor);
			System.out.println("Table created…");
		} catch (IOException e) {

			System.out.println("IOError: cannot create Table.");
			e.printStackTrace();
		}
	}

	
	
	public void formatHbaseM2() {
		try {
			// Create a HBaseAdmin
			HBaseAdmin admin = new HBaseAdmin(config);
			// Create table

			 admin.disableTable("tempM2");
			 admin.disableTable("tempM2V");
			// admin.disableTable("tempMap");
//			 admin.disableTable("tempM1");
//			 admin.disableTable("tempRaw");
			//
			 admin.deleteTable("tempM2");
			 admin.deleteTable("tempM2V");
			// admin.deleteTable("tempMap");
//			 admin.deleteTable("tempM1");
//			 admin.deleteTable("tempRaw");

		} catch (IOException e) {

			System.out.println("IOError: cannot format hbase.");
			e.printStackTrace();
		}
	}
	
	public void formatHbase() {
		try {
			// Create a HBaseAdmin
			HBaseAdmin admin = new HBaseAdmin(config);
			// Create table

			 admin.disableTable("tempM2");
			 admin.disableTable("tempM2V");
			// admin.disableTable("tempMap");
			 admin.disableTable("tempM1");
			 admin.disableTable("tempRaw");
			//
			 admin.deleteTable("tempM2");
			 admin.deleteTable("tempM2V");
			// admin.deleteTable("tempMap");
			 admin.deleteTable("tempM1");
			 admin.deleteTable("tempRaw");

		} catch (IOException e) {

			System.out.println("IOError: cannot format hbase.");
			e.printStackTrace();
		}
	}

	public void ini() {

		// Try to create a Table with 2 column family (Title, Author)
		HTableDescriptor descriptor = new HTableDescriptor("tsTab");
		descriptor.addFamily(new HColumnDescriptor("attri"));
		descriptor.addFamily(new HColumnDescriptor("model"));

		try {
			// Create a HBaseAdmin
			HBaseAdmin admin = new HBaseAdmin(config);
			// Create table
			admin.createTable(descriptor);
			System.out.println("Table created…");
		} catch (IOException e) {

			System.out.println("IOError: cannot create Table.");
			e.printStackTrace();
		}
	}

	public int put(String tabname, String row, String cf, String qual,
			String val) {
		try {
			HTable table = new HTable(config, tabname);
			Put put = new Put(Bytes.toBytes(row));
			// for (int j = 0; j < cfs.length; j++) {
			put.add(Bytes.toBytes(cf), Bytes.toBytes(qual), Bytes.toBytes(val));
			table.put(put);
			// }

			return 1;
		} catch (IOException e) {
			e.printStackTrace();
		}

		return 0;
	}

	public void scanGloIni(String tabname) {

		try {
			HTable table = new HTable(config, tabname);
			Scan s = new Scan();
			rs = null;
			rs = table.getScanner(s);
			scanr = null;
			// return rs;

		} catch (IOException e) {
			System.out.println("scan initialization problem");
			// return null;
		}

	}

	public void scanIni(String tabname, String strw, String edrw) {

		try {
			HTable table = new HTable(config, tabname);
			Scan s = new Scan();
			s.setStartRow(strw.getBytes());
			s.setStopRow(edrw.getBytes());
			rs = null;
			rs = table.getScanner(s);
			scanr = null;
			// return rs;

		} catch (IOException e) {
			System.out.println("scan initialization problem");
			// return null;
		}

	}

	public String scanNext(String cf, String qual) {
		try {

			// HTable table = new HTable(config, tabname);
			// Scan s = new Scan();
			// ResultScanner rs = table.getScanner(s);
			int cnt = 0;

			Result r = rs.next();

			if (r == null)
				return "NO";
			else {
				String res = new String(r.getValue(cf.getBytes(),
						qual.getBytes()));
				return res;
			}

		} catch (IOException e) {
			System.out.println("scan next probblem");
			return "NO";
		}
	}

	public void scanMNext() {
		try {
			scanr = rs.next();
			return;
		} catch (IOException e) {
			System.out.println("scan next problem");

		}
	}

	public String scanM(String cf, String[] qual, int qn, String[] res) {

		// HTable table = new HTable(config, tabname);
		// Scan s = new Scan();
		// ResultScanner rs = table.getScanner(s);
		int cnt = 0;

		if (scanr == null) {
			res[0] = "NO";
			return "NO";
		} else {
			for (int i = 0; i < qn; ++i) {
				String tres = new String(scanr.getValue(cf.getBytes(),
						qual[i].getBytes()));
				res[i] = tres;
			}
			return "Yes";
		}
	}

	public String scanR() {

		if (scanr == null) {
			return "NO";
		} else {
			String tres = new String(scanr.getRow());
			return tres;
		}
	}
	
	//........get test......................................................//

	public void getTestRow(String tabname, String rkey) {

		// throws IOException {
		// String res = "NO";
		try {
			HTable table = new HTable(config, tabname);
			Get g = new Get(rkey.getBytes());
			//g.addColumn(cf.getBytes(), qual.getBytes());

			Result rs = table.get(g);

			rs.getValue("attri".getBytes(), "vl".getBytes());
			rs.getValue("attri".getBytes(), "vr".getBytes());
			
			rs.getValue("model".getBytes(), "cof1".getBytes());
			
			return;

		} catch (IOException e) {
			// System.out.println("get probblem");
			return;
		}
	}
	public void getTestIndiCol(String tabname, String rkey) {

		// throws IOException {
		// String res = "NO";
		try {
			HTable table = new HTable(config, tabname);
				
	     	Get g = new Get(rkey.getBytes());
			g.addColumn("attri".getBytes(), "vl".getBytes());
			Result rs = table.get(g);
			String res=new String(rs.value());
			
			Get g1 = new Get(rkey.getBytes());
			g.addColumn("attri".getBytes(), "vr".getBytes());
			Result rs1 = table.get(g1);
			String res1=new String(rs.value());
			
			Get g2 = new Get(rkey.getBytes());
			g.addColumn("model".getBytes(), "cof1".getBytes());
			Result rs2 = table.get(g2);
			String res2=new String(rs.value());
			
			return;

		} catch (IOException e) {
			// System.out.println("get probblem");
			return;
		}
	}
	
	//....................................................................//
	
	
	public void get(String tabname, String rkey, String cf, String[] qual,
			String[] res) {

		// throws IOException {
		// String res = "NO";
		try {
			HTable table = new HTable(config, tabname);
			Get g = new Get(rkey.getBytes());
			// g.addColumn(cf.getBytes(), qual.getBytes());

			Result rs = table.get(g);

			for (int i = 0, j = qual.length; i < j; ++i) {

				// tmp= Byte[]{};
				if (rs.getValue(cf.getBytes(), qual[i].getBytes()) == null)
					// if(tmp.length()== 0)
					return;
				else {
					String tmp = new String(rs.getValue(cf.getBytes(),
							qual[i].getBytes()));
					res[i] = tmp;
				}
			}
			return;

		} catch (IOException e) {
			// System.out.println("get probblem");
			return;
		}
	}

	public void multiScanIni(String tabname, int ptno) {

		try {
			HTable table = new HTable(config, tabname);
			Scan s = new Scan();
			mulrs[ptno] = null;
			mulrs[ptno] = table.getScanner(s);
			mulscanr[ptno] = null;
			// return rs;

		} catch (IOException e) {
			System.out.println("scan initialization problem");
			// return null;
		}

	}

	public String multiScanR(int ptno) {

		if (mulscanr[ptno] == null) {
			return "NO";
		} else {
			String tres = new String(mulscanr[ptno].getRow());
			return tres;
		}
	}

	public void multiScanMNext(int ptno) {
		try {
			mulscanr[ptno] = mulrs[ptno].next();
			return;
		} catch (IOException e) {
			System.out.println("scan next problem");

		}
	}

	public String multiScanM(String cf, String[] qual, String[] res, int ptno) {

		// HTable table = new HTable(config, tabname);
		// Scan s = new Scan();
		// ResultScanner rs = table.getScanner(s);
		int cnt = 0;

		if (mulscanr[ptno] == null) {
			res[0] = "NO";
			return "NO";
		} else {
			for (int i = 0; i < qual.length; ++i) {
				String tres = new String(mulscanr[ptno].getValue(cf.getBytes(),
						qual[i].getBytes()));
				res[i] = tres;
			}
			return "Yes";
		}
	}

	// ...............tuple caching........................//

	public void cacheGet(String tabname, String rkey, String cf, String[] qual,
			String[] res) {

		// throws IOException {
		// String res = "NO";
		try {

			if (cacheFch(rkey, cf, qual, res) == 1) {

			} else {

				HTable table = new HTable(config, tabname);
				Get g = new Get(rkey.getBytes());
				// g.addColumn(cf.getBytes(), qual.getBytes());

				Result rs = table.get(g);

				cachePut(rkey, rs);

				for (int i = 0, j = qual.length; i < j; ++i) {

					// tmp= Byte[]{};
					if (rs.getValue(cf.getBytes(), qual[i].getBytes()) == null)
						// if(tmp.length()== 0)
						return;
					else {
						String tmp = new String(rs.getValue(cf.getBytes(),
								qual[i].getBytes()));
						res[i] = tmp;
					}
				}
			}
			return;

		} catch (IOException e) {
			// System.out.println("get probblem");
			return;
		}
	}

	public void cacheGetPut(String tabname, String rkey, String cf, String[] qual,
			String[] res) {

		// throws IOException {
		// String res = "NO";
		try {

				HTable table = new HTable(config, tabname);
				Get g = new Get(rkey.getBytes());
				// g.addColumn(cf.getBytes(), qual.getBytes());

				Result rs = table.get(g);

				cachePut(rkey, rs);

				for (int i = 0, j = qual.length; i < j; ++i) {

					// tmp= Byte[]{};
					if (rs.getValue(cf.getBytes(), qual[i].getBytes()) == null)
						// if(tmp.length()== 0)
						return;
					else {
						String tmp = new String(rs.getValue(cf.getBytes(),
								qual[i].getBytes()));
						res[i] = tmp;
					}
				}
			
			return;

		} catch (IOException e) {
			// System.out.println("get probblem");
			return;
		}
	}
	
	public void cacheIni() {

		for (int i = 0; i < cacheSig.length; i++)
			cacheSig[i] = 0;
		return;
	}

	public void cachePut(String rkey, Result rs) {
		int hc = rkey.hashCode()%cacheSize;
		if(hc<0) hc=hc*(-1);
		
		if (cacheSig[hc] == 0) {
			cache[hc] = rs;
			cacheRkey[hc] = rkey;
		} else {
			hc++;
			while (cacheSig[hc] == 1 && hc < cacheSize) {
             hc++;
			}
			//hc--;
			cacheSig[hc] = 1;
			cacheRkey[hc] = rkey;
			cache[hc] = rs;
		}
		return;
	}

	public int cacheIsExist(String rkey) {
		int hc = rkey.hashCode()%cacheSize;
		if(hc<0) hc=hc*(-1);
		
		if(cacheSig[hc] == 0) return 0;
		while ((hc < cacheSize && cacheRkey[hc].equals(rkey)==false)){
			hc++;
		}
		if(hc>=cacheSize) return 0; else return 1;
	}
	
	public int cacheFch(String rkey, String cf, String[] qual, String[] res) {
		int hc = rkey.hashCode()%cacheSize;
		if(hc<0) hc=hc*(-1);
		
		if(cacheSig[hc] == 0) return 0;
		while ((hc < cacheSize && cacheRkey[hc].equals(rkey)==false)){
			hc++;
		}
		if(hc>=cacheSize) return 0;
		
		//hc--;
		Result tmprs = cache[hc];

		for (int i = 0, j = qual.length; i < j; ++i) {

			if (tmprs.getValue(cf.getBytes(), qual[i].getBytes()) == null)
				return 0;
			else {
				res[i] = new String(tmprs.getValue(cf.getBytes(),
						qual[i].getBytes()));
			}
		}
		return 1;
	}

}
