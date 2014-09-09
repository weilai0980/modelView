package basetool;

import java.io.*;
import java.util.*;
import java.text.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

public class seq_join {
	
	private Vector<ResultScanner> scr = new Vector<ResultScanner>();
	private Vector<Result> res = new Vector<Result>();
	private Vector<Result> res_prv = new Vector<Result>(); 
	private Vector<HTable> tables = new Vector<HTable>();
	
	public static Boolean set_exp = false;
	public static int exp;
	
	private Configuration config = HBaseConfiguration.create();
	
	
	private void create_result_table(String name, Vector<String> s_names) throws Exception {
		
		HBaseAdmin hba = new HBaseAdmin(config);
		
		if(hba.tableExists(name)){
			
			hba.disableTable(name);
			hba.deleteTable(name);
		}
		
		HTableDescriptor ht = new HTableDescriptor(name);
		
		for(int i = 0; i < s_names.size(); i++){
			ht.addFamily(new HColumnDescriptor(s_names.get(i)));
		}
		
		hba.createTable(ht);
		hba.close();
		
		System.out.println("Target table created");
	}
	
	
		
	private String find_intersect_prime (int ts_1, int ts_2) throws Exception {
		
		Boolean found = false;
		
		
		while(res.get(ts_1) != null && res.get(ts_2) != null && found == false){
			
			String inter_1[] = Bytes.toString(res.get(ts_1).getRow()).split(",");
			String inter_2[] = Bytes.toString(res.get(ts_2).getRow()).split(",");
			
			/*
			if(set_exp == false){
				exp = Integer.parseInt(inter_1[0].substring(inter_1[0].indexOf('E') + 1));
				set_exp = true;
			}
			*/
			double inter_1_left = Double.parseDouble(inter_1[0]);
			double inter_1_right = Double.parseDouble(inter_1[1]);
			
			double inter_2_left = Double.parseDouble(inter_2[0]);
			double inter_2_right = Double.parseDouble(inter_2[1]);
			
			double intersect_left = Math.max(inter_1_left, inter_2_left);
			double intersect_right = Math.min(inter_1_right,inter_2_right);
			
			if(inter_2_right < inter_1_right){
				res_prv.set(ts_2, res.get(ts_2));
				res.set(ts_2, scr.get(ts_2).next());
				res_prv.set(ts_1, res.get(ts_1));
				
			}
			
			if(inter_1_right < inter_2_right){
				res_prv.set(ts_1, res.get(ts_1));
				res.set(ts_1, scr.get(ts_1).next());
				res_prv.set(ts_2, res.get(ts_2));
			}
			
			if(inter_1_right == inter_2_right){
				res_prv.set(ts_2, res.get(ts_2));
				res.set(ts_2, scr.get(ts_2).next());
				res_prv.set(ts_1, res.get(ts_1));
				res.set(ts_1, scr.get(ts_1).next());
			}
			
			//need to change with <= for [a,b] type of intervals -- right now considers [a,b)
			if(intersect_left < intersect_right){
				found = true;
				return (intersect_left + "," + intersect_right);
			}
		}
		
		return null;
	}
	
	
	
	private String find_intersect_sec(String interval, int ts_2) throws Exception {
		
		String inter_1[] = interval.split(",");
		
		double inter_1_left = Double.parseDouble(inter_1[0]);
		double inter_1_right = Double.parseDouble(inter_1[1]);
		
		Boolean found = false;
		
		while(res.get(ts_2) != null && found == false){
			
			String inter_2[] = Bytes.toString(res.get(ts_2).getRow()).split(",");
			
			double inter_2_left = Double.parseDouble(inter_2[0]);
			double inter_2_right = Double.parseDouble(inter_2[1]);
			
			//need to change with > for [a,b] type of intervals -- right now considers [a,b)
			if(inter_2_left >= inter_1_right){
				return null;
			}
			
			double intersect_left = Math.max(inter_1_left, inter_2_left);
			double intersect_right = Math.min(inter_1_right,inter_2_right);
			
			if(inter_2_right <= inter_1_right){
				res_prv.set(ts_2, res.get(ts_2));
				res.set(ts_2, scr.get(ts_2).next());
			}else{
				res_prv.set(ts_2, res.get(ts_2));
			}
			
			//need to change with <= for [a,b] type of intervals -- right now considers [a,b)
			if(intersect_left < intersect_right){
				found = true;
				return (intersect_left + "," + intersect_right);
			}
			
		}
		
		return null;
	}
	
	
	
	private void add_to_result(String intersection, Vector<String> s_names, String result_name) throws Exception{
		
		/* In order to store the results in sorted order if needed to do further processing
		 * ensures that length of decimal places is same for all and its equal to exponent size
		 */
		
		String format = "#0.";
		for(int i = 0; i < exp; i++){
			format = format + "0";
		}
		
		String interval[] = intersection.split(",");
		double left = Double.parseDouble(interval[0])/Math.pow(10, exp);
		double right = Double.parseDouble(interval[1])/Math.pow(10,exp);
		
		DecimalFormat fm = new DecimalFormat(format);
		intersection = fm.format(left) + "E" + exp + "," + fm.format(right) + "E" + exp;
		
		
		HTable table = new HTable(config, result_name);
		Put p = new Put(Bytes.toBytes(intersection));
		
		for(int i = 0; i < s_names.size(); i++){
			
			String key = Bytes.toString(res_prv.get(i).getRow());
			
			p.add(Bytes.toBytes(s_names.get(i)), Bytes.toBytes("org_intrvl"), Bytes.toBytes(key));
			
			NavigableMap<byte[], NavigableMap<byte[], byte[]>> mp = res_prv.get(i).getNoVersionMap();
			
			for(byte[] family: mp.keySet()){
				
				NavigableMap<byte[], byte[]> cols = mp.get(family);
				
				for(byte[] column : cols.keySet()){
					
					byte[] value = cols.get(column);
					
					String identif = Bytes.toString(family) + ":" + Bytes.toString(column);
					
					p.add(Bytes.toBytes(s_names.get(i)), Bytes.toBytes(identif), value);
					
				}
				
			}
			
		}
		
		table.put(p);
		table.flushCommits();
		table.close();		
		
	}
	
	
	
	private void join(Vector<String> s_names, Vector<String> s_counts, String result_name, String summary_file) throws Exception {
		
		long row_count = 0;
		
		if(s_names.size() < 2){
			System.out.println("Join not possible for less than two tables");
			return;
		
		}else{
			create_result_table(result_name, s_names);
		}
	
		long start_time = System.currentTimeMillis();
		
		exp = 0;
		
		for(int i = 0; i < s_names.size(); i++){
			
			HTable table = new HTable(config, s_names.get(i)); 
			Scan s = new Scan(); 
			ResultScanner rs = table.getScanner(s);
			
			tables.add(table);
			scr.add(rs);
			Result r = rs.next();
			res.add(r);
			res_prv.add(r);
			
			String inter[] = Bytes.toString(r.getRow()).split(",");
			int exp_1 = Integer.parseInt(inter[0].substring(inter[0].indexOf('E') + 1));
			if(exp_1 > exp){
				exp = exp_1;
			}
			
		}
		
		set_exp = true;
		
		while(!res.contains(null)){
			
			String prime = find_intersect_prime(0,1);
			
			if(prime != null){
			
				for(int i = 2; i < res.size(); i++){
					
					prime = find_intersect_sec(prime, i);
				
					if(prime == null){
						break;
					}
				}
			}
			
			if(prime != null){
				row_count += 1;
				add_to_result(prime, s_names, result_name);
			}
		}
		
		System.out.println("closing all open resources");
		
		for(int i = 0; i < tables.size(); i++){
			scr.get(i).close();
			tables.get(i).close();
		}
		
		long end_time = System.currentTimeMillis();
		
		FileWriter fw = new FileWriter(summary_file, true);
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write(s_names.size() + ",");
		for(int i = 0; i < s_counts.size(); i++){
			bw.write(s_counts.get(i) + ",");
		}
		bw.write(row_count + "," + (end_time - start_time) + "\n");
		bw.close();
		fw.close();
		
		System.out.println("Total rows in result table = " + row_count);
		
	}
	
	
	public static void main(String args[]) throws Exception {
		
		Vector<String> join_tables = new Vector<String>();
		Vector<String> table_counts = new Vector<String>();
		
		//join_tables.add("test-1");
		//join_tables.add("test-2");
		//join_tables.add("test-3");
		//join_tables.add("test-4");
		
		String result_table = args[1];
		
		FileReader fr = new FileReader(args[0]);
		BufferedReader br = new BufferedReader(fr);
		String s = "";
		while((s = br.readLine()) != null){
			if(s.charAt(0) != '#'){
				String row[] = s.split(",");
				join_tables.add(row[0]);
				table_counts.add(row[1]);
			}
		}
		br.close();
		fr.close();
		
		
		int repeat = Integer.parseInt(args[2]);
		String summary_file = args[3];
		
		FileWriter fw = new FileWriter(summary_file, true);
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write("total_tables,");
		
		for(int i = 0; i < join_tables.size(); i++){
			bw.write("table_" + (i + 1) + "_records,");
		}
		bw.write("output_record,completion_time\n");
		bw.close();
		fw.close();
		
		
		for(int i = 0; i < repeat; i++){
			seq_join sj = new seq_join();
			sj.join(join_tables, table_counts, result_table, summary_file);
		}
	}

}
