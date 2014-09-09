package basetool;

import java.util.*;
import java.text.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

public class seq_join_2 {
	
	private Vector<ResultScanner> scr = new Vector<ResultScanner>();
	private Vector<Result> res = new Vector<Result>();
	private Vector<Result> res_prv = new Vector<Result>(); 
	private Vector<HTable> tables = new Vector<HTable>();
	
	
	private Configuration config = HBaseConfiguration.create();
	
	
	private void create_result_table(String name, Vector<String> s_names) throws Exception {
		
		HBaseAdmin hba = new HBaseAdmin(config);
		
		// if target table already exists then delete and recreate
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
			
			String inter_1_left = inter_1[0];
			String inter_1_right = inter_1[1];
			
			String inter_2_left = inter_2[0];
			String inter_2_right = inter_2[1];
			
			String intersect_left = inter_1_left;
			
			if(inter_1_left.compareTo(inter_2_left) < 0){
				intersect_left = inter_2_left;
			}
			
			String intersect_right = inter_1_right;
			
			if(inter_1_right.compareTo(inter_2_right) > 0){
				intersect_right = inter_2_right;
			}
			
			if(inter_2_right.compareTo(inter_1_right) < 0){
				res_prv.set(ts_2, res.get(ts_2));
				res.set(ts_2, scr.get(ts_2).next());
				res_prv.set(ts_1, res.get(ts_1));
				
			}
			
			if(inter_1_right.compareTo(inter_2_right) < 0){
				res_prv.set(ts_1, res.get(ts_1));
				res.set(ts_1, scr.get(ts_1).next());
				res_prv.set(ts_2, res.get(ts_2));
			}
			
			if(inter_1_right.equals(inter_2_right)){
				res_prv.set(ts_2, res.get(ts_2));
				res.set(ts_2, scr.get(ts_2).next());
				res_prv.set(ts_1, res.get(ts_1));
				res.set(ts_1, scr.get(ts_1).next());
			}
			
			//need to change with <= for [a,b] type of intervals -- right now considers [a,b)
			if(intersect_left.compareTo(intersect_right) < 0){
				found = true;
				return (intersect_left + "," + intersect_right);
			}
		}
		
		return null;
	}
	
	
	
	private String find_intersect_sec(String interval, int ts_2) throws Exception {
		
		String inter_1[] = interval.split(",");
		
		String inter_1_left = inter_1[0];
		String inter_1_right = inter_1[1];
		
		Boolean found = false;
		
		while(res.get(ts_2) != null && found == false){
			
			String inter_2[] = Bytes.toString(res.get(ts_2).getRow()).split(",");
			
			String inter_2_left = inter_2[0];
			String inter_2_right = inter_2[1];
			
			//need to change with > for [a,b] type of intervals -- right now considers [a,b)
			if(inter_2_left.equals(inter_1_right) || inter_2_left.compareTo(inter_1_right) > 0){
				return null;
			}
			
			String intersect_left = inter_1_left;
			
			if(inter_1_left.compareTo(inter_2_left) < 0){
				intersect_left = inter_2_left;
			}
			
			String intersect_right = inter_1_right;
			
			if(inter_1_right.compareTo(inter_2_right) > 0){
				intersect_right = inter_2_right;
			}
			
			if(inter_2_right.compareTo(inter_1_right) < 0 || inter_2_right.equals(inter_1_right)) {
				res_prv.set(ts_2, res.get(ts_2));
				res.set(ts_2, scr.get(ts_2).next());
			}else{
				res_prv.set(ts_2, res.get(ts_2));
			}
			
			//need to change with <= for [a,b] type of intervals -- right now considers [a,b)
			if(intersect_left.compareTo(intersect_right) < 0){
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
	
	
	
	private void join(Vector<String> s_names, String result_name) throws Exception {
		
		long row_count = 0;
		
		if(s_names.size() < 2){
			System.out.println("Join not possible for less than two tables");
			return;
		
		}else{
			create_result_table(result_name, s_names);
		}
		
		
		for(int i = 0; i < s_names.size(); i++){
			
			HTable table = new HTable(config, s_names.get(i)); 
			Scan s = new Scan(); 
			ResultScanner rs = table.getScanner(s);
			
			tables.add(table);
			scr.add(rs);
			Result r = rs.next();
			res.add(r);
			res_prv.add(r);
		}
		
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
		
		System.out.println("Total rows in result table = " + row_count);
		
	}
	
	
	public static void main(String args[]) throws Exception {
		
		Vector<String> join_tables = new Vector<String>();
		join_tables.add("test-1");
		join_tables.add("test-2");
		join_tables.add("test-3");
		join_tables.add("test-4");
		String result_table = "join-result";
		
		seq_join_2 sj = new seq_join_2();
		sj.join(join_tables, result_table);
	}

}
