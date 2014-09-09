package basetool;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.hbase.io.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;


public class copy_tables {
	
	public static class copy_map extends TableMapper<ImmutableBytesWritable, Put> {
		
		String table_name;
		double my_left = Double.MAX_VALUE;
		double my_right = Double.MIN_VALUE;
		double seg_size = Double.MAX_VALUE;
		
		public void setup(Context context) throws IOException,InterruptedException {
			
			Configuration conf = context.getConfiguration();
			table_name = conf.get("table_name");
		}
		
		public void cleanup(Context context) throws IOException,InterruptedException {
			
			HTable d_table = new HTable(context.getConfiguration(), "domain_table");
			Get g = new Get(Bytes.toBytes("final_domain"));
			Result r = d_table.get(g);
			
			double left = Bytes.toDouble(r.getValue(Bytes.toBytes("domain"),Bytes.toBytes("left")));
			double right = Bytes.toDouble(r.getValue(Bytes.toBytes("domain"),Bytes.toBytes("right")));
			double min_seg_size = Bytes.toDouble(r.getValue(Bytes.toBytes("domain"),Bytes.toBytes("min_seg_size")));
			
			
			if(my_left < left){
				Put p = new Put(Bytes.toBytes("final_domain"));
				p.add(Bytes.toBytes("domain"), Bytes.toBytes("left"), Bytes.toBytes(my_left));
				d_table.put(p);
			}
			
			if(my_right > right){
				Put p = new Put(Bytes.toBytes("final_domain"));
				p.add(Bytes.toBytes("domain"), Bytes.toBytes("right"), Bytes.toBytes(my_right));
				d_table.put(p);
			}
			
			if(seg_size < min_seg_size){
				Put p = new Put(Bytes.toBytes("final_domain"));
				p.add(Bytes.toBytes("domain"), Bytes.toBytes("min_seg_size"), Bytes.toBytes(seg_size));
				d_table.put(p);
			}
			
			d_table.flushCommits();
			d_table.close();
			
		}
		
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
			
			String key = Bytes.toString(row.get());
			
			double l = Double.parseDouble(key.split(",")[0]);
			double r = Double.parseDouble(key.split(",")[1]);
			
			if(l < my_left){
				my_left = l;
			}
			
			if(r > my_right){
				my_right = r;
			}
			
			if(r - l < seg_size){
				seg_size = r - l;
			}
			
			key = table_name + "," + key;
			
	   		context.write(row, insert_row(key, value));
	   	}

	  	private static Put insert_row(String key, Result result) throws IOException {
	  		
	  		Put p = new Put(Bytes.toBytes(key));
	 		for (KeyValue kv : result.raw()) {
				
	 			KeyValue kv1 = new KeyValue(Bytes.toBytes(key), kv.getFamily(),kv.getQualifier(),kv.getValue());
	 			p.add(kv1);
			}
			return p;
	   	}
	}
	
	public static String copy(Vector<String> table_names, String target_table) throws Exception{
		
		Configuration config = HBaseConfiguration.create();
		HBaseAdmin hba = new HBaseAdmin(config);
		
		
		//if target table is already existing
		if(hba.tableExists(target_table)){
			
			hba.disableTable(target_table);
			hba.deleteTable(target_table);
		}
		
		HTableDescriptor ht = new HTableDescriptor(target_table);
		
		//add all the families from the source tables
		for(int i = 0; i < table_names.size(); i++){
			
			HTable table = new HTable(config, table_names.get(i)); 
			Scan s = new Scan(); 
			ResultScanner rs = table.getScanner(s);
			Result r = rs.next();
			
			if(r == null){
				System.out.println("input tables can not be empty");
				rs.close();
				table.close();
				throw new Exception();
			}
			
			NavigableMap<byte[], NavigableMap<byte[], byte[]>> m = r.getNoVersionMap();
			
			for(byte[] family : m.keySet()){
				
				if(!ht.hasFamily(family)){
					ht.addFamily(new HColumnDescriptor(family));
				}
			}
			rs.close();
			table.close();
			
		}
		
		hba.createTable(ht);
		
		//create table to store the domain of the join interval
		if(hba.tableExists("domain_table")){
			
			hba.disableTable("domain_table");
			hba.deleteTable("domain_table");
		}
		
		ht = new HTableDescriptor("domain_table");
		ht.addFamily(new HColumnDescriptor("domain"));
		hba.createTable(ht);
		hba.close();
		
		//set up initial domain
		double left = Double.MAX_VALUE;
		double right = Double.MIN_VALUE;
		double min_seg_size = Double.MAX_VALUE;
		
		HTable d_table = new HTable(config, "domain_table");
		Put p = new Put(Bytes.toBytes("final_domain"));
		p.add(Bytes.toBytes("domain"), Bytes.toBytes("left"), Bytes.toBytes(left));
		p.add(Bytes.toBytes("domain"), Bytes.toBytes("right"), Bytes.toBytes(right));
		p.add(Bytes.toBytes("domain"), Bytes.toBytes("min_seg_size"), Bytes.toBytes(min_seg_size));
		d_table.put(p);
		d_table.flushCommits();
		
		
		for(int i = 0; i < table_names.size(); i++){
			
			Configuration conf = new Configuration();
			
			Scan scan1 = new Scan();
			scan1.setCaching(500);
			scan1.setCacheBlocks(false);
			
			Job job1 = new Job(conf);
			job1.setJarByClass(copy_tables.class);
			
			job1.getConfiguration().set("table_name", table_names.get(i));
			
			TableMapReduceUtil.initTableMapperJob(table_names.get(i), scan1, copy_map.class, ImmutableBytesWritable.class, Put.class, job1);
			TableMapReduceUtil.initTableReducerJob(target_table, null, job1);
			job1.waitForCompletion(true);
			
		}
		
		Get g = new Get(Bytes.toBytes("final_domain"));
		Result r = d_table.get(g);
		left = Bytes.toDouble(r.getValue(Bytes.toBytes("domain"),Bytes.toBytes("left")));
		right = Bytes.toDouble(r.getValue(Bytes.toBytes("domain"),Bytes.toBytes("right")));
		min_seg_size = Bytes.toDouble(r.getValue(Bytes.toBytes("domain"),Bytes.toBytes("min_seg_size")));
		
		d_table.close();
		
		//removing the temporary table 
		
		/*
		hba = new HBaseAdmin(config);
		hba.disableTable("domain_table");
		hba.deleteTable("domain_table");
		hba.close();
		*/
		
		return String.valueOf(left) + "," + String.valueOf(right) + "," + String.valueOf(min_seg_size);
	}
	
	
	public static void main(String args[]) throws Exception{
		
		Vector<String> tables = new Vector<String>();
		tables.add("test-1");
		tables.add("test-2");
		tables.add("test-3");
		//tables.add("test-4");
		
		String target_table = "join-source";
		
		String domain = copy_tables.copy(tables, target_table);
		
		System.out.println(domain);
	}
	    

}
