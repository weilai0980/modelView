package basetool;

import java.io.*;
import java.util.*;
import java.text.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.*;
import org.apache.hadoop.hbase.io.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;




public class map_red_join_3 {
	
	private Configuration config = HBaseConfiguration.create();
	private static int rep = 0;
	
	public static class distribute_map extends TableMapper<IntWritable, Result> {
		
		private double step_size;
		
		//fetch global variables
		public void setup(Context context) throws IOException,InterruptedException {
			
			Configuration conf = context.getConfiguration();
			step_size = Double.parseDouble(conf.get("domain").split(",")[2]);
		}
		
		
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
			
			String key = Bytes.toString(row.get());
			String key_split[] = key.split(",");
			
			double my_left = Double.parseDouble(key_split[1]);
			double my_right = Double.parseDouble(key_split[2]);
			
			int left_bound = (int)(my_left/step_size);
			int right_bound = (int)((my_right - 1)/step_size);
			
			
			for(int i = left_bound; i <= right_bound; i++){
				context.write(new IntWritable(i), value);
			}
		}
	}

	
	
	
	public static class join_reduce extends TableReducer <IntWritable, Result, Text> {
		
		int table_count;
		
		boolean set_exp = false;
		int exp;
		
		//extract the number of tables
		public void setup(Context context) throws IOException,InterruptedException {
			
			Configuration config = context.getConfiguration();
			table_count = Integer.parseInt(config.get("table_count"));
			
			double domain_right = Double.parseDouble(config.get("domain").split(",")[1]);
			
			exp = 0;
			while(domain_right >= 1){
				domain_right = domain_right/10;
				exp ++;
			}
			
			exp = exp - 1;
			set_exp = true;
		
		}
		
		
		private Put add_to_result(String intersection, TreeMap<String, Result> all_values) throws Exception{
			
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
			
			
			Put p = new Put(Bytes.toBytes(intersection));
			
			for(String table : all_values.keySet()){
	
				Result res = all_values.get(table);
				String k[] = Bytes.toString(res.getRow()).split(",");
			
				p.add(Bytes.toBytes(table), Bytes.toBytes("org_intrvl"), Bytes.toBytes(k[1] + "," + k[2]));
				
				NavigableMap<byte[], NavigableMap<byte[], byte[]>> mp = res.getNoVersionMap();
				
				for(byte[] family: mp.keySet()){
					
					NavigableMap<byte[], byte[]> cols = mp.get(family);
					
					for(byte[] column : cols.keySet()){
						
						byte[] value = cols.get(column);
						
						String identif = Bytes.toString(family) + ":" + Bytes.toString(column);
						
						p.add(Bytes.toBytes(k[0]), Bytes.toBytes(identif), value);
						
					}
					
				}
			}
			
			return p;
				
		}
		
		
		public void reduce (IntWritable key, Iterable<Result> values, Context context) throws IOException, InterruptedException {
			
			TreeMap<String, String[]> all_tables = new TreeMap<String, String[]>();
			TreeMap<String, Result> all_values = new TreeMap<String, Result>();
			
			Double intersect_left = Double.MIN_VALUE;
			Double intersect_right = Double.MAX_VALUE;
			
			
			for(Result r : values){
				
				String k[] = Bytes.toString(r.getRow()).split(",");
				
				/*
				if(set_exp == false){
					exp = Integer.parseInt(k[1].substring(k[1].indexOf('E') + 1));
					set_exp = true;
				}		
				*/
				
				if(!all_tables.containsKey(k[0])){
					all_tables.put(k[0], new String[]{k[1], k[2]});
					all_values.put(k[0], r);
				}else{
					
					String old_left = all_tables.get(k[0])[0];
					String new_left = k[1];
					
					if(Double.parseDouble(new_left) < Double.parseDouble(old_left)){
						all_tables.put(k[0], new String[]{k[1], k[2]});
						all_values.put(k[0], r);
					}
				}
				
			}
			
			
			if(all_tables.size() < table_count){
				return;
			
			}else{
				
				for(String table : all_tables.keySet()){
					
					if(Double.parseDouble(all_tables.get(table)[0]) > intersect_left){
						intersect_left = Double.parseDouble(all_tables.get(table)[0]);
					}
					
					if(Double.parseDouble(all_tables.get(table)[1]) < intersect_right){
						intersect_right = Double.parseDouble(all_tables.get(table)[1]);
					}
				}
				
				if(intersect_left < intersect_right){
					
					String intersection = intersect_left + "," + intersect_right;
					
					Put p;
					try {
						
						p = add_to_result(intersection, all_values);
						context.write(new Text(intersection), p);
					
					} catch (Exception e) {
						e.printStackTrace();
					}
					
					
				}
			}
		}
	}
		
	
	
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
	
	
	private void join(Vector<String> s_names, Vector<String> s_counts, String result_name, String summary_file) throws Exception {
		
		if(s_names.size() < 2){
			System.out.println("Join not possible for less than two tables");
			return;
		
		}else{
			create_result_table(result_name, s_names);
		}
		
		String domain = "";
		String copy_table = "join-source";
		
		if(map_red_join_3.rep <= 1){
			
			domain = copy_tables.copy(s_names, copy_table);
		
		}else{
			
			HTable d_table = new HTable(config, "domain_table");
			Get g = new Get(Bytes.toBytes("final_domain"));
			Result r = d_table.get(g);
			double left = Bytes.toDouble(r.getValue(Bytes.toBytes("domain"),Bytes.toBytes("left")));
			double right = Bytes.toDouble(r.getValue(Bytes.toBytes("domain"),Bytes.toBytes("right")));
			double min_seg_size = Bytes.toDouble(r.getValue(Bytes.toBytes("domain"),Bytes.toBytes("min_seg_size")));
			d_table.close();
			domain = String.valueOf(left) + "," + String.valueOf(right) + "," + String.valueOf(min_seg_size);
		}
		
		long start_time = System.currentTimeMillis();
		
		Scan scan1 = new Scan();
		scan1.setCaching(1000);
		scan1.setCacheBlocks(false);
		
		Job job1 = new Job(config);
		job1.setJarByClass(map_red_join_3.class);
		
		job1.getConfiguration().set("domain", domain);
		job1.getConfiguration().set("table_count", String.valueOf(s_names.size()));
		
		TableMapReduceUtil.initTableMapperJob(copy_table, scan1, distribute_map.class, IntWritable.class, Result.class, job1);
		TableMapReduceUtil.initTableReducerJob(result_name, join_reduce.class, job1);
		job1.waitForCompletion(true);
		
		long end_time = System.currentTimeMillis();
		
		/*
		HBaseAdmin hba = new HBaseAdmin(config);
		hba.disableTable(copy_table);
		hba.deleteTable(copy_table);
		hba.close();
		*/
		
		Counters c = job1.getCounters();
		long map_inp_rec = c.findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue();
		long map_out_rec = c.findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_OUTPUT_RECORDS").getValue();
		long rpc_calls = c.findCounter("HBase Counters", "RPC_CALLS").getValue();
		long reduce_inp_grps = c.findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_INPUT_GROUPS").getValue();
		long reduce_out_rec = c.findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();
		
		FileWriter fw = new FileWriter(summary_file, true);
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write(s_names.size() + ",");
		for(int i = 0; i < s_counts.size(); i++){
			bw.write(s_counts.get(i) + ",");
		}
		bw.write(map_inp_rec + "," + map_out_rec + "," + rpc_calls + "," + reduce_inp_grps + "," + reduce_out_rec + "," + (end_time - start_time) + "\n");
		bw.close();
		fw.close();		
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
		bw.write("map_input_rec,map_output_rec,rpc_calls,reduce_grps,reduce_output_rec,completion_time\n");
		bw.close();
		fw.close();
		
		
		for(int i = 0; i < repeat; i++){
			map_red_join_3 j = new map_red_join_3();
			map_red_join_3.rep = i + 1;
			j.join(join_tables, table_counts, result_table, summary_file);
		}
	
	}
	
}
