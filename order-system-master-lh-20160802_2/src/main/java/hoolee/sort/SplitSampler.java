package hoolee.sort;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class SplitSampler implements Sampler{
	private File inFile;
	private int max_lines_to_sample;
	private int assumed_line_size_in_B;
	private HashMap<String, ArrayList<String>> keys_values_map = new HashMap<String, ArrayList<String>>();
	private long lines_sampled = 0;
	private long asumed_lines_in_file = 0;
	
	public SplitSampler(File inFile, int max_lines_to_sample, int assumed_line_size_in_B){
		this.inFile = inFile;
		this.max_lines_to_sample = max_lines_to_sample;
		this.assumed_line_size_in_B = assumed_line_size_in_B;
	}
	@Override
	public void doSample() throws Exception{
		final long fileSize = inFile.length();
		asumed_lines_in_file = fileSize/assumed_line_size_in_B;
		final int split_count = (int)(fileSize/(1024*1024*40))+1;// 40MB per split
		final long size_per_slipt = fileSize/split_count;
		final int samples_per_split = max_lines_to_sample/split_count+1;// not zero
		for(int i=0; i<split_count; i++){
			long begin_offset = size_per_slipt*i;
			MySplitReader reader = new MySplitReader(inFile, begin_offset, 1024*44);
			reader.nextLine();// skip the fist line
			for(int j=0; j<samples_per_split; j++){
				String line = reader.nextLine();
				if(line == null){
					break;
				}
//				System.out.println("SplitSampler, line=" + line);
				for(String pairStr : line.split("\t")){
					int idx = pairStr.indexOf(':');
					String k = pairStr.substring(0, idx);
					String v = pairStr.substring(idx+1);
					ArrayList<String> values_list = keys_values_map.get(k);
					if(values_list == null){
						values_list = new ArrayList<String>();
						values_list.add(v);
						keys_values_map.put(k, values_list);
					}else{
						values_list.add(v);
					}
				}
				lines_sampled++;
			}
			reader.close();
		}
	}
//	@Override
//	public String toString(){
//		return keys_values_map.toString();
//	}
	@Override 
	public long getLinesReadCount(){
		return asumed_lines_in_file;
	}
	@Override
	public long getLinesSampedCount(){
		return lines_sampled;
	}
	@Override
	public Map<String, ArrayList<String>> getKeysValuesMap(){
		return keys_values_map;
	}
	private static class MySplitReader {
		private BufferedReader reader;
		public MySplitReader(File inFile, long begin_offset, int buf_size){
			try{
				FileInputStream in_stream = new FileInputStream(inFile);
				in_stream.getChannel().position(begin_offset);
				reader = new BufferedReader(new InputStreamReader(in_stream), buf_size);
			}catch(Exception e){
				e.printStackTrace();
				System.exit(-1);
			}
		}
		public String nextLine() throws IOException{
			return reader.readLine();
		}
		public void close(){
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
