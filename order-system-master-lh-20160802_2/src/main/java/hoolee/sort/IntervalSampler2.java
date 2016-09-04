package hoolee.sort;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * 负责采样，同时采样结果保存在该对象当中 
 */
public class IntervalSampler2{
	public static void main(String[] args) throws Exception{
	}
	private File file_to_sample;
	private double freq;
	private long lines_read = 0;
    private long lines_sampled = 0;
	private HashMap<String, ArrayList<String>> keys_values_map = new HashMap<String, ArrayList<String>>();
	public IntervalSampler2(File file_to_sample, double freq){
		this.file_to_sample = file_to_sample;
		this.freq = freq;
	}
	public void doSample() throws Exception{
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(new FileInputStream(file_to_sample)), 1024*1024*16);
		String line;
		while((line=reader.readLine()) != null){
			++lines_read;
			if ((double) lines_sampled / lines_read < freq) {// sample this line
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
				++lines_sampled;
			}
		}
		reader.close();
	}
	public long getLinesReadCount(){
		return lines_read;
	}
	public long getLinesSampedCount(){
		return lines_sampled;
	}
	public Map<String, ArrayList<String>> getKeysValuesMap(){
		return keys_values_map;
	}
}
