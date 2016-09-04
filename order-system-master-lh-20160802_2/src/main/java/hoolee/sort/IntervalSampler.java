package hoolee.sort;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import hoolee.io.MyFileInputStream;
/**
 * 负责采样，同时采样结果保存在该对象当中 
 */
public class IntervalSampler implements Sampler{
	public static void main(String[] args) throws Exception{
	}
	private File file_to_sample;
	private double freq;
	private long lines_read = 0;
    private long lines_sampled = 0;
	private HashMap<String, ArrayList<String>> keys_values_map = new HashMap<String, ArrayList<String>>();
	private Object lock;
	public IntervalSampler(File file_to_sample, double freq){
		this.file_to_sample = file_to_sample;
		this.freq = freq;
	}
	public IntervalSampler(File file_to_sample, double freq, Object lock){
		this.file_to_sample = file_to_sample;
		this.freq = freq;
		this.lock = lock;
	}
	@Override
	public void doSample() throws Exception{
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(new FileInputStream(file_to_sample)), 1024*1024*40);
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
	public void doSample_WithLock() throws Exception{
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(new MyFileInputStream(file_to_sample, lock)), 1024*1024*40);
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
	@Override
	public long getLinesReadCount(){
		return lines_read;
	}
	@Override
	public long getLinesSampedCount(){
		return lines_sampled;
	}
	@Override
	public Map<String, ArrayList<String>> getKeysValuesMap(){
		return keys_values_map;
	}
}
