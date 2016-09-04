package hoolee.sort;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
/**
 * 将IntervalSampler2的信息汇总
 */
public class IntervalSamplerHelper {

	public static void main(String[] args) {

	}
	private long lines_read = 0;
    private long lines_sampled = 0;
	private HashMap<String, ArrayList<String>> keys_values_map = new HashMap<String, ArrayList<String>>();
	public IntervalSamplerHelper(ArrayList<IntervalSampler2> samplers){
		for(IntervalSampler2 sampler : samplers){
			this.lines_read += sampler.getLinesReadCount();
			this.lines_sampled += sampler.getLinesSampedCount();
			for(Map.Entry<String, ArrayList<String>> entry : sampler.getKeysValuesMap().entrySet()){
				String key = entry.getKey();
				ArrayList<String> value = entry.getValue();
				ArrayList<String> old_values_list = keys_values_map.get(key);
				if(old_values_list == null){
					keys_values_map.put(key, value);
				}else{
					old_values_list.addAll(value);
				}
			}
		}
	}
	public long getLinesReadCount(){
		return lines_read;
	}
	public long getLinesSampledCount(){
		return lines_sampled;
	}
	public HashMap<String, ArrayList<String>> getKeysValuesMap(){
		return keys_values_map;
	}
	public ArrayList<String> getValues(String key){
		return keys_values_map.get(key);
	}

}
