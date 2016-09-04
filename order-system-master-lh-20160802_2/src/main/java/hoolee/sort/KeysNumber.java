package hoolee.sort;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import hoolee.util.Pair;

public class KeysNumber {
	public static final int NOT_A_KEYNUMBER = 1000000;
	public static final int KEY_IS_FULL = Integer.MAX_VALUE;
	private final int max_keys_number;
	private HashMap<String, Integer> keyToNumber;
	private HashMap<Integer, String> numberToKey;
	int number_count = 1;
	public KeysNumber(Map<String, ArrayList<String>> keysValuesMap, int maxKeys){
		if(NOT_A_KEYNUMBER <= maxKeys){
			throw new RuntimeException("NOT_A_KEYNUMBER=" + NOT_A_KEYNUMBER + " is <= maxKeys=" + maxKeys);
		}
		System.out.println("keysValuesMap.size()=" + keysValuesMap.size());
		max_keys_number = maxKeys;
		keyToNumber = new HashMap<String, Integer>();
		numberToKey = new HashMap<Integer, String>();
		ArrayList<Pair<Integer, String>> keys_freq = new ArrayList<Pair<Integer, String>>();
		for(Map.Entry<String, ArrayList<String>> entry : keysValuesMap.entrySet()){
			keys_freq.add(new Pair<Integer, String>(entry.getValue().size(), entry.getKey()));
		}
		Collections.sort(keys_freq);
		for(int i=keys_freq.size()-1; i>=0; --i){
			if(number_count > max_keys_number){
				break;
			}
			Pair<Integer, String> pair = keys_freq.get(i);
//			System.out.println(pair.value + " -> " + pair.key);
			String key = keys_freq.get(i).value;
			keyToNumber.put(key, number_count);
			numberToKey.put(number_count, key);
			number_count++;
		}
	}
	public String getKey(int number){
		return numberToKey.get(number);
	}
	/**返回NOT_A_KEYNUMBER表示没有找到对应的编号*/
	public int getNumber(String key){
		Integer number = keyToNumber.get(key);
		return number==null ? NOT_A_KEYNUMBER : number;
	}
	/**
	 * !!!! 这里可能成为瓶颈 !!!!
	 * @param key
	 * @return 返回KEY_IS_FULL表示编号已经超过界限，
	 * 否则将key放入，并返回key的编号
	 */
	public synchronized int putIfAbsent(String key){
		Integer number = keyToNumber.get(key);
		if(number == null){
			if(number_count > max_keys_number){
				return KEY_IS_FULL;
			}
			numberToKey.put(number_count, key);
			keyToNumber.put(key, number_count);
			return number_count++;
		}else{
			return number;
		}
	}
	
	
}
