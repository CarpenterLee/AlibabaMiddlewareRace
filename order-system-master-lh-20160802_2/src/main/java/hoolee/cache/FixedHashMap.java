package hoolee.cache;

import java.util.LinkedHashMap;
import java.util.Map;

public class FixedHashMap<K, V> extends LinkedHashMap<K, V>{
	private static final long serialVersionUID = 100123123123L;
	private int MAX_ENTRIES;
	public FixedHashMap(int max_size){
//		super((int)(max_size/0.75));
		MAX_ENTRIES = max_size;
	}
	@Override
	protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
		return size() > MAX_ENTRIES;
	}

}
