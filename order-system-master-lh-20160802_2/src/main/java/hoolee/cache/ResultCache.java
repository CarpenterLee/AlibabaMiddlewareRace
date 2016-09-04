package hoolee.cache;

import java.util.concurrent.atomic.AtomicLong;

public class ResultCache<K, V> {
	private FixedHashMap<K, V> result_cache;
	private AtomicLong get_times = new AtomicLong();
	private AtomicLong hit_times = new AtomicLong();
	public ResultCache(int max_result_size){
		result_cache = new FixedHashMap<K, V>(max_result_size);
	}
	public void put(K key, V value){
		synchronized(result_cache){
			result_cache.put(key, value);
		}
	}
	public V get(K key){
		long times = get_times.getAndIncrement();
		if(times%10000 == 0){
			double hit_tatio = 1.0*hit_times.get()/get_times.get();
			System.out.println(this + ", ResultCache.size()=" + result_cache.size() + "\t" + hit_tatio);
		}
		synchronized(result_cache){
			V v = result_cache.remove(key);
			if(v != null){
				result_cache.put(key, v);
				hit_times.incrementAndGet();
			}
			return v;
		}
	}
}
