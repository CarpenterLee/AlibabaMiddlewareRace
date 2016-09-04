package hoolee.util;


public class Pair<K, V> implements Comparable<Pair<K, V>>{
	public K key;
	public V value;
	public Pair(K key, V value){
		this.key = key;
		this.value = value;
	}
	@Override
	public int compareTo(Pair<K, V> other){
		return ((Comparable<K>)key).compareTo(other.key);
	}
	@Override
	public String toString() {
		return "("+key+", "+value+")";
	}
	
}
