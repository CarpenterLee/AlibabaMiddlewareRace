package hoolee.util;

public class Trinal<K, E, M> implements Comparable<Trinal<K, E, M>>{
	public K first;
	public E second;
	public M third;
	public Trinal(K first, E second, M third) {
		this.first = first;
		this.second = second;
		this.third = third;
	}
	@Override
	public int compareTo(Trinal<K, E, M> other){
		return ((Comparable<K>)first).compareTo(other.first);
	}
	@Override
	public String toString(){
		return "(" + first + ", " + second + ", " + third + ")";
	}
}
