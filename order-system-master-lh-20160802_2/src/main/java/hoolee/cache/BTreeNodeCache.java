package hoolee.cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import hoolee.index.BTreeNodeLookUpMutiDir;

/**
 * 用于BTreeNodeLookUpMutiDir对象的的缓存
 * @author hadoop
 * 2016.07.21
 */
public class BTreeNodeCache<E> {
	private ConcurrentHashMap<Long, BTreeNodeLookUpMutiDir<E>> mediumNode_cache;
	private FixedHashMap<Integer, BTreeNodeLookUpMutiDir<E>> leafNode_cache;
//	private AtomicLong get_times = new AtomicLong(1); 
//	private AtomicLong leaf_hit_times = new AtomicLong(); 
	
	public BTreeNodeCache(int max_medium_node_in_cache, int max_leaf_node_in_cache){
		mediumNode_cache = new ConcurrentHashMap<Long, BTreeNodeLookUpMutiDir<E>>(max_medium_node_in_cache);
		leafNode_cache = new FixedHashMap<Integer, BTreeNodeLookUpMutiDir<E>>(max_leaf_node_in_cache);
	}

	public void putLeafNode(int file_number, long begin_offset, BTreeNodeLookUpMutiDir<E> leaf_node){
		return; // do not cache leaf node
//		synchronized(leafNode_cache){
//			leafNode_cache.put(hashCodeForIntLong(file_number, begin_offset), leaf_node);
//		}
	}
	public BTreeNodeLookUpMutiDir<E> getLeafNodeCache(int file_number, long begin_offset){
		return null; // leaf node will always miss
//		long times = get_times.getAndIncrement();
//		if(times%20000 == 0){
//			double leaf_hit_ratio = 1.0*leaf_hit_times.get()/get_times.get();
//			System.out.println(this + ", BTreeNodeCache, mediumNode_cache.size()=" + mediumNode_cache.size()
//									+ ", leafNode_cache.size()=" + leafNode_cache.size() + "\t" + leaf_hit_ratio);
//		}
//		int t = hashCodeForIntLong(file_number, begin_offset);
//		synchronized(leafNode_cache){
//			 BTreeNodeLookUpMutiDir<E> leaf = leafNode_cache.remove(t);;
//			 if(leaf != null){
//				 leaf_hit_times.incrementAndGet();
//				 leafNode_cache.put(t, leaf);
//			 }
//			return leaf;
//		}
	}
	/**
	 * 由于同一次索引的所有medium node都存在同一个文件中（2.idx），
	 * 因此可以用每个medium node的begin offset来唯一标识该node
	 */
	public void putMediumNode(long begin_offest, BTreeNodeLookUpMutiDir<E> medium_node){
		mediumNode_cache.putIfAbsent(begin_offest, medium_node);
	}
	/**
	 * 根据medium node的begin_offset得到MediumNode的缓存（如果缓存过的话），
	 * 该方法并不负责从硬盘上读取数据。
	 */
	public BTreeNodeLookUpMutiDir<E> getMediumNodeCache(long begin_offest){
		return mediumNode_cache.get(begin_offest);
	}
	private int hashCodeForIntLong(int file_number, long begin_offset){
		return ((int)begin_offset<<11) | file_number;
	}
}
