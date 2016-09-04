package hoolee.index;

import java.io.File;
import java.util.ArrayList;
import java.util.Map;

import avro.io.MyDecoder;
import gzx.cache.buffer.BufferPoolInputStream;
import hoolee.cache.BTreeNodeCache;
import hoolee.sort.MyUUID;
import hoolee.sort.TypeInference;
import hoolee.util.Pair;
/**
 * 用于文件存储在多个目录下的检索
 */
public class BTreeNodeLookUpMutiDir<E> {
	private static final long NOT_AN_OFFSET = -1;
	private ArrayList<E> division_value_list = new ArrayList<E>();
	private ArrayList<Integer> file_number_list = new ArrayList<Integer>();
	private ArrayList<Long> offset_list = new ArrayList<Long>();
	private int treeLevel = 0;
	private long nodeOffset = 0;
	private TypeInference.Type type;
	private int file_number;
	private Map<Integer, File> leafNodeNumber_filePath;
	private BTreeNodeCache<E> cache;
	
	public BTreeNodeLookUpMutiDir(Map<Integer, File> leafNodeNumber_filePath, TypeInference.Type type, 
			int max_medium_node_in_cache, int max_leaf_node_in_cache){
			this(leafNodeNumber_filePath, BTreeNodeLevel.LEVEL_ROOT, 0, 0, type, 
					new BTreeNodeCache<E>(max_medium_node_in_cache, max_leaf_node_in_cache));
	}
	private BTreeNodeLookUpMutiDir(Map<Integer, File> leafNodeNumber_filePath, 
			int treeLevel, int file_number, long nodeOffset, 
			TypeInference.Type type, BTreeNodeCache<E> cache){
		this.leafNodeNumber_filePath = leafNodeNumber_filePath;
		this.treeLevel = treeLevel;
		this.nodeOffset = nodeOffset;
		this.type = type;
		this.file_number = file_number;
		this.cache = cache;
		try {
			readNode();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	/**返回(sorted文件编号，文件内偏移地址)*/
	public Pair<Integer, Long> lookUp(E key){
		return lookUp2(key);
	}
	private Pair<Integer, Long> lookUp2(E key){
		if(key == null){
			return null;
		}
		Pair<Integer, Long> medium_node_n_f = findNodeNumberAndOffset_BinSearch(key);// 从root找到对应的medium节点
		if(medium_node_n_f == null)
			return null;
		BTreeNodeLookUpMutiDir<E> medium_node = medium_node = cache.getMediumNodeCache(medium_node_n_f.value);
		if(medium_node == null){// medium node miss
			medium_node = new BTreeNodeLookUpMutiDir<E>(leafNodeNumber_filePath,  
				BTreeNodeLevel.LEVEL_MEDIUM, 0, medium_node_n_f.value, type, cache);
		}
		long leaf_node_offset = medium_node.findLeafNodeOffset(key);// 从medium找到对应的leaf节点
		if(leaf_node_offset == NOT_AN_OFFSET)
			return null;
		BTreeNodeLookUpMutiDir<E> leaf_node = cache.getLeafNodeCache(medium_node_n_f.key, leaf_node_offset);
		if(leaf_node == null){// leaf node miss
			leaf_node = new BTreeNodeLookUpMutiDir<E>(leafNodeNumber_filePath, 
				BTreeNodeLevel.LEVEL_LEAF, medium_node_n_f.key, leaf_node_offset, type, cache);
		}
		Pair<Integer, Long> rs = leaf_node.findOriginalFileOffset(key);
		return rs;
	}
	/**返回(sorted文件编号，文件内偏移地址)*/
	private Pair<Integer, Long> findOriginalFileOffset(E key){
		int idx = division_value_list.indexOf(key);
		if(idx == -1)
			return null;
		return new Pair<Integer, Long>(file_number_list.get(idx), offset_list.get(idx));
	}
	private long findLeafNodeOffset(E key){
		Pair<Integer, Long> n_f = findNodeNumberAndOffset_BinSearch(key);
		if(n_f == null)
			return NOT_AN_OFFSET;
		return n_f.value;
	}
	private Pair<Integer, Long> findNodeNumberAndOffset_BinSearch(E key){
		int i = 0;
		int j = division_value_list.size()-1;
		Comparable key_cmp = (Comparable<?>)key;
		while(i <= j){
			int mid = (i+j)>>1;
			E e_mid = division_value_list.get(mid);
			if(key_cmp.compareTo(e_mid) > 0){
				i = mid+1;
			}else{
				j = mid-1;
			}
		}
		if(i >= division_value_list.size()){
			return null;
		}
		return new Pair<Integer, Long>(i, offset_list.get(i));	
	}
	private Pair<Integer, Long> findNodeNumberAndOffset(E key){
		int i = 0;
		while(i<division_value_list.size() && 
				((Comparable)key).compareTo(division_value_list.get(i))>0){
			++i;
		}
		if(i >= division_value_list.size())
			return null;
		return new Pair<Integer, Long>(i, offset_list.get(i));
			
	}
	private void readNode() throws Exception{
		File node_file = null;
		if(treeLevel == BTreeNodeLevel.LEVEL_LEAF){
			node_file = leafNodeNumber_filePath.get(file_number);
		}else{
			File baseDir = leafNodeNumber_filePath.get(0).getParentFile();
			node_file = new File(baseDir, treeLevel+".idx");
		}
//		FileInputStream in_stream = new FileInputStream(node_file);
//		FileChannel channel = in_stream.getChannel();
//		channel.position(nodeOffset);
		BufferPoolInputStream in_stream = new BufferPoolInputStream(node_file);
		in_stream.postion(nodeOffset);
		MyDecoder decoder = new MyDecoder(in_stream);
//		int division_value_list_size = decoder.readInt();
		int division_value_list_size = decoder.readLineLength();
		for(int i=0; i<division_value_list_size; ++i){
			switch(type){
			case BOOLEAN:
				division_value_list.add((E)Boolean.valueOf(decoder.readBoolean()));
				break;
			case LONG:
				division_value_list.add((E)Long.valueOf(decoder.readLong()));
				break;
			case DOUBLE:
				division_value_list.add((E)Double.valueOf(decoder.readDouble()));
				break;
			case STR_UUID:
				String uuid_prefix = decoder.readString();
				String uuid = MyUUID.byte16ToUUID_WithPrefix(uuid_prefix, decoder.buf, decoder.readFixed(16));
				division_value_list.add((E)uuid);
				break;
			case STR_HALFUUID:
				String uuid_prefix2 = decoder.readString();
				String half_uuid = MyUUID.byte8ToHALFUUID_WithPrefix(uuid_prefix2, decoder.buf, decoder.readFixed(8));
				division_value_list.add((E)half_uuid);
				break;
			case UUID:
				division_value_list.add((E)MyUUID.byte16ToUUID(decoder.buf, decoder.readFixed(16)));
				break;
			default: // STRING
				division_value_list.add((E)decoder.readString());
			}
		}
		int file_number_list_size = decoder.readInt();
		for(int i=0; i<file_number_list_size; ++i){
			file_number_list.add(decoder.readInt());
		}
		int offset_list_size = decoder.readInt();
		for(int i=0; i<offset_list_size; ++i){
			offset_list.add(decoder.readLong());
		}
		in_stream.close();
		if(treeLevel == BTreeNodeLevel.LEVEL_LEAF){// put into cache
			cache.putLeafNode(file_number, nodeOffset, this);
		}else if(treeLevel == BTreeNodeLevel.LEVEL_MEDIUM){
			cache.putMediumNode(nodeOffset, this);
		}
	}
}
