package hoolee.batch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import avro.io.MyDecoder;
import hoolee.index.BTreeNodeLevel;
import hoolee.index.BTreeNodeMakeTree2;
import hoolee.io.Dir;
import hoolee.sort.MyUUID;
import hoolee.sort.TypeInference;
import hoolee.util.Pair;

public class Indexing {
	/**
	 * 根据offsetFiles指定的offset文件建立索引，索引存储目录结构跟offsetFiles目录结构相同。
	 * @param offsetFiles offset文件的二维数组，顺序遍历该数组，就是文件编号顺序。
	 * @param offsetSizeFile
	 * @param indexOutDirName
	 * @param type
	 * @param threadPoolSize
	 * @return 返回leafNode存储文件的id到对应file的映射
	 */
	public Map<Integer, File> indexing(final File[][] offsetFiles, File offsetSizeFile, 
			String indexOutDirName, final TypeInference.Type type, int threadPoolSize) throws Exception{
		System.out.println("####method indexing()");
		System.out.println("offsetFiles=");
		for(int i=0; i<offsetFiles.length; ++i){
			System.out.println("offsetFiles[" + i + "].length=" + offsetFiles[i].length);
			for(int j=0; j<offsetFiles[i].length; ++j)
				System.out.println(offsetFiles[i][j] + "\t" + offsetFiles[i][j].length());
		}
		System.out.println("offsetSizeFile=" + offsetSizeFile);
		System.out.println("indexOutDirName=" + indexOutDirName);
		System.out.println("type=" + type);
		System.out.println("threadPoolSize=" + threadPoolSize);
		
		final File[] indexOutDirs = new File[offsetFiles.length];
		int offsetFiles_count = 0;
		for(int i=0; i<indexOutDirs.length; ++i){
			indexOutDirs[i] = Dir.mkDirsAndClean(
					new File(offsetFiles[i][0].getParentFile().getParentFile(), indexOutDirName));
			offsetFiles_count += offsetFiles[i].length;
		}
		final File[] offsetInFiles = new File[offsetFiles_count];
		int t0 = 0;
		for(int i=0; i<offsetFiles.length; ++i){
			for(int j=0; j<offsetFiles[i].length; ++j){
				offsetInFiles[t0++] = offsetFiles[i][j];
			}
		}
		
		final Pair<Integer, Long>[] fileNumber_lines = readOffsetSizeFile2(offsetSizeFile).toArray(new Pair[0]);
//		System.out.println("fileNumber_lines=" + Arrays.toString(fileNumber_lines));
		final long total_lines = fileNumber_lines[fileNumber_lines.length-1].value;
		final int node_size = (int)Math.ceil(Math.pow(total_lines, 1.0/3));
		final int parallelism = node_size;
//		int offset_files = fileNumber_lines.length-1;
		System.out.println("Btree node_size=" + node_size);
		int medium_node_per_thread = (int)(node_size/parallelism);
		int left_node = (int)(node_size%parallelism);
		ExecutorService executors = Executors.newFixedThreadPool(threadPoolSize);
		ExecutorCompletionService<Pair<Integer, BTreeNodeMakeTree2<Comparable<?>>>> completionService = 
				new ExecutorCompletionService<Pair<Integer, BTreeNodeMakeTree2<Comparable<?>>>>(executors);
		final FileOutputStream outputStream_medium = new FileOutputStream(new File(indexOutDirs[0], BTreeNodeLevel.LEVEL_MEDIUM+".idx"));
		int cur_medium_node = 0;
		final ConcurrentHashMap<Integer, File> leafNodeNumber_filaPath_map = new ConcurrentHashMap<Integer, File>();
		for(int i=0; i<parallelism; ++i){
			final int medium_id = i;
			final int bg_medium_node = cur_medium_node;// inclusive
			if(i<parallelism-left_node){// medium_node_per_thread
				cur_medium_node += medium_node_per_thread;
			}else{// medium_node_per_thread+1
				cur_medium_node += medium_node_per_thread+1;
			}
			final int ed_medium_node = cur_medium_node;// exclusive
			completionService.submit(new Callable<Pair<Integer, BTreeNodeMakeTree2<Comparable<?>>>>(){
				@Override
				public Pair<Integer, BTreeNodeMakeTree2<Comparable<?>>> call() throws Exception{
					try{
//					synchronized(IndexMultiThread.this){
//					System.out.println("thread call()----" + medium_id);
					long bg_lines_to_index = bg_medium_node*node_size*node_size;// inclusive
					long ed_lines_to_index = Math.min(ed_medium_node*node_size*node_size, total_lines);// exclusive
					if(bg_lines_to_index>=total_lines){// 数据已被分完,没有东西可算
						return new Pair<Integer, BTreeNodeMakeTree2<Comparable<?>>>(medium_id, null);
					}
					
					Pair<Integer, Long> bg_physical = offsetInFiles(fileNumber_lines, bg_lines_to_index);
					Pair<Integer, Long> ed_physical = offsetInFiles(fileNumber_lines, ed_lines_to_index-1);
					BTreeNodeMakeTree2<Comparable<?>> medium_node = 
							new BTreeNodeMakeTree2<Comparable<?>>(outputStream_medium, BTreeNodeLevel.LEVEL_MEDIUM, type);
					// 同一个medium_node的所有leaf_node会存在同一个文件里，文件编号为medium_id
					File leafFile = new File(indexOutDirs[medium_id%indexOutDirs.length], BTreeNodeLevel.LEVEL_LEAF+"_"+medium_id+".idx");
					FileOutputStream outputStream_leaf = new FileOutputStream(leafFile);
					leafNodeNumber_filaPath_map.put(medium_id, leafFile);
					BTreeNodeMakeTree2<Comparable<?>> leaf_node = new BTreeNodeMakeTree2<Comparable<?>>(outputStream_leaf, 
							BTreeNodeLevel.LEVEL_LEAF, type);
					if(bg_physical.key.equals(ed_physical.key)){// only one file
						leaf_node = indexingOffsetFile(offsetInFiles[bg_physical.key], bg_physical.key, 
								bg_physical.value, ed_physical.value, 
								medium_node, leaf_node, node_size, type);
					}else{// more than one file
						leaf_node = indexingOffsetFile(offsetInFiles[bg_physical.key], bg_physical.key, bg_physical.value, 
								fileNumber_lines[bg_physical.key].value-1, 
								medium_node, leaf_node, node_size, type);
						for(int f=bg_physical.key+1; f<ed_physical.key; ++f){
							leaf_node = indexingOffsetFile(offsetInFiles[f], f, 0, fileNumber_lines[f].value-1, 
									medium_node, leaf_node, node_size, type);
						}
						leaf_node = indexingOffsetFile(offsetInFiles[ed_physical.key], ed_physical.key, 0, ed_physical.value, 
								medium_node, leaf_node, node_size, type);
					}
					if(leaf_node.size() > 0){
						leaf_node.writeOut();
						medium_node.add(leaf_node.getMaxDivision(), leaf_node.writeOut());
					}
//					System.out.println("thread " + medium_id + ", medium_node.size()=" + medium_node.size());
					if(medium_node.size() > node_size){
						throw new RuntimeException("medium_node.size() is too big: " + medium_node.size() + ", thread " + medium_id);
					}
					outputStream_leaf.close();
					return new Pair<Integer, BTreeNodeMakeTree2<Comparable<?>>>(medium_id, medium_node);
//					}
					}catch(Exception e){
						e.printStackTrace();
						System.exit(-1);
					}
					return null;
				}// call() 
			});// Callable
		}// for 
		TreeSet<Pair<Integer, BTreeNodeMakeTree2<Comparable<?>>>> medium_node_set = 
				new TreeSet<Pair<Integer, BTreeNodeMakeTree2<Comparable<?>>>>();
		for(int i=0; i<parallelism; ++i){// wait completion
			Pair<Integer, BTreeNodeMakeTree2<Comparable<?>>> rs = completionService.take().get();
			medium_node_set.add(rs);
		}
		executors.shutdown();
//		System.out.println(medium_node_set);
		FileOutputStream outputStream_root = new FileOutputStream(new File(indexOutDirs[0], BTreeNodeLevel.LEVEL_ROOT+".idx"));
//		FileOutputStream outputStream_root = new FileOutputStream(indexOutDir+"/"+BTreeNodeLevel.LEVEL_ROOT+".idx");
		BTreeNodeMakeTree2<Comparable<?>> root_node = 
				new BTreeNodeMakeTree2<Comparable<?>>(outputStream_root, BTreeNodeLevel.LEVEL_ROOT, type);
		for(Pair<Integer, BTreeNodeMakeTree2<Comparable<?>>> mdm : medium_node_set){
			if(mdm.value == null){
				break;
			}
			root_node.add(mdm.value.getMaxDivision(), mdm.value.writeOut());
		}
		root_node.writeOut();
		outputStream_medium.close();
		outputStream_root.close();
		return leafNodeNumber_filaPath_map;
	}
	/**
	 * 重offsetFile的bg_lines开始创建索引，并将结果写入到leaf_node中，
	 * 如果leaf_node写满，则合并到medium_node，同时创建新的leaf_node。
	 * @param offsetFile
	 * @param fileNumber offet文件编号
	 * @param bg_lines 起始行（从0开始编号），inclusive
	 * @param ed_lines 终止行，inclusive
	 * @param medium_node
	 * @param leaf_node
	 * @param buckets_per_node
	 * @return 返回最后一个leaf_node
	 */
	private BTreeNodeMakeTree2<Comparable<?>> indexingOffsetFile(File offsetFile, int fileNumber,  
			long bg_lines, long ed_lines,
			BTreeNodeMakeTree2<Comparable<?>> medium_node, 
			BTreeNodeMakeTree2<Comparable<?>> leaf_node,
			int buckets_per_node,
			TypeInference.Type type) throws Exception{
		FileInputStream in_stream = new FileInputStream(offsetFile);
		MyDecoder decoder = new MyDecoder(in_stream, 1024*1024);
		long lines_count = 0;
		for(; lines_count<bg_lines; lines_count++){// skip lines
			switch(type){
			case BOOLEAN:
				decoder.readBoolean();break;
			case LONG:
				decoder.readLong();break;
			case DOUBLE:
//				decoder.readDouble();break;
				decoder.skipFixed(8);break;
			case STR_UUID:
				decoder.skipString();
				decoder.skipFixed(16);
				break;
			case STR_HALFUUID:
				decoder.skipString();
				decoder.skipFixed(8);
				break;
			case UUID:
				decoder.skipFixed(16);break;
			default:
				decoder.skipString();
			}
			decoder.readLong();
		}
//		byte[] uuid_bytes = new byte[16];
		for(; lines_count<=ed_lines; lines_count++){// lines to index
			Comparable<?> key = null;
			switch(type){
			case BOOLEAN:
				key = decoder.readBoolean();break;
			case LONG:
				key = decoder.readLong();break;
			case DOUBLE:
				key = decoder.readDouble();break;
			case STR_UUID:
				String uuid_prefix = decoder.readString();
				key = MyUUID.byte16ToUUID_WithPrefix(uuid_prefix, decoder.buf, decoder.readFixed(16));
				break;
			case STR_HALFUUID:
				String uuid_prefix2 = decoder.readString();
				key = MyUUID.byte8ToHALFUUID_WithPrefix(uuid_prefix2, decoder.buf, decoder.readFixed(8));
				break;
			case UUID:
				key = MyUUID.byte16ToUUID(decoder.buf, decoder.readFixed(16));
				break;
			default:
				key = decoder.readString();
			}
			long offset = decoder.readLong();
			if(leaf_node.size() >= buckets_per_node){// leaf is full
				long leaf_node_offset = leaf_node.writeOut();
				medium_node.add(leaf_node.getMaxDivision(), leaf_node_offset);
				leaf_node = new BTreeNodeMakeTree2<Comparable<?>>(leaf_node.getFileOutputStream(), 
						BTreeNodeLevel.LEVEL_LEAF, type);
			}
			leaf_node.add(key, fileNumber, offset);
		}
		in_stream.close();
		return leaf_node;
		
	}
	/**
	 * 将全局行号（从0开始）转换成对应的offset文件和文件内行号（从0开始）。
	 * @param fileNumber_lines
	 * @param global_line_number 从0开始计数的全局行号
	 * @return offset_file_number, line_number 
	 */
	private Pair<Integer, Long> offsetInFiles(final Pair<Integer, Long>[] fileNumber_lines, long global_line_number){
		int i = 0;
		long lines_count = global_line_number+1;
		while(lines_count-fileNumber_lines[i].value > 0){
			lines_count -= fileNumber_lines[i].value;
			i++;
		}
		if(i == fileNumber_lines.length-1){
			throw new RuntimeException("illegal offsetFile number: " + i);
		}
		return new Pair<Integer, Long>(i, lines_count-1);
	}
	private TreeSet<Pair<Integer, Long>> readOffsetSizeFile2(File offsetSizeFile) throws Exception{
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(offsetSizeFile)), 1024*1024);
		String line;
		TreeSet<Pair<Integer, Long>> fileNumber_lines = new TreeSet<Pair<Integer, Long>>();
		long total_lines = 0;
		while((line=reader.readLine()) != null){
			int idx = line.indexOf('\t');
			int fileNumber = Integer.parseInt(line.substring(0, idx));
			long lines_in_file = Long.parseLong(line.substring(idx+1));
			total_lines += lines_in_file;
			fileNumber_lines.add(new Pair<Integer, Long>(fileNumber, lines_in_file));
		}
		reader.close();
		fileNumber_lines.add(new Pair<Integer, Long>(fileNumber_lines.size(), total_lines));
//		System.out.println("fileNumber_lines: " + fileNumber_lines);
		System.out.println("distinct_lines_to_index=" + total_lines);
		return fileNumber_lines;
	}
}
