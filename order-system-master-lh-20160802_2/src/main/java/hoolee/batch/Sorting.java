package hoolee.batch;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import avro.io.MyDecoder;
import avro.io.BufferedBinaryEncoder;
import hoolee.io.Dir;
import hoolee.sort.KeysNumber;
import hoolee.sort.MyUUID;
import hoolee.sort.TypeInference;
import hoolee.util.DistinctDisk;
import hoolee.util.Pair;
import hoolee.util.Trinal;

public class Sorting {
	/**
	 * 对secondary offset文件进行排序。
	 * sorting之后的文件均分到每个sortOutDirs指定的目录中, 
	 * offset文件会均匀分布到到offsetOutDirs指定的目录中，
	 * 结构跟taggedFiles相同
	 * @param secondaryOffsetTaggedFiles 分类好文件的二维数组，顺序遍历该数组，就是文件编号顺序。
	 * @param sortOutDirName
	 * @param offsetOutDirName
	 * @param sortBy
	 * @param keyNumber
	 * @param typeInference
	 * @param threadPoolSize
	 * @return sortted文件编号->文件路径的映射，以及，
	 * 所有offset文件（目录结构跟secondaryOffsetFiles的相同）。
	 */
	public Pair<Map<Integer, File>, File[][]> sortSecondaryOffsetFiles(final File[][] secondaryOffsetTaggedFiles, final String sortOutDirName, 
			final String offsetOutDirName, final TypeInference.Type keyType,
			int threadPoolSize) throws Exception{
		System.out.println("####method sortSecondaryOffsetFiles()");
		System.out.println("secondaryOffsetTaggedFiles=");
		for(int i=0; i<secondaryOffsetTaggedFiles.length; ++i){
			System.out.println("secondaryOffsetTaggedFiles[" + i + "].length=" + secondaryOffsetTaggedFiles[i].length);
			for(int j=0; j<secondaryOffsetTaggedFiles[i].length; ++j){
				System.out.println(secondaryOffsetTaggedFiles[i][j] + "\t" + secondaryOffsetTaggedFiles[i][j].length());
			}
		}
		System.out.println("sortOutDirName=" + sortOutDirName);
		System.out.println("offsetOutDirName=" + offsetOutDirName);
		System.out.println("keyType=" + keyType);
		System.out.println("threadPoolSize=" + threadPoolSize);
		
		if(keyType != TypeInference.Type.LONG){
			throw new RuntimeException("Only LONG keyType is allowed!!");
		}
		final File[] sortOutDirs = new File[secondaryOffsetTaggedFiles.length];
		final File[] offsetOutDirs = new File[secondaryOffsetTaggedFiles.length];
		final File[][] offsetFiles = new File[secondaryOffsetTaggedFiles.length][];
		for(int i=0; i<secondaryOffsetTaggedFiles.length; ++i){
//			if(secondaryOffsetTaggedFiles[i].length == 0){//存储目录比文件个数多时，有的目录将分不到文件。
//				continue;
//			}
			File p_f = secondaryOffsetTaggedFiles[i][0].getParentFile().getParentFile();
			sortOutDirs[i] = Dir.mkDirsAndClean(new File(p_f, sortOutDirName));// 跟taggedFiles使用相同的上级目录
			offsetOutDirs[i] = Dir.mkDirsAndClean(new File(p_f, offsetOutDirName));
			offsetFiles[i] = new File[secondaryOffsetTaggedFiles[i].length];
		}
//		final TypeInference.Type key_type = typeInference.getType(sortBy);
		int buckets = 0;
		int max_row_len = 0;
		for(int i=0; i<secondaryOffsetTaggedFiles.length; ++i){
			buckets += secondaryOffsetTaggedFiles[i].length;
			max_row_len = Math.max(max_row_len, secondaryOffsetTaggedFiles[i].length);
		}
		final Object[] disk_read_write_locks = DistinctDisk.newLocks();
		ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
		ExecutorCompletionService<Pair<Integer, Long>> completionService = // <ofst_file_id, distinct_key_count>
				new ExecutorCompletionService<Pair<Integer, Long>>(executor);
		final ConcurrentHashMap<Integer, File> sorttedFileNumber_filePath = new ConcurrentHashMap<Integer, File>();
		for(int j=0; j<max_row_len; ++j){// load balancing, sorting by collumn
			for(int i=0; i<secondaryOffsetTaggedFiles.length; ++i){
				if(j >= secondaryOffsetTaggedFiles[i].length){
					continue;
				}
				final int jj = j;
				final int ii = i;
				completionService.submit(new Callable<Pair<Integer, Long>>(){ 
					@Override
					public Pair<Integer, Long> call() throws Exception{
						try{
						FileInputStream in_stream = new FileInputStream(secondaryOffsetTaggedFiles[ii][jj]);
						Object lock = DistinctDisk.getLockOfFile(disk_read_write_locks, secondaryOffsetTaggedFiles[ii][jj]);
						MyDecoder decoder = new MyDecoder(in_stream, 1024*1024*40, lock);
						ArrayList<Trinal<Long, Integer, Long>> result1 = new ArrayList<Trinal<Long, Integer, Long>>();
						while(!decoder.isEnd()){
							long key1 = decoder.readLong();
							int fileNumber1 = decoder.readInt();
							long offset1 = decoder.readLong();
							result1.add(new Trinal<Long, Integer, Long>(key1, fileNumber1, offset1));
						}
						Collections.sort(result1);
						int file_number = Integer.parseInt(secondaryOffsetTaggedFiles[ii][jj].getName().split("\\.")[0]);
						File cur_sorttedFile = new File(sortOutDirs[ii], file_number+".st");
						System.out.println("result.size()=" + result1.size());
						System.out.println("cur_sortted_out_file: " + cur_sorttedFile);
						FileOutputStream fileOutputStream_sort = new FileOutputStream(cur_sorttedFile);
						sorttedFileNumber_filePath.put(file_number, cur_sorttedFile);
						offsetFiles[ii][jj] = new File(offsetOutDirs[ii], file_number+".ofst");
						FileOutputStream fileOutputStream_offset = new FileOutputStream(offsetFiles[ii][jj]);
						BufferedBinaryEncoder encoder_sort = new BufferedBinaryEncoder(fileOutputStream_sort, 1024*512);
						BufferedBinaryEncoder encoder_offset = new BufferedBinaryEncoder(fileOutputStream_offset, 1024*512);
						synchronized(DistinctDisk.getLockOfFile(disk_read_write_locks, cur_sorttedFile)){
						long cur_offset = 0;
						for(Trinal<Long, Integer, Long> tr : result1){
							long key1 = tr.first;
							int fileNumber1 = tr.second;
							long offset1 = tr.third;
							encoder_offset.writeLong(key1);
							encoder_offset.writeLong(cur_offset);
							cur_offset += encoder_sort.writeLongWithReturn(key1);
							cur_offset += encoder_sort.writeIntWithReturn(fileNumber1);
							cur_offset += encoder_sort.writeLongWithReturn(offset1);
						}
//						System.out.println("cur_offset=" + cur_offset + ", distinct_key_count=" + distinct_key_count);
						encoder_sort.flush();
						encoder_offset.flush();
						fileOutputStream_sort.close();
						fileOutputStream_offset.close();
//						writer.close();
//						System.out.println("sort_result.size()=" + result.size());
						return new Pair<Integer, Long>(
								Integer.parseInt(secondaryOffsetTaggedFiles[ii][jj].getName().split("\\.")[0]), 
								Long.valueOf(result1.size()));
						}// synchronized
						}catch(Exception e){
							e.printStackTrace();
							System.exit(-1);
						}
						return null;
					}// call()
				});// Callable
			}
		}
		BufferedWriter writer_0_sz = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(new File(offsetOutDirs[0], "0.size"))), 1024*64);
		System.out.println(offsetOutDirs[0] + "/0.size:");
		for(int i=0; i<buckets; ++i){// wait completion
			Pair<Integer, Long> fileNumber_lines =  completionService.take().get();
			writer_0_sz.append(String.valueOf(fileNumber_lines.key))
					.append('\t')
					.append(String.valueOf(fileNumber_lines.value));
			writer_0_sz.newLine();
			System.out.println(fileNumber_lines);
		}
		writer_0_sz.close();
		executor.shutdown();
		return new Pair<Map<Integer, File>, File[][]>(sorttedFileNumber_filePath, offsetFiles);
	}
	
	/**
	 * 在1次排序中生成两种offset，键值分别由sortBy和secondKey指定。
	 * sorting之后的文件均分到每个sortOutDirs指定的目录中, 
	 * offset文件会均匀分到到offsetOutDirs指定的目录中，
	 * 结构跟taggedFiles相同
	 * @param taggedFiles 分类好文件的二维数组，顺序遍历该数组，就是文件编号顺序。
	 * @param sortOutDirName
	 * @param offsetOutDirName_primary
	 * @param sortBy
	 * @param secondKey 第二种offset的键值
	 * @param keyNumber
	 * @param typeInference
	 * @param threadPoolSize
	 * @return sortted文件编号->文件路径的映射，以及，
	 * secondary offset文件编号->文件路径的映射，以及，
	 * 所有sortBy的offset文件（目录结构跟taggedFiles的相同），以及，
	 * 所有secondKey的offset文件（目录结构跟taggedFiles的相同）。
	 */
	public Pair<ArrayList<Map<Integer, File>>, ArrayList<File[][]>> sortWithDualOffsetOut(final File[][] taggedFiles, final String sortOutDirName, 
			final String offsetOutDirName_primary, final String sortBy, 
			final String offsetOutDirName_secondary, final String secondKey, 
			final KeysNumber keyNumber, final TypeInference typeInference, int threadPoolSize) throws Exception{
		System.out.println("####method sortingWithDualOffset()");
		System.out.println("taggedFiles=");
		for(int i=0; i<taggedFiles.length; ++i){
			System.out.println("taggedFiles[" + i + "].length=" + taggedFiles[i].length);
			for(int j=0; j<taggedFiles[i].length; ++j){
				System.out.println(taggedFiles[i][j] + "\t" + taggedFiles[i][j].length());
			}
		}
		System.out.println("sortOutDirName=" + sortOutDirName);
		System.out.println("offsetOutDirName_primary=" + offsetOutDirName_primary);
		System.out.println("sortBy=" + sortBy);
		System.out.println("offsetOutDirName_secondary=" + offsetOutDirName_secondary);
		System.out.println("secondKey=" + secondKey);
		System.out.println("threadPoolSize=" + threadPoolSize);
		
		final File[] sortOutDirs = new File[taggedFiles.length];
		final File[] offsetOutDirs_primary = new File[taggedFiles.length];
		final File[][] offsetFiles_primary = new File[taggedFiles.length][];
		final File[] offsetOutDirs_secondary = new File[taggedFiles.length];
		final File[][] offsetFiles_secondary = new File[taggedFiles.length][];
		for(int i=0; i<taggedFiles.length; ++i){
//			if(taggedFiles[i].length == 0){//存储目录比文件个数多时，有的目录将分不到文件。
//				continue;
//			}
			File p_f = taggedFiles[i][0].getParentFile().getParentFile();
			sortOutDirs[i] = Dir.mkDirsAndClean(new File(p_f, sortOutDirName));// 跟taggedFiles使用相同的上级目录
			offsetOutDirs_primary[i] = Dir.mkDirsAndClean(new File(p_f, offsetOutDirName_primary));
			offsetFiles_primary[i] = new File[taggedFiles[i].length];
			offsetOutDirs_secondary[i] = Dir.mkDirsAndClean(new File(p_f, offsetOutDirName_secondary));
			offsetFiles_secondary[i] = new File[taggedFiles[i].length];
		}
		final TypeInference.Type key_type_primary = typeInference.getType(sortBy);
		final TypeInference.Type key_type_secondary = typeInference.getType(secondKey);
		System.out.println("key_type_primary=" + key_type_primary);
		System.out.println("key_type_secondary=" + key_type_secondary);
		
		if(key_type_secondary != TypeInference.Type.LONG){
			throw new RuntimeException("only LONG secondary key type is allowed");
		}
		int buckets = 0;
		int max_row_len = 0;
		for(int i=0; i<taggedFiles.length; ++i){
			buckets += taggedFiles[i].length;
			max_row_len = Math.max(max_row_len, taggedFiles[i].length);
		}
		final Object[] disk_read_write_locks = DistinctDisk.newLocks();
		ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
		ExecutorCompletionService<Pair<Integer, Long>> completionService = // <ofst_file_id, distinct_key_count>
				new ExecutorCompletionService<Pair<Integer, Long>>(executor);
		final ConcurrentHashMap<Integer, File> sorttedFileNumber_filePath = new ConcurrentHashMap<Integer, File>();
		final ConcurrentHashMap<Integer, File> secondaryOffsetFileNumber_filePath = new ConcurrentHashMap<Integer, File>();
		final ConcurrentHashMap<Integer, Long> seconday_FileNumber_lines = new ConcurrentHashMap<Integer, Long>();// 0.size
		for(int j=0; j<max_row_len; ++j){// load balancing, sorting by column
			for(int i=0; i<taggedFiles.length; ++i){
				if(j >= taggedFiles[i].length){
					continue;
				}
				final int jj = j;
				final int ii = i;
				completionService.submit(new Callable<Pair<Integer, Long>>(){ 
					@Override
					public Pair<Integer, Long> call() throws Exception{
						try{
						FileInputStream in_stream = new FileInputStream(taggedFiles[ii][jj]);
						Object lock = DistinctDisk.getLockOfFile(disk_read_write_locks, taggedFiles[ii][jj]);
						MyDecoder decoder = new MyDecoder(in_stream, 1024*1024*60, lock);
						ArrayList<Pair<Pair<Object, Object>, Object>> result = new ArrayList<Pair<Pair<Object, Object>, Object>>();
						int line_size;
//						byte[] line_byte_buf = new byte[1024*64];
//						byte[] uuid_bytes = new byte[16];
						while(!decoder.isEnd()){// every line
							line_size=decoder.readInt();
//							decoder.readFixed(line_byte_buf, 0, line_size);
//							MyDecoder line_decoder = new MyDecoder(
//									new ByteArrayInputStream(line_byte_buf, 0, line_size), line_size+1);
							int bg_pos = decoder.readFixed(line_size);
							MyDecoder line_decoder = new MyDecoder(decoder.buf, bg_pos, line_size);
							Object value_primary = null;
							Object value_secondary = null;
							while(!line_decoder.isEnd()){// every pair in the line
								int key_number = line_decoder.readInt();
								String key_name;
								if(key_number == KeysNumber.NOT_A_KEYNUMBER){
									key_name = line_decoder.readString();
								}else{
									key_name = keyNumber.getKey(key_number);
								}
								Object v_value = null;
								switch(typeInference.getType(key_name)){
								case BOOLEAN:
									v_value = line_decoder.readBoolean();break;
								case LONG:
									v_value = line_decoder.readLong();break;
								case DOUBLE:
									v_value = line_decoder.readDouble();break;
								case STR_UUID:
									if(key_name.equals(sortBy)){
										String uuid_prefix = line_decoder.readString();
										v_value = MyUUID.byte16ToUUID_WithPrefix(uuid_prefix, 
												line_decoder.buf, line_decoder.readFixed(16));
									}else{
										line_decoder.skipString();
										line_decoder.skipFixed(16);
									}
									break;
								case STR_HALFUUID:
									if(key_name.equals(sortBy)){
										String uuid_prefix = line_decoder.readString();
										v_value = MyUUID.byte8ToHALFUUID_WithPrefix(uuid_prefix, 
												line_decoder.buf, line_decoder.readFixed(8));
									}else{
										line_decoder.skipString();
										line_decoder.skipFixed(8);
									}
									break;
								case UUID:
									if(key_name.equals(sortBy)){
										v_value = MyUUID.byte16ToUUID(line_decoder.buf, line_decoder.readFixed(16));
									}else{
										line_decoder.skipFixed(16);
									}
									break;
								default:// STRING
									if(key_name.equals(sortBy)){
										v_value = line_decoder.readString();
									}else{
										line_decoder.skipString();
									}
								}
								if(key_name.equals(sortBy)){
									value_primary = v_value;
								}else if(key_name.equals(secondKey)){
									value_secondary = v_value;
								}
								if(value_primary!=null && value_secondary!=null){// both found
									break;
								}
							}// while every pair
							result.add(new Pair<Pair<Object, Object>, Object>(
									new Pair<Object, Object>(value_primary, value_secondary),
									Arrays.copyOfRange(decoder.buf, bg_pos, bg_pos+line_size)));
						}// while every line
						Collections.sort(result);
						int file_number = Integer.parseInt(taggedFiles[ii][jj].getName().split("\\.")[0]);
						File cur_sorttedFile = new File(sortOutDirs[ii], file_number+".st");
						System.out.println("result.size(): " + result.size());
						System.out.println("cur_sortted_out_file: " + cur_sorttedFile);
						FileOutputStream fileOutputStream_sort = new FileOutputStream(cur_sorttedFile);
						sorttedFileNumber_filePath.put(file_number, cur_sorttedFile);
						offsetFiles_primary[ii][jj] = new File(offsetOutDirs_primary[ii], file_number+".ofst");
						FileOutputStream fileOutputStream_offset_primary = new FileOutputStream(offsetFiles_primary[ii][jj]);
						
						offsetFiles_secondary[ii][jj] = new File(offsetOutDirs_secondary[ii], file_number+".ofst");
						secondaryOffsetFileNumber_filePath.put(file_number, offsetFiles_secondary[ii][jj]);
						FileOutputStream fileOutputStream_offset_secondary = new FileOutputStream(offsetFiles_secondary[ii][jj]);

						BufferedBinaryEncoder encoder_sort = new BufferedBinaryEncoder(fileOutputStream_sort, 1024*1024);
						BufferedBinaryEncoder encoder_offset_primary = new BufferedBinaryEncoder(fileOutputStream_offset_primary, 1024*1024);
						BufferedBinaryEncoder encoder_offset_secondary = new BufferedBinaryEncoder(fileOutputStream_offset_secondary, 1024*1024);
						synchronized(DistinctDisk.getLockOfFile(disk_read_write_locks, cur_sorttedFile)){
						long cur_offset = 0;
						Object last_key = null;
						long distinct_key_count = 0;
						for(Pair<Pair<Object, Object>, Object> k_k_v : result){
							Object v_primary = k_k_v.key.key;
							Object v_secondary = k_k_v.key.value;
							if(!v_primary.equals(last_key)){
								last_key = v_primary;
								distinct_key_count++;
								switch(key_type_primary){
								case BOOLEAN:
									encoder_offset_primary.writeBoolean((Boolean)v_primary);break;
								case LONG:
									encoder_offset_primary.writeLong((Long)v_primary);break;
								case DOUBLE:
									encoder_offset_primary.writeDouble((Double)v_primary); break;
								case STR_UUID:
									String str_uuid = (String)v_primary;
									int idx = str_uuid.indexOf('_');
									encoder_offset_primary.writeString(str_uuid.substring(0, idx));
									encoder_offset_primary.writeFixed(MyUUID.UUIDToByte16(str_uuid.substring(idx+1)));
									break;
								case STR_HALFUUID:
									String str_halfuuid = (String)v_primary;
									int idx2 = str_halfuuid.indexOf('-');
									encoder_offset_primary.writeString(str_halfuuid.substring(0, idx2));
									encoder_offset_primary.writeFixed(MyUUID.HALFUUIDToByte8(str_halfuuid.substring(idx2+1)));
									break;
								case UUID:
									encoder_offset_primary.writeFixed(MyUUID.UUIDToByte16((String)v_primary));
									break;
								default:
									encoder_offset_primary.writeString((String)v_primary);
								}
								encoder_offset_primary.writeLong(cur_offset);
							}
							encoder_offset_secondary.writeLong((Long)v_secondary);
							encoder_offset_secondary.writeIndex(file_number);
							encoder_offset_secondary.writeLong(cur_offset);
							
							byte[] b_arr = (byte[])k_k_v.value;
							cur_offset += encoder_sort.writeIntWithReturn(b_arr.length);
							encoder_sort.writeFixed(b_arr);
							cur_offset += b_arr.length;
						}
//						System.out.println("cur_offset=" + cur_offset + ", distinct_key_count=" + distinct_key_count);
						encoder_sort.flush();
						encoder_offset_primary.flush();
						encoder_offset_secondary.flush();
						fileOutputStream_sort.close();
						fileOutputStream_offset_primary.close();
						fileOutputStream_offset_secondary.close();
						int f_number = Integer.parseInt(taggedFiles[ii][jj].getName().split("\\.")[0]); 
						seconday_FileNumber_lines.put(f_number, Long.valueOf(result.size()));
						return new Pair<Integer, Long>(f_number, distinct_key_count);
						}//synchronized
						}catch(Exception e){
							e.printStackTrace();
							System.exit(-1);
						}
						return null;
					}// call()
				});// Callable
			}
		}
		BufferedWriter writer_0_sz_primary = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(new File(offsetOutDirs_primary[0], "0.size"))), 1024*64);
		System.out.println(offsetOutDirs_primary[0] + "/0.size:");
		for(int i=0; i<buckets; ++i){// wait completion
			Pair<Integer, Long> fileNumber_lines =  completionService.take().get();
			writer_0_sz_primary.append(String.valueOf(fileNumber_lines.key))
					.append('\t')
					.append(String.valueOf(fileNumber_lines.value));
			writer_0_sz_primary.newLine();
			System.out.println(fileNumber_lines);
		}
		BufferedWriter writer_0_sz_secondary = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(new File(offsetOutDirs_secondary[0], "0.size"))), 1024*64);
		System.out.println(offsetOutDirs_secondary[0] + "/0.size:");
		for(Map.Entry<Integer, Long> entry : seconday_FileNumber_lines.entrySet()){// secondary offset 0.size  
			writer_0_sz_secondary.append(String.valueOf(entry.getKey()))
			.append('\t')
			.append(String.valueOf(entry.getValue()));
			writer_0_sz_secondary.newLine();
			System.out.println(entry);
		}
		writer_0_sz_primary.close();
		writer_0_sz_secondary.close();
		executor.shutdown();
		
		ArrayList<Map<Integer, File>> map_list = new ArrayList<Map<Integer, File>>();
		map_list.add(sorttedFileNumber_filePath);
		map_list.add(secondaryOffsetFileNumber_filePath);
		ArrayList<File[][]> rs_list = new ArrayList<File[][]>();
		rs_list.add(offsetFiles_primary);
		rs_list.add(offsetFiles_secondary);
		return new Pair<ArrayList<Map<Integer, File>>, ArrayList<File[][]>>(map_list, rs_list);
	}
	/**
	 * 对tag之后的文件进行排序。
	 * sorting之后的文件均分到每个sortOutDirs指定的目录中, 
	 * offset文件会均匀分布到到offsetOutDirs指定的目录中，
	 * 结构跟taggedFiles相同
	 * @param taggedFiles 分类好文件的二维数组，顺序遍历该数组，就是文件编号顺序。
	 * @param sortOutDirName
	 * @param offsetOutDirName
	 * @param sortBy
	 * @param keyNumber
	 * @param typeInference
	 * @param threadPoolSize
	 * @return sortted文件编号->文件路径的映射，以及，
	 * 所有offset文件（目录结构跟taggedFiles的相同）。
	 */
	public Pair<Map<Integer, File>, File[][]> sortTaggedFiles(final File[][] taggedFiles, final String sortOutDirName, 
			final String offsetOutDirName, final String sortBy, 
			final KeysNumber keyNumber, final TypeInference typeInference, int threadPoolSize) throws Exception{
		System.out.println("####method sortTaggedFiles()");
		System.out.println("taggedFiles=");
		for(int i=0; i<taggedFiles.length; ++i){
			System.out.println("taggedFiles[" + i + "].length=" + taggedFiles[i].length);
			for(int j=0; j<taggedFiles[i].length; ++j){
				System.out.println(taggedFiles[i][j] + "\t" + taggedFiles[i][j].length());
			}
		}
		System.out.println("sortOutDirName=" + sortOutDirName);
		System.out.println("offsetOutDirName=" + offsetOutDirName);
		System.out.println("sortBy=" + sortBy);
		System.out.println("threadPoolSize=" + threadPoolSize);
		
		final File[] sortOutDirs = new File[taggedFiles.length];
		final File[] offsetOutDirs = new File[taggedFiles.length];
		final File[][] offsetFiles = new File[taggedFiles.length][];
		for(int i=0; i<taggedFiles.length; ++i){
			File p_f = taggedFiles[i][0].getParentFile().getParentFile();
			sortOutDirs[i] = Dir.mkDirsAndClean(new File(p_f, sortOutDirName));// 跟taggedFiles使用相同的上级目录
			offsetOutDirs[i] = Dir.mkDirsAndClean(new File(p_f, offsetOutDirName));
			offsetFiles[i] = new File[taggedFiles[i].length];
		}
		final TypeInference.Type key_type = typeInference.getType(sortBy);
		int buckets = 0;
		int max_row_len = 0;
		for(int i=0; i<taggedFiles.length; ++i){
			buckets += taggedFiles[i].length;
			max_row_len = Math.max(max_row_len, taggedFiles[i].length);
		}
		final Object[] disk_read_write_locks = DistinctDisk.newLocks();
		ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
		ExecutorCompletionService<Pair<Integer, Long>> completionService = // <ofst_file_id, distinct_key_count>
				new ExecutorCompletionService<Pair<Integer, Long>>(executor);
		final ConcurrentHashMap<Integer, File> sorttedFileNumber_filePath = new ConcurrentHashMap<Integer, File>();
		for(int j=0; j<max_row_len; ++j){// load balancing, sorting by column
			for(int i=0; i<taggedFiles.length; ++i){
				if(j >= taggedFiles[i].length){
					continue;
				}
				final int jj = j;
				final int ii = i;
				completionService.submit(new Callable<Pair<Integer, Long>>(){ 
					@Override
					public Pair<Integer, Long> call() throws Exception{
						try{
						FileInputStream in_stream = new FileInputStream(taggedFiles[ii][jj]);
						Object lock = DistinctDisk.getLockOfFile(disk_read_write_locks, taggedFiles[ii][jj]);
						MyDecoder decoder = new MyDecoder(in_stream, 1024*1024*60, lock);
						ArrayList<Pair<Object, Object>> result = new ArrayList<Pair<Object, Object>>();
						int line_size;
						while(!decoder.isEnd()){// every line
							line_size=decoder.readInt();
							int bg_pos = decoder.readFixed(line_size);
							MyDecoder line_decoder = new MyDecoder(decoder.buf, bg_pos, line_size);
							Object sortby_value = null;
							while(!line_decoder.isEnd()){// every pair in the line
								int key_number = line_decoder.readInt();
								String key_name;
								if(key_number == KeysNumber.NOT_A_KEYNUMBER){
									key_name = line_decoder.readString();
								}else{
									key_name = keyNumber.getKey(key_number);
								}
								switch(typeInference.getType(key_name)){
								case BOOLEAN:
									sortby_value = line_decoder.readBoolean();break;
								case LONG:
									sortby_value = line_decoder.readLong();break;
								case DOUBLE:
									sortby_value = line_decoder.readDouble();break;
								case STR_UUID:
									if(key_name.equals(sortBy)){
										String uuid_prefix = line_decoder.readString();
										sortby_value = MyUUID.byte16ToUUID_WithPrefix(uuid_prefix, 
												line_decoder.buf, line_decoder.readFixed(16));
									}else{
										line_decoder.skipString();
										line_decoder.skipFixed(16);
									}
									break;
								case STR_HALFUUID:
									if(key_name.equals(sortBy)){
										String uuid_prefix = line_decoder.readString();
										sortby_value = MyUUID.byte8ToHALFUUID_WithPrefix(uuid_prefix, 
												line_decoder.buf, line_decoder.readFixed(8));
									}else{
										line_decoder.skipString();
										line_decoder.skipFixed(8);
									}
									break;
								case UUID:
									if(key_name.equals(sortBy)){
										sortby_value = MyUUID.byte16ToUUID(line_decoder.buf, line_decoder.readFixed(16));
									}else{
										line_decoder.skipFixed(16);
									}
									break;
								default:// STRING
									if(key_name.equals(sortBy)){
										sortby_value = line_decoder.readString();
									}else{
										line_decoder.skipString();
									}
								}
								if(key_name.equals(sortBy)){
									break;
								}
							}// while every pair
//							result.add(new Pair<Object, Object>(sortby_value, Arrays.copyOf(line_byte_buf, line_size)));
							result.add(new Pair<Object, Object>(sortby_value, Arrays.copyOfRange(decoder.buf, bg_pos, bg_pos+line_size)));
						}// while every line
						Collections.sort(result);
						int file_number = Integer.parseInt(taggedFiles[ii][jj].getName().split("\\.")[0]);
						File cur_sorttedFile = new File(sortOutDirs[ii], file_number+".st");
						System.out.println("result.size()=" + result.size());
 						System.out.println("cur_sortted_out_file: " + cur_sorttedFile);
						FileOutputStream fileOutputStream_sort = new FileOutputStream(cur_sorttedFile);
						sorttedFileNumber_filePath.put(file_number, cur_sorttedFile);
						offsetFiles[ii][jj] = new File(offsetOutDirs[ii], file_number+".ofst");
						FileOutputStream fileOutputStream_offset = new FileOutputStream(offsetFiles[ii][jj]);
						BufferedBinaryEncoder encoder_sort = new BufferedBinaryEncoder(fileOutputStream_sort, 1024*1024);
						BufferedBinaryEncoder encoder_offset = new BufferedBinaryEncoder(fileOutputStream_offset, 1024*1024);
						long cur_offset = 0;
						Object last_key = null;
						long distinct_key_count = 0;
						synchronized(DistinctDisk.getLockOfFile(disk_read_write_locks, cur_sorttedFile)){
						for(Pair<Object, Object> p : result){
							if(!p.key.equals(last_key)){
								last_key = p.key;
								distinct_key_count++;
								switch(key_type){
								case BOOLEAN:
									encoder_offset.writeBoolean((Boolean)p.key);break;
								case LONG:
									encoder_offset.writeLong((Long)p.key);break;
								case DOUBLE:
									encoder_offset.writeDouble((Double)p.key); break;
								case STR_UUID:
									String str_uuid = (String)p.key;
									int idx = str_uuid.indexOf('_');
									encoder_offset.writeString(str_uuid.substring(0, idx));
									encoder_offset.writeFixed(MyUUID.UUIDToByte16(str_uuid.substring(idx+1)));
									break;
								case STR_HALFUUID:
									String str_halfuuid = (String)p.key;
									int idx2 = str_halfuuid.indexOf('-');
									encoder_offset.writeString(str_halfuuid.substring(0, idx2));
									encoder_offset.writeFixed(MyUUID.HALFUUIDToByte8(str_halfuuid.substring(idx2+1)));
									break;
								case UUID:
									encoder_offset.writeFixed(MyUUID.UUIDToByte16((String)p.key));
									break;
								default:
									encoder_offset.writeString((String)p.key);
								}
								encoder_offset.writeLong(cur_offset);
							}
							byte[] b_arr = (byte[])p.value;
							cur_offset += encoder_sort.writeIntWithReturn(b_arr.length);
							encoder_sort.writeFixed(b_arr);
							cur_offset += b_arr.length;
						}
						encoder_sort.flush();
						encoder_offset.flush();
						fileOutputStream_sort.close();
						fileOutputStream_offset.close();
//						System.out.println("sort_result.size()=" + result.size());
						}// synchronized(DistinctDisk.getLockOfFile(disk_read_write_locks, cur_sorttedFile))
						return new Pair<Integer, Long>(
								Integer.parseInt(taggedFiles[ii][jj].getName().split("\\.")[0]), distinct_key_count);
						}catch(Exception e){
							e.printStackTrace();
							System.exit(-1);
						}
						return null;
					}// call()
				});// Callable
			}
		}
		BufferedWriter writer_0_sz = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(new File(offsetOutDirs[0], "0.size"))), 1024*64);
		System.out.println(offsetOutDirs[0] + "/0.size:");
		for(int i=0; i<buckets; ++i){// wait completion
			Pair<Integer, Long> fileNumber_lines =  completionService.take().get();
			writer_0_sz.append(String.valueOf(fileNumber_lines.key))
					.append('\t')
					.append(String.valueOf(fileNumber_lines.value));
			writer_0_sz.newLine();
			System.out.println(fileNumber_lines);
		}
		writer_0_sz.close();
		executor.shutdown();
		return new Pair<Map<Integer, File>, File[][]>(sorttedFileNumber_filePath, offsetFiles);
	}
	
	/**二级排序*/
	public Pair<Map<Integer, File>, File[][]> secondarySortTaggedFiles(final File[][] taggedFiles, final String sortOutDirName, 
			final String offsetOutDirName, final String sortBy, final String secondary_sortBy, 
			final KeysNumber keyNumber, final TypeInference typeInference, int threadPoolSize) throws Exception{
		System.out.println("####method secondarySortTaggedFiles()");
		System.out.println("taggedFiles=");
		for(int i=0; i<taggedFiles.length; ++i){
			System.out.println("taggedFiles[" + i + "].length=" + taggedFiles[i].length);
			for(int j=0; j<taggedFiles[i].length; ++j){
				System.out.println(taggedFiles[i][j] + "\t" + taggedFiles[i][j].length());
			}
		}
		System.out.println("sortOutDirName=" + sortOutDirName);
		System.out.println("offsetOutDirName=" + offsetOutDirName);
		System.out.println("sortBy=" + sortBy);
		System.out.println("secondary_soryBy=" + secondary_sortBy);
		System.out.println("threadPoolSize=" + threadPoolSize);
		
		final File[] sortOutDirs = new File[taggedFiles.length];
		final File[] offsetOutDirs = new File[taggedFiles.length];
		final File[][] offsetFiles = new File[taggedFiles.length][];
		for(int i=0; i<taggedFiles.length; ++i){
			File p_f = taggedFiles[i][0].getParentFile().getParentFile();
			sortOutDirs[i] = Dir.mkDirsAndClean(new File(p_f, sortOutDirName));// 跟taggedFiles使用相同的上级目录
			offsetOutDirs[i] = Dir.mkDirsAndClean(new File(p_f, offsetOutDirName));
			offsetFiles[i] = new File[taggedFiles[i].length];
		}
		final TypeInference.Type key_type = typeInference.getType(sortBy);
		int buckets = 0;
		int max_row_len = 0;
		for(int i=0; i<taggedFiles.length; ++i){
			buckets += taggedFiles[i].length;
			max_row_len = Math.max(max_row_len, taggedFiles[i].length);
		}
		final Object[] disk_read_write_locks = DistinctDisk.newLocks();
		ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
		ExecutorCompletionService<Pair<Integer, Long>> completionService = // <ofst_file_id, distinct_key_count>
				new ExecutorCompletionService<Pair<Integer, Long>>(executor);
		final ConcurrentHashMap<Integer, File> sorttedFileNumber_filePath = new ConcurrentHashMap<Integer, File>();
		for(int j=0; j<max_row_len; ++j){// load balancing, sorting by collumn
			for(int i=0; i<taggedFiles.length; ++i){
				if(j >= taggedFiles[i].length){
					continue;
				}
				final int jj = j;
				final int ii = i;
				completionService.submit(new Callable<Pair<Integer, Long>>(){ 
					@Override
					public Pair<Integer, Long> call() throws Exception{
						try{
						FileInputStream in_stream = new FileInputStream(taggedFiles[ii][jj]);
						Object lock = DistinctDisk.getLockOfFile(disk_read_write_locks, taggedFiles[ii][jj]);
						MyDecoder decoder = new MyDecoder(in_stream, 1024*1024*60, lock);
						ArrayList<Trinal<Object, Object, byte[]>> result = new ArrayList<Trinal<Object, Object, byte[]>>(); 
						int line_size;
						while(!decoder.isEnd()){// every line
							line_size=decoder.readInt();
							int bg_pos = decoder.readFixed(line_size);
							MyDecoder line_decoder = new MyDecoder(decoder.buf, bg_pos, line_size);
							Object v_value = null;
							Object sortby_value = null;
							Object secondary_sortby_value = null;
							while(!line_decoder.isEnd()){// every pair in the line
								int key_number = line_decoder.readInt();
								String key_name;
								if(key_number == KeysNumber.NOT_A_KEYNUMBER){
									key_name = line_decoder.readString();
								}else{
									key_name = keyNumber.getKey(key_number);
								}
								switch(typeInference.getType(key_name)){
								case BOOLEAN:
									v_value = line_decoder.readBoolean();break;
								case LONG:
									v_value = line_decoder.readLong();break;
								case DOUBLE:
									v_value = line_decoder.readDouble();break;
								case STR_UUID:
									if(key_name.equals(sortBy) || key_name.equals(secondary_sortBy)){
										String uuid_prefix = line_decoder.readString();
										v_value = MyUUID.byte16ToUUID_WithPrefix(uuid_prefix, 
												line_decoder.buf, line_decoder.readFixed(16));
									}else{
										line_decoder.skipString();
										line_decoder.skipFixed(16);
									}
									break;
								case STR_HALFUUID:
									if(key_name.equals(sortBy) || key_name.equals(secondary_sortBy)){
										String uuid_prefix = line_decoder.readString();
										v_value = MyUUID.byte8ToHALFUUID_WithPrefix(uuid_prefix, 
												line_decoder.buf, line_decoder.readFixed(8));
									}else{
										line_decoder.skipString();
										line_decoder.skipFixed(8);
									}
									break;
								case UUID:
									if(key_name.equals(sortBy) || key_name.equals(secondary_sortBy)){
										v_value = MyUUID.byte16ToUUID(line_decoder.buf, line_decoder.readFixed(16));
									}else{
										line_decoder.skipFixed(16);
									}
									break;
								default:// STRING
									if(key_name.equals(sortBy) || key_name.equals(secondary_sortBy)){
										v_value = line_decoder.readString();
									}else{
										line_decoder.skipString();
									}
								}
								if(key_name.equals(sortBy)){
									sortby_value = v_value;
								}else if(key_name.equals(secondary_sortBy)){
									secondary_sortby_value = v_value;
								}
								if(sortby_value!=null && secondary_sortby_value!=null){
									break;
								}
							}// while every pair
							result.add(new Trinal<Object, Object, byte[]>(sortby_value, 
									secondary_sortby_value, Arrays.copyOfRange(decoder.buf, bg_pos, bg_pos+line_size)));
						}// while every line
						Collections.sort(result, new Comparator<Trinal<Object, Object, byte[]>>(){
							@Override
							public int compare(Trinal<Object, Object, byte[]> a, Trinal<Object, Object, byte[]> b){
								int t = ((Comparable)a.first).compareTo(b.first);
								if(t != 0){
									return t;
								}
								return ((Comparable)a.second).compareTo(b.second);
							}
						});
						int file_number = Integer.parseInt(taggedFiles[ii][jj].getName().split("\\.")[0]);
						File cur_sorttedFile = new File(sortOutDirs[ii], file_number+".st");
						System.out.println("result.size()=" + result.size());
						System.out.println("cur_sortted_out_file: " + cur_sorttedFile);
						FileOutputStream fileOutputStream_sort = new FileOutputStream(cur_sorttedFile);
						sorttedFileNumber_filePath.put(file_number, cur_sorttedFile);
						offsetFiles[ii][jj] = new File(offsetOutDirs[ii], file_number+".ofst");
						FileOutputStream fileOutputStream_offset = new FileOutputStream(offsetFiles[ii][jj]);
						BufferedBinaryEncoder encoder_sort = new BufferedBinaryEncoder(fileOutputStream_sort, 1024*1024);
						BufferedBinaryEncoder encoder_offset = new BufferedBinaryEncoder(fileOutputStream_offset, 1024*1024);
						long cur_offset = 0;
						Object last_key = null;
						long distinct_key_count = 0;
						synchronized(DistinctDisk.getLockOfFile(disk_read_write_locks, cur_sorttedFile)){
						for(Trinal<Object, Object, byte[]> p : result){
							if(!p.first.equals(last_key)){
								last_key = p.first;
								distinct_key_count++;
								switch(key_type){
								case BOOLEAN:
									encoder_offset.writeBoolean((Boolean)p.first);break;
								case LONG:
									encoder_offset.writeLong((Long)p.first);break;
								case DOUBLE:
									encoder_offset.writeDouble((Double)p.first); break;
								case STR_UUID:
									String str_uuid = (String)p.first;
									int idx = str_uuid.indexOf('_');
									encoder_offset.writeString(str_uuid.substring(0, idx));
									encoder_offset.writeFixed(MyUUID.UUIDToByte16(str_uuid.substring(idx+1)));
									break;
								case STR_HALFUUID:
									String str_halfuuid = (String)p.first;
									int idx2 = str_halfuuid.indexOf('-');
									encoder_offset.writeString(str_halfuuid.substring(0, idx2));
									encoder_offset.writeFixed(MyUUID.HALFUUIDToByte8(str_halfuuid.substring(idx2+1)));
									break;
								case UUID:
									encoder_offset.writeFixed(MyUUID.UUIDToByte16((String)p.first));
									break;
								default:
									encoder_offset.writeString((String)p.first);
								}
								encoder_offset.writeLong(cur_offset);
							}
							byte[] b_arr = (byte[])p.third;
							cur_offset += encoder_sort.writeIntWithReturn(b_arr.length);
							encoder_sort.writeFixed(b_arr);
							cur_offset += b_arr.length;
						}
//						System.out.println("cur_offset=" + cur_offset + ", distinct_key_count=" + distinct_key_count);
						encoder_sort.flush();
						encoder_offset.flush();
						fileOutputStream_sort.close();
						fileOutputStream_offset.close();
//						System.out.println("sort_result.size()=" + result.size());
						return new Pair<Integer, Long>(
								Integer.parseInt(taggedFiles[ii][jj].getName().split("\\.")[0]), distinct_key_count);
						}// synchronized
						}catch(Exception e){
							e.printStackTrace();
							System.exit(-1);
						}
						return null;
					}// call()
				});// Callable
			}
		}
		BufferedWriter writer_0_sz = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(new File(offsetOutDirs[0], "0.size"))), 1024*64);
		System.out.println(offsetOutDirs[0] + "/0.size:");
		for(int i=0; i<buckets; ++i){// wait completion
			Pair<Integer, Long> fileNumber_lines =  completionService.take().get();
			writer_0_sz.append(String.valueOf(fileNumber_lines.key))
					.append('\t')
					.append(String.valueOf(fileNumber_lines.value));
			writer_0_sz.newLine();
			System.out.println(fileNumber_lines);
		}
		writer_0_sz.close();
		executor.shutdown();
		return new Pair<Map<Integer, File>, File[][]>(sorttedFileNumber_filePath, offsetFiles);
	}

	
}
