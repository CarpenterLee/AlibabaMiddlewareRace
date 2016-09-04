package hoolee.batch;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import avro.io.BufferedBinaryEncoder;
import avro.io.MyDecoder;
import hoolee.io.Dir;
import hoolee.io.MyByteArrayOutputStream;
import hoolee.io.MyFileInputStream;
import hoolee.sort.KeysNumber;
import hoolee.sort.MyUUID;
import hoolee.sort.TypeInference;
import hoolee.util.DistinctDisk;
import hoolee.util.Pair;

public class Tagging_beforeMyStringSpliter {
	/**
	 * 对secondary offset文件做tagging
	 * tagging之后的文件均分到每个tagOutDirs指定的目录中
	 * @param inFiles 待tagging文件列表，下标顺序就是处理顺序。如果在不同盘上，需要下标尽量均匀。
	 * @param tags
	 * @param tagOutDirs tagging之后文件的存放目录，要保证不在同一块盘上。
	 * @param tagBy
	 * @param keyNumber
	 * @param typeInference
	 * @param threadPoolSize
	 * @return tagging之后的文件列表，下标第一维相同的文件都在同一个目录下。
	 */
	public File[][] tagSecondaryOffsetFiles(final File[] secondaryOffsetFiles, 
			final Comparable<Object>[] tags, final File[] tagOutDirs, 
			final TypeInference.Type keyType, 
			int threadPoolSize) throws Exception{
		System.out.println("####method tagSecondaryOffsetFiles()");
		System.out.println("secondaryOffsetFiles=" + Arrays.toString(secondaryOffsetFiles));
		if(tags.length > 0){
			System.out.println("tags[0] class=" + tags[0].getClass());
		}
		System.out.println("tags=" + Arrays.toString(tags));
		System.out.println("tagOutDirs=" + Arrays.toString(tagOutDirs));
		System.out.println("keyType=" + keyType);
		System.out.println("threadPoolSize=" + threadPoolSize);
		if(keyType != TypeInference.Type.LONG){
			throw new RuntimeException("only LONG keyType is allowed!!");
		}
		for(File dir : tagOutDirs){
			Dir.mkDirsAndClean(dir);
		}
		int distinct_dirs_count = tagOutDirs.length;
		int buckets = tags.length+1;
		System.out.println("buckets=" + buckets);
		int buckets_per_dir = buckets/distinct_dirs_count;
		int left_buckets = buckets%distinct_dirs_count;
		int resultFiles_length = Math.min(distinct_dirs_count, buckets);// buckets maybe smaller than distinct_dirs_count
		System.out.println("resultFiles_length=" + resultFiles_length);
		File[][] resultFiles = new File[resultFiles_length][];
		int cur_count = 0;
		final BufferedOutputStream outputStreams[] = new BufferedOutputStream[buckets];
		final HashMap<BufferedOutputStream, File> outputStream_file_map = 
				new HashMap<BufferedOutputStream, File>();// for write lock
		for(int d=0; d<resultFiles.length; ++d){
			int bg = cur_count;// inclusive
			if(d<left_buckets){
				cur_count += buckets_per_dir+1;
			}else{
				cur_count += buckets_per_dir;
			}
			int ed = cur_count;// exclusive
			resultFiles[d] = new File[ed-bg];
			for(int i=bg; i<ed; ++i){
				File f = new File(tagOutDirs[d], i+".tg");
				System.out.println("tagged out file: " + f);
				resultFiles[d][i-bg] = f;
				outputStreams[i] = new BufferedOutputStream(new FileOutputStream(f), 1024*1024);// output File on disk
				outputStream_file_map.put(outputStreams[i], f);
			}
		}
		final Object[] disk_read_locks = DistinctDisk.newLocks();
		final Object[] disk_write_locks = DistinctDisk.newLocks();
		ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
		CompletionService<Integer> completionService = 
				new ExecutorCompletionService<Integer>(executor);
		for(final File inFile : secondaryOffsetFiles){// for every file
			completionService.submit(new Callable<Integer>(){
				@Override
				public Integer call() throws Exception{
					try{
					FileInputStream in_stream = new FileInputStream(inFile);
					Object lock = DistinctDisk.getLockOfFile(disk_read_locks, inFile);
					MyDecoder decoder = new MyDecoder(in_stream, 1024*1024*40, lock);// decoder for this task
					MyByteArrayOutputStream buf_outputStreams[] = new MyByteArrayOutputStream[tags.length+1];
					BufferedBinaryEncoder encoders[] = new BufferedBinaryEncoder[tags.length+1];
					for(int i=0; i<encoders.length; ++i){
						buf_outputStreams[i] = new MyByteArrayOutputStream();
						encoders[i] = new BufferedBinaryEncoder(buf_outputStreams[i], 1024*16);// encoder for each bucket
					}
					long lines_read = 0;
					while(!decoder.isEnd()){// every <key, offset>
						lines_read++;
						Object key1 = decoder.readLong();
						int tg = getTag(tags, (Comparable<Object>)key1);// tagging
						if(tg == -1){
							throw new RuntimeException("fail to tagging line, key: " + key1);
						}
						int file_number1 = decoder.readInt();
						long offset1 = decoder.readLong();
						encoders[tg].writeLong((Long)key1);
						encoders[tg].writeInt(file_number1);
						encoders[tg].writeLong(offset1);
						encoders[tg].flush();
						if(buf_outputStreams[tg].size() > 1024*64){
//							synchronized(outputStreams[tg]){
							synchronized(DistinctDisk.getLockOfFile(disk_write_locks, 
									outputStream_file_map.get(outputStreams[tg]))){
								outputStreams[tg].write(buf_outputStreams[tg].getByteBuf(), 0, buf_outputStreams[tg].size());
							}
							buf_outputStreams[tg].reset();
						}
					}// while !decoder.isEnd()
					System.out.println("lines_read=" + lines_read);
					for(int i=0; i<outputStreams.length; ++i){
						encoders[i].flush();
						if(buf_outputStreams[i].size() > 0){
//							synchronized(outputStreams[i]){
							synchronized(DistinctDisk.getLockOfFile(disk_write_locks, 
									outputStream_file_map.get(outputStreams[i]))){
								outputStreams[i].write(buf_outputStreams[i].getByteBuf(), 0, buf_outputStreams[i].size());
							}
						}
					}
					}catch(Exception e){
						e.printStackTrace();
						System.exit(-1);
					}
					System.out.println(inFile + " has tagged");
					return 0;
				}// call()
			});
		}// for every file
		for(int i=0; i<secondaryOffsetFiles.length; ++i){// wait every task completion
			completionService.take();
		}
		executor.shutdownNow();
		for(int i=0; i<buckets; ++i){
			outputStreams[i].close();
		}
		return resultFiles;
	}
	//read once, tag twice
	public Pair<File[][], File[][]> tagOrdinaryFiles_DualTag(final File[] inFiles, 
			final Comparable<Object>[] tags_a, final Comparable<Object>[] tags_b,
			final File[] tagOutDirs_a, final File[] tagOutDirs_b, 
			final String tagBy_a, final String tagBy_b, 
			final KeysNumber keyNumber, 
			final TypeInference typeInference, int threadPoolSize) throws Exception{
		System.out.println("####method tagOrdinaryFiles_DualTag()");
		System.out.println("inFiles=" + Arrays.toString(inFiles));
		System.out.println("tags_a=" + Arrays.toString(tags_a));
		System.out.println("tagOutDirs_a=" + Arrays.toString(tagOutDirs_a));
		System.out.println("tagBy_a=" + tagBy_a);
		System.out.println("tags_b=" + Arrays.toString(tags_b));
		System.out.println("tagOutDirs_b=" + Arrays.toString(tagOutDirs_b));
		System.out.println("tagBy_b=" + tagBy_b);
		System.out.println("threadPoolSize=" + threadPoolSize);
		
		for(File dir : tagOutDirs_a){
			Dir.mkDirsAndClean(dir);
		}
		for(File dir : tagOutDirs_b){
			Dir.mkDirsAndClean(dir);
		}
		int distinct_dirs_count_a = tagOutDirs_a.length;
		int buckets_a = tags_a.length+1;
		System.out.println("buckets_a=" + buckets_a);
		int buckets_per_dir_a = buckets_a/distinct_dirs_count_a;
		int left_buckets_a = buckets_a%distinct_dirs_count_a;
		int resultFiles_length_a = Math.min(distinct_dirs_count_a, buckets_a);// buckets maybe smaller than distinct_dirs_count
		File[][] resultFiles_a = new File[resultFiles_length_a][];
		int cur_count_a = 0;
		final BufferedOutputStream outputStreams_a[] = new BufferedOutputStream[buckets_a];
		final HashMap<BufferedOutputStream, File> outputStream_file_map_a = 
				new HashMap<BufferedOutputStream, File>();// for write lock
		for(int d=0; d<resultFiles_a.length; ++d){
			int bg = cur_count_a;// inclusive
			if(d<left_buckets_a){
				cur_count_a += buckets_per_dir_a+1;
			}else{
				cur_count_a += buckets_per_dir_a;
			}
			int ed = cur_count_a;// exclusive
			resultFiles_a[d] = new File[ed-bg];
			for(int i=bg; i<ed; ++i){
				File f = new File(tagOutDirs_a[d], i+".tg");
				System.out.println("tagged out file: " + f);
				resultFiles_a[d][i-bg] = f;
				outputStreams_a[i] = new BufferedOutputStream(new FileOutputStream(f), 1024*512);// output file on disk
				outputStream_file_map_a.put(outputStreams_a[i], f);
			}
		}
		System.out.println();
		int distinct_dirs_count_b = tagOutDirs_b.length;
		int buckets_b = tags_b.length+1;
		System.out.println("buckets_b=" + buckets_b);
		int buckets_per_dir_b = buckets_b/distinct_dirs_count_b;
		int left_buckets_b = buckets_b%distinct_dirs_count_b;
		int resultFiles_length_b = Math.min(distinct_dirs_count_b, buckets_b);// buckets maybe smaller than distinct_dirs_count
		File[][] resultFiles_b = new File[resultFiles_length_b][];
		int cur_count_b = 0;
		final BufferedOutputStream outputStreams_b[] = new BufferedOutputStream[buckets_b];
		final HashMap<BufferedOutputStream, File> outputStream_file_map_b = 
				new HashMap<BufferedOutputStream, File>();// for write lock
		for(int d=0; d<resultFiles_b.length; ++d){
			int bg = cur_count_b;// inclusive
			if(d<left_buckets_b){
				cur_count_b += buckets_per_dir_b+1;
			}else{
				cur_count_b += buckets_per_dir_b;
			}
			int ed = cur_count_b;// exclusive
			resultFiles_b[d] = new File[ed-bg];
			for(int i=bg; i<ed; ++i){
				File f = new File(tagOutDirs_b[d], i+".tg");
				System.out.println("tagged out file: " + f);
				resultFiles_b[d][i-bg] = f;
				outputStreams_b[i] = new BufferedOutputStream(new FileOutputStream(f), 1024*512);// output file on disk
				outputStream_file_map_b.put(outputStreams_b[i], f);
			}
		}
		
		final Object[] disk_read_locks = DistinctDisk.newLocks();
		final Object[] disk_write_locks = DistinctDisk.newLocks();
//		final Object[] disk_write_locks_b = DistinctDisk.newLocks();
		ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
		CompletionService<Integer> completionService = 
				new ExecutorCompletionService<Integer>(executor);
		for(final File inFile : inFiles){// for every file
			completionService.submit(new Callable<Integer>(){
				@Override
				public Integer call() throws Exception{
					try{
					Object lock = DistinctDisk.getLockOfFile(disk_read_locks, inFile);
					BufferedReader reader = new BufferedReader(
							new InputStreamReader(new MyFileInputStream(inFile, lock)), 1024*1024*40);// reader for this task
					
					MyByteArrayOutputStream br_outputStreams_a[] = new MyByteArrayOutputStream[tags_a.length+1];
					BufferedBinaryEncoder encoders_a[] = new BufferedBinaryEncoder[tags_a.length+1];
					for(int i=0; i<encoders_a.length; ++i){
						br_outputStreams_a[i] = new MyByteArrayOutputStream();
						encoders_a[i] = new BufferedBinaryEncoder(br_outputStreams_a[i], 1024);// encoder for each bucket A
					}
					MyByteArrayOutputStream br_outputStreams_b[] = new MyByteArrayOutputStream[tags_b.length+1];
					BufferedBinaryEncoder encoders_b[] = new BufferedBinaryEncoder[tags_b.length+1];
					for(int i=0; i<encoders_b.length; ++i){
						br_outputStreams_b[i] = new MyByteArrayOutputStream();
						encoders_b[i] = new BufferedBinaryEncoder(br_outputStreams_b[i], 1024);// encoder for each bucket B
					}
					
					MyByteArrayOutputStream line_buf_stream = new MyByteArrayOutputStream();
					BufferedBinaryEncoder line_encoder = new BufferedBinaryEncoder(line_buf_stream, 1024);
					
					String line;
					String sortBy_a_colon = tagBy_a+":";
					String sortBy_b_colon = tagBy_b+":";
					while((line=reader.readLine()) != null){// every line
						String[] line_pairs = line.split("\t");
						int tg_a = -1;
						int tg_b = -1;
						for(String pairStr : line_pairs){
							if(pairStr.startsWith(sortBy_a_colon)){
								Object value_obj = null;
								int idx = pairStr.indexOf(':');
								String value_str = pairStr.substring(idx+1);
								switch(typeInference.getType(tagBy_a)){
								case LONG:
									value_obj = Long.parseLong(value_str);
									break;
								case DOUBLE:
									value_obj = Double.parseDouble(value_str);
									break;
								default:
									value_obj = value_str;
								}
								tg_a = getTag(tags_a, (Comparable<Object>)value_obj);// tagging A
							}else if(pairStr.startsWith(sortBy_b_colon)){
								Object value_obj = null;
								int idx = pairStr.indexOf(':');
								String value_str = pairStr.substring(idx+1);
								switch(typeInference.getType(tagBy_b)){
								case LONG:
									value_obj = Long.parseLong(value_str);
									break;
								case DOUBLE:
									value_obj = Double.parseDouble(value_str);
									break;
								default:
									value_obj = value_str;
								}
								tg_b = getTag(tags_b, (Comparable<Object>)value_obj);// tagging B
							}
							
						}
						if(tg_a==-1 || tg_b==-1){
							throw new RuntimeException("fail to tagging line: " + line);
						}
						// encoder the line
						for(String pair : line_pairs){// all pairs in the line
							String[] kv = pair.split(":");
							String k = kv[0];
							String v = kv[1];
							int key_number = keyNumber.getNumber(k);
							if(key_number == KeysNumber.NOT_A_KEYNUMBER){//write key
								line_encoder.writeInt(KeysNumber.NOT_A_KEYNUMBER);
								line_encoder.writeString(k);
							}else{
								line_encoder.writeInt(key_number);
							}
							switch(typeInference.getType(k)){// write value
							case BOOLEAN:
								line_encoder.writeBoolean(v.equals("true"));break;
							case LONG:
								line_encoder.writeLong(Long.parseLong(v));break;
							case DOUBLE:
								line_encoder.writeDouble(Double.parseDouble(v));break;
							case STR_UUID:
								int idx = v.indexOf('_');
								line_encoder.writeString(v.substring(0, idx));
								line_encoder.writeFixed(MyUUID.UUIDToByte16(v.substring(idx+1)));
								break;
							case STR_HALFUUID:
								int idx2 = v.indexOf('-');
								line_encoder.writeString(v.substring(0, idx2));
								line_encoder.writeFixed(MyUUID.HALFUUIDToByte8(v.substring(idx2+1)));
								break;
							case UUID:
								line_encoder.writeFixed(MyUUID.UUIDToByte16(v));break;
							default:// STRING
								line_encoder.writeString(v);
							}
						}//for
						line_encoder.flush();
						
						encoders_a[tg_a].writeInt(line_buf_stream.size());//write this line to encoders
						encoders_a[tg_a].writeFixed(line_buf_stream.getByteBuf(), 0, line_buf_stream.size());
						encoders_a[tg_a].flush();//一定要刷新
//						line_buf_stream.reset();
						encoders_b[tg_b].writeInt(line_buf_stream.size());//write this line to encoders
						encoders_b[tg_b].writeFixed(line_buf_stream.getByteBuf(), 0, line_buf_stream.size());
						encoders_b[tg_b].flush();//一定要刷新
						line_buf_stream.reset();
						
						if(br_outputStreams_a[tg_a].size() > 1024*64){
//							synchronized(outputStreams_a[tg_a]){
							synchronized(DistinctDisk.getLockOfFile(disk_write_locks, 
									outputStream_file_map_a.get(outputStreams_a[tg_a]))){
								outputStreams_a[tg_a].write(br_outputStreams_a[tg_a].getByteBuf(), 
										0, br_outputStreams_a[tg_a].size());
							}
							br_outputStreams_a[tg_a].reset();
						}
						if(br_outputStreams_b[tg_b].size() > 1024*64){
//							synchronized(outputStreams_b[tg_b]){
							synchronized(DistinctDisk.getLockOfFile(disk_write_locks, 
									outputStream_file_map_b.get(outputStreams_b[tg_b]))){
								outputStreams_b[tg_b].write(br_outputStreams_b[tg_b].getByteBuf(), 
										0, br_outputStreams_b[tg_b].size());
							}
							br_outputStreams_b[tg_b].reset();
						}
						
					}// while every line
					for(int i=0; i<outputStreams_a.length; ++i){
						encoders_a[i].flush();
						if(br_outputStreams_a[i].size() > 0){
//							synchronized(outputStreams_a[i]){
							synchronized(DistinctDisk.getLockOfFile(disk_write_locks, 
									outputStream_file_map_a.get(outputStreams_a[i]))){
								outputStreams_a[i].write(br_outputStreams_a[i].getByteBuf(), 0, br_outputStreams_a[i].size());
							}
						}
					}
					for(int i=0; i<outputStreams_b.length; ++i){
						encoders_b[i].flush();
						if(br_outputStreams_b[i].size() > 0){
//							synchronized(outputStreams_b[i]){
							synchronized(DistinctDisk.getLockOfFile(disk_write_locks, 
									outputStream_file_map_b.get(outputStreams_b[i]))){
								outputStreams_b[i].write(br_outputStreams_b[i].getByteBuf(), 0, br_outputStreams_b[i].size());
							}
						}
					}
					
					reader.close();
					}catch(Exception e){
						e.printStackTrace();
						System.exit(-1);
					}
					System.out.println(inFile + " has tagged");
					return 0;
				}// call()
			});
		}// for every file
		for(int i=0; i<inFiles.length; ++i){// wait every task completion
			completionService.take();
		}
		executor.shutdownNow();
		for(int i=0; i<buckets_a; ++i){
			outputStreams_a[i].close();
		}
		for(int i=0; i<buckets_b; ++i){
			outputStreams_b[i].close();
		}
		return new Pair<File[][], File[][]>(resultFiles_a, resultFiles_b);
//		return null;
	}
	/**
	 * 对原始数据文件做tagging
	 * tagging之后的文件均分到每个tagOutDirs指定的目录中
	 * @param inFiles 待tagging文件列表，下标顺序就是处理顺序。如果在不同盘上，需要下标尽量均匀。
	 * @param tags
	 * @param tagOutDirs tagging之后文件的存放目录，要保证不在同一块盘上。
	 * @param tagBy
	 * @param keyNumber
	 * @param typeInference
	 * @param threadPoolSize
	 * @return tagging之后的文件列表，下标第一维相同的文件都在同一个目录下。
	 */
	public File[][] tagOrdinaryFiles(final File[] inFiles, 
			final Comparable<Object>[] tags, final File[] tagOutDirs, 
			final String tagBy, final KeysNumber keyNumber, 
			final TypeInference typeInference, int threadPoolSize) throws Exception{
		System.out.println("####method tagOrdinaryFiles()");
		System.out.println("inFiles=" + Arrays.toString(inFiles));
		if(tags.length > 0){
			System.out.println("tags[0] class=" + tags[0].getClass());
		}
		System.out.println("tags=" + Arrays.toString(tags));
		System.out.println("tagOutDirs=" + Arrays.toString(tagOutDirs));
		System.out.println("tagBy=" + tagBy);
		System.out.println("threadPoolSize=" + threadPoolSize);
		
		for(File dir : tagOutDirs){
			Dir.mkDirsAndClean(dir);
		}
		int distinct_dirs_count = tagOutDirs.length;
		int buckets = tags.length+1;
		System.out.println("buckets=" + buckets);
		int buckets_per_dir = buckets/distinct_dirs_count;
		int left_buckets = buckets%distinct_dirs_count;
		int resultFiles_length = Math.min(distinct_dirs_count, buckets);// buckets maybe smaller than distinct_dirs_count
		File[][] resultFiles = new File[resultFiles_length][];
		int cur_count = 0;
		final BufferedOutputStream outputStreams[] = new BufferedOutputStream[buckets];
		final HashMap<BufferedOutputStream, File> outputStream_file_map = 
				new HashMap<BufferedOutputStream, File>();// for write lock
		for(int d=0; d<resultFiles.length; ++d){
			int bg = cur_count;// inclusive
			if(d<left_buckets){
				cur_count += buckets_per_dir+1;
			}else{
				cur_count += buckets_per_dir;
			}
			int ed = cur_count;// exclusive
			resultFiles[d] = new File[ed-bg];
			for(int i=bg; i<ed; ++i){
				File f = new File(tagOutDirs[d], i+".tg");
				System.out.println("tagged out file: " + f);
				resultFiles[d][i-bg] = f;
				outputStreams[i] = new BufferedOutputStream(new FileOutputStream(f), 1024*1024);// output file on disk
				outputStream_file_map.put(outputStreams[i], f);
			}
		}
		final Object[] disk_read_locks = DistinctDisk.newLocks();
		final Object[] disk_write_locks = DistinctDisk.newLocks();
		ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
		CompletionService<Integer> completionService = 
				new ExecutorCompletionService<Integer>(executor);
		for(final File inFile : inFiles){// for every file
			completionService.submit(new Callable<Integer>(){
				@Override
				public Integer call() throws Exception{
					try{
					Object lock = DistinctDisk.getLockOfFile(disk_read_locks, inFile);
					BufferedReader reader = new BufferedReader(
							new InputStreamReader(new MyFileInputStream(inFile, lock)), 1024*1024*40);// reader for this task
					MyByteArrayOutputStream br_outputStreams[] = new MyByteArrayOutputStream[tags.length+1];
					BufferedBinaryEncoder encoders[] = new BufferedBinaryEncoder[tags.length+1];
					for(int i=0; i<encoders.length; ++i){
						br_outputStreams[i] = new MyByteArrayOutputStream();
						encoders[i] = new BufferedBinaryEncoder(br_outputStreams[i], 1024);// encoder for each bucket
					}
					MyByteArrayOutputStream line_buf_stream = new MyByteArrayOutputStream();
					BufferedBinaryEncoder line_encoder = new BufferedBinaryEncoder(line_buf_stream, 1024);
					
					String line;
					String sortBy_colon = tagBy+":";
					while((line=reader.readLine()) != null){// every line
						String[] line_pairs = line.split("\t");
						int tg = -1;
						for(String pairStr : line_pairs){
							if(pairStr.startsWith(sortBy_colon)){
								Object value_obj = null;
								int idx = pairStr.indexOf(':');
								String value_str = pairStr.substring(idx+1);
								switch(typeInference.getType(tagBy)){
								case LONG:
									value_obj = Long.parseLong(value_str);
									break;
								case DOUBLE:
									value_obj = Double.parseDouble(value_str);
									break;
								default:
									value_obj = value_str;
								}
								tg = getTag(tags, (Comparable<Object>)value_obj);// tagging
							}
						}
						if(tg == -1){
							throw new RuntimeException("fail to tagging line: " + line);
						}
						for(String pair : line_pairs){// all pairs in the line
							String[] kv = pair.split(":");
							String k = kv[0];
							String v = kv[1];
							int key_number = keyNumber.getNumber(k);
							if(key_number == KeysNumber.NOT_A_KEYNUMBER){//write key
								line_encoder.writeInt(KeysNumber.NOT_A_KEYNUMBER);
								line_encoder.writeString(k);
							}else{
								line_encoder.writeInt(key_number);
							}
							switch(typeInference.getType(k)){// write value
							case BOOLEAN:
								line_encoder.writeBoolean(v.equals("true"));break;
							case LONG:
								line_encoder.writeLong(Long.parseLong(v));break;
							case DOUBLE:
								line_encoder.writeDouble(Double.parseDouble(v));break;
							case STR_UUID:
								int idx = v.indexOf('_');
								line_encoder.writeString(v.substring(0, idx));
								line_encoder.writeFixed(MyUUID.UUIDToByte16(v.substring(idx+1)));
								break;
							case STR_HALFUUID:
								int idx2 = v.indexOf('-');
								line_encoder.writeString(v.substring(0, idx2));
								line_encoder.writeFixed(MyUUID.HALFUUIDToByte8(v.substring(idx2+1)));
								break;
							case UUID:
								line_encoder.writeFixed(MyUUID.UUIDToByte16(v));break;
							default:// STRING
								line_encoder.writeString(v);
							}
						}//for
						line_encoder.flush();
						
						encoders[tg].writeInt(line_buf_stream.size());//write this line to encoders
						encoders[tg].writeFixed(line_buf_stream.getByteBuf(), 0, line_buf_stream.size());
						encoders[tg].flush();//一定要刷新
						line_buf_stream.reset();
						
						if(br_outputStreams[tg].size() > 1024*16){
//							synchronized(outputStreams[tg]){
							synchronized(DistinctDisk.getLockOfFile(disk_write_locks, 
									outputStream_file_map.get(outputStreams[tg]))){
								outputStreams[tg].write(br_outputStreams[tg].getByteBuf(), 0, br_outputStreams[tg].size());
							}
							br_outputStreams[tg].reset();
						}
					}// while every line
					for(int i=0; i<outputStreams.length; ++i){
						encoders[i].flush();
						if(br_outputStreams[i].size() > 0){
//							synchronized(outputStreams[i]){
							synchronized(DistinctDisk.getLockOfFile(disk_write_locks, 
									outputStream_file_map.get(outputStreams[i]))){
								outputStreams[i].write(br_outputStreams[i].getByteBuf(), 0, br_outputStreams[i].size());
							}
						}
					}
					reader.close();
					}catch(Exception e){
						e.printStackTrace();
						System.exit(-1);
					}
					System.out.println(inFile + " has tagged");
					return 0;
				}// call()
			});
		}// for every file
		for(int i=0; i<inFiles.length; ++i){// wait every task completion
			completionService.take();
		}
		executor.shutdownNow();
		for(int i=0; i<buckets; ++i){
			outputStreams[i].close();
		}
		return resultFiles;
	}
//	/**对行编码的时候将某些字段放在前面*/
//	public File[][] tagOrdinaryFiles_SequenceKey(final File[] inFiles, 
//			final Comparable<Object>[] tags, final File[] tagOutDirs, 
//			final String tagBy, final KeysNumber keyNumber, 
//			final TypeInference typeInference, int threadPoolSize) throws Exception{
//		System.out.println("####method tagOrdinaryFiles_SequenceKey()");
//		System.out.println("inFiles=" + Arrays.toString(inFiles));
//		if(tags.length > 0){
//			System.out.println("tags[0] class=" + tags[0].getClass());
//		}
//		System.out.println("tags=" + Arrays.toString(tags));
//		System.out.println("tagOutDirs=" + Arrays.toString(tagOutDirs));
//		System.out.println("tagBy=" + tagBy);
//		System.out.println("threadPoolSize=" + threadPoolSize);
//		
//		for(File dir : tagOutDirs){
//			Dir.mkDirsAndClean(dir);
//		}
//		int distinct_dirs_count = tagOutDirs.length;
//		int buckets = tags.length+1;
//		System.out.println("buckets=" + buckets);
//		int buckets_per_dir = buckets/distinct_dirs_count;
//		int left_buckets = buckets%distinct_dirs_count;
//		int resultFiles_length = Math.min(distinct_dirs_count, buckets);// buckets maybe smaller than distinct_dirs_count
//		File[][] resultFiles = new File[resultFiles_length][];
//		int cur_count = 0;
//		final BufferedOutputStream outputStreams[] = new BufferedOutputStream[buckets];
//		for(int d=0; d<resultFiles.length; ++d){
//			int bg = cur_count;// inclusive
//			if(d<left_buckets){
//				cur_count += buckets_per_dir+1;
//			}else{
//				cur_count += buckets_per_dir;
//			}
//			int ed = cur_count;// exclusive
//			resultFiles[d] = new File[ed-bg];
//			for(int i=bg; i<ed; ++i){
//				File f = new File(tagOutDirs[d], i+".tg");
//				System.out.println("tagged out file: " + f);
//				resultFiles[d][i-bg] = f;
//				outputStreams[i] = new BufferedOutputStream(new FileOutputStream(f), 1024*512);// output file on disk
//			}
//		}
//		final Object[] disk_read_locks = DistinctDisk.newLocks();
//		ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
//		CompletionService<Integer> completionService = 
//				new ExecutorCompletionService<Integer>(executor);
//		for(final File inFile : inFiles){// for every file
//			completionService.submit(new Callable<Integer>(){
//				@Override
//				public Integer call() throws Exception{
//					try{
//					Object lock = DistinctDisk.getLockOfFile(disk_read_locks, inFile);
//					BufferedReader reader = new BufferedReader(
//							new InputStreamReader(new MyFileInputStream(inFile, lock)), 1024*1024*40);// reader for this task
//					MyByteArrayOutputStream br_outputStreams[] = new MyByteArrayOutputStream[tags.length+1];
//					BufferedBinaryEncoder encoders[] = new BufferedBinaryEncoder[tags.length+1];
//					for(int i=0; i<encoders.length; ++i){
//						br_outputStreams[i] = new MyByteArrayOutputStream();
//						encoders[i] = new BufferedBinaryEncoder(br_outputStreams[i], 1024);// encoder for each bucket
//					}
//					MyByteArrayOutputStream line_buf_stream = new MyByteArrayOutputStream();
//					BufferedBinaryEncoder line_encoder = new BufferedBinaryEncoder(line_buf_stream, 1024);
//					
//					String line;
//					String sortBy_colon = tagBy+":";
//					while((line=reader.readLine()) != null){// every line
//						String[] line_pairs = line.split("\t");
//						int tg = -1;
//						for(String pairStr : line_pairs){
//							if(pairStr.startsWith(sortBy_colon)){
//								Object value_obj = null;
//								int idx = pairStr.indexOf(':');
//								String value_str = pairStr.substring(idx+1);
//								switch(typeInference.getType(tagBy)){
//								case LONG:
//									value_obj = Long.parseLong(value_str);
//									break;
//								case DOUBLE:
//									value_obj = Double.parseDouble(value_str);
//									break;
//								default:
//									value_obj = value_str;
//								}
//								tg = getTag(tags, (Comparable<Object>)value_obj);// tagging
//							}
//						}
//						if(tg == -1){
//							throw new RuntimeException("fail to tagging line: " + line);
//						}
//						LinkedHashMap<String, String> keyValues_of_line = new LinkedHashMap<String, String>(); 
//						for(String pair : line_pairs){// all pairs in the line
//							int idx = pair.indexOf(':');
//							String k = pair.substring(0, idx);
//							String v = pair.substring(idx+1);
//							keyValues_of_line.put(k,v);
//						}//for
//						ArrayList<String> keys_list = new ArrayList<String>();
//						keys_list.add(tagBy);
//						if(keyValues_of_line.containsKey("createtime")){
//							keys_list.add("createtime");	
//						}
//						for(Map.Entry<String, String> entry : keyValues_of_line.entrySet()){
//							if(!keys_list.contains(entry.getKey())){
//								keys_list.add(entry.getKey());
//							}
//						}
//						for(String k : keys_list){
//							String v = keyValues_of_line.get(k);
//							int key_number = keyNumber.getNumber(k);
//							if(key_number == KeysNumber.NOT_A_KEYNUMBER){//write key
//								line_encoder.writeInt(KeysNumber.NOT_A_KEYNUMBER);
//								line_encoder.writeString(k);
//							}else{
//								line_encoder.writeInt(key_number);
//							}
//							switch(typeInference.getType(k)){// write value
//							case BOOLEAN:
//								line_encoder.writeBoolean(v.equals("true"));break;
//							case LONG:
//								line_encoder.writeLong(Long.parseLong(v));break;
//							case DOUBLE:
//								line_encoder.writeDouble(Double.parseDouble(v));break;
//							case STR_UUID:
//								int idx = v.indexOf('_');
//								line_encoder.writeString(v.substring(0, idx));
//								line_encoder.writeFixed(MyUUID.UUIDToByte16(v.substring(idx+1)));
//								break;
//							case STR_HALFUUID:
//								int idx2 = v.indexOf('-');
//								line_encoder.writeString(v.substring(0, idx2));
//								line_encoder.writeFixed(MyUUID.HALFUUIDToByte8(v.substring(idx2+1)));
//								break;
//							case UUID:
//								line_encoder.writeFixed(MyUUID.UUIDToByte16(v));break;
//							default:// STRING
//								line_encoder.writeString(v);
//							}
//						}
//						line_encoder.flush();
//						
//						encoders[tg].writeInt(line_buf_stream.size());//write this line to encoders
//						encoders[tg].writeFixed(line_buf_stream.getByteBuf(), 0, line_buf_stream.size());
//						encoders[tg].flush();//一定要刷新
//						line_buf_stream.reset();
//						
//						if(br_outputStreams[tg].size() > 1024*16){
//							synchronized(outputStreams[tg]){
//								outputStreams[tg].write(br_outputStreams[tg].getByteBuf(), 0, br_outputStreams[tg].size());
//							}
//							br_outputStreams[tg].reset();
//						}
//					}// while every line
//					for(int i=0; i<outputStreams.length; ++i){
//						encoders[i].flush();
//						if(br_outputStreams[i].size() > 0){
//							synchronized(outputStreams[i]){
//								outputStreams[i].write(br_outputStreams[i].getByteBuf(), 0, br_outputStreams[i].size());
//							}
//						}
//					}
//					reader.close();
//					}catch(Exception e){
//						e.printStackTrace();
//						System.exit(-1);
//					}
//					System.out.println(inFile + " has tagged");
//					return 0;
//				}// call()
//			});
//		}// for every file
//		for(int i=0; i<inFiles.length; ++i){// wait every task completion
//			completionService.take();
//		}
//		executor.shutdownNow();
//		for(int i=0; i<buckets; ++i){
//			outputStreams[i].close();
//		}
//		return resultFiles;
//	}
	
	
	private int getTag(Comparable<Object>[] tags, Comparable<Object> value){
		int i = 0;
		int j = tags.length-1;
		while(i <= j){
			int mid = (i+j)>>1;
			if(value.compareTo(tags[mid]) < 0){
				j = mid-1;
			}else{
				i = mid+1;
			}
		}
		return i;
	}
	/**
	 * 返回的tags是已经转换成相应类型的
	 * @param samples 采样数据，String。
	 * @param type 已经推断出的类型
	 * @param buckets 要分成的区间数
	 * @return 对应类型的tags
	 */
	public Comparable<Object>[] getTags(String[] samples, TypeInference.Type type, int buckets){
		System.out.println("--getTags()--");
		System.out.println("samples.length=" + samples.length);
		System.out.println("type=" + type);
		System.out.println("buckets=" + buckets);
		if(buckets > samples.length+1){
			throw new RuntimeException("buckets=" + buckets + 
					" is bigger than samples.size+1(" + (samples.length+1) + ")");
		}
		Object[] sample_objs = null;
		switch(type){
		case LONG:
			sample_objs = new Object[samples.length];
			for(int i=0; i<samples.length; ++i){
				sample_objs[i] = Long.parseLong(samples[i]);
			}
			break;
		case DOUBLE:
			sample_objs = new Object[samples.length];
			for(int i=0; i<samples.length; ++i){
				sample_objs[i] = Double.parseDouble(samples[i]);
			}
			break;
		default:
			sample_objs = samples;
		}
		Arrays.sort(sample_objs);
		ArrayList<Object> tags = new ArrayList<Object>();
		int interval = (samples.length+2)/buckets;
		int left = (samples.length+2)%buckets;
		int cur = interval-2;
		for(int i=0; i<buckets-1; i++){
			if(i<left){
				cur++;
			}
			tags.add(sample_objs[cur]);
			cur += interval;
		}
		return tags.toArray(new Comparable[0]);
	}
}
