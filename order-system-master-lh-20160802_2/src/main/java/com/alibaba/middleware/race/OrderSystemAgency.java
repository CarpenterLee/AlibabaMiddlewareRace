package com.alibaba.middleware.race;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import avro.io.MyDecoder;
import gzx.cache.buffer.BufferPoolInputStream;
import hoolee.batch.IndexDriver;
import hoolee.cache.ResultCache;
import hoolee.index.BTreeNodeLookUpMutiDir;
import hoolee.sort.KeysNumber;
import hoolee.sort.MyUUID;
import hoolee.sort.TypeInference;
import hoolee.util.DistinctDisk;
import hoolee.util.Pair;
import hoolee.util.SearchContext;
import hoolee.util.Trinal;
public class OrderSystemAgency implements OrderSystem {
	final long bucket_size_in_B = 1024*1024*100;
	final int medium_nodes_cache_length = 1000;
	final int leaf_nodes_cache_length = 5000;
	ResultCache<String, ResultImp> buyer_result_cache = null;
	ResultCache<String, ResultImp> good_result_cache = null;
	
	final int assumed_order_line_size_in_B = 230;
	final int assumed_good_line_size_in_B = 947;
	final int assumed_buyer_line_size_in_B = 262;
	final double freq = 0.001;
	final int parallelism = 8;
	
	private SearchContext order_sorttedByBuyerid_context;
	private BTreeNodeLookUpMutiDir<Comparable<?>> order_sorttedByBuyerid_rootNode;
	private SearchContext order_sorttedByOrderid_context;
	private BTreeNodeLookUpMutiDir<Comparable<?>> order_sorttedByOrderid_rootNode;
	private SearchContext order_sorttedByGoodid_context;
	private BTreeNodeLookUpMutiDir<Comparable<?>> order_sorttedByGoodid_rootNode;
	private SearchContext buyer_context;
	private BTreeNodeLookUpMutiDir<Comparable<?>> buyer_rootNode;
	private SearchContext good_context;
	private BTreeNodeLookUpMutiDir<Comparable<?>> good_rootNode;
	
	final int threadPool_size_for_this = 4;

	private volatile boolean isAllInit = false;
	Collection<String> orderFiles;
	Collection<String> buyerFiles;
	Collection<String> goodFiles;
	Collection<String> storeFolders;
	IndexDriver indexDriver = new IndexDriver();
	String tagOutDirName = "tagged";
	String sortOutDirName = "sortted";
	String offsetOutDirName = "offset"; 
	String offsetOutDirName_secondary = "secondary_offset"; 
	String indexOutDirName = "index";
	String[] storeFolders_order_sorttedByOrderid;
	String[] storeFolders_order_sorttedByBuyerid;
	String[] storeFolders_order_sorttedByGoodid;
	String[] storeFolders_buyer;		
	String[] storeFolders_good;	
	ArrayList<String> secondary_offsetFiles_list = new ArrayList<String>();
//	Pair<File[][], File[][]> taggedFiles;
	SearchContext dual_tagged_context;
	/*
	 * BTree索引可以是主键索引，也可以是非主键索引。主键做因直接对源文件排序并索引即可，
	 * 非主键索引要依附于某一个主键索引。由于文件已经按照【主键索引排序】，非主键索引就不能
	 * 对源文件排序了，而是要对非主键的在主键文件中的offset进行排序，并【对这些offset进行
	 * 排序索引】。
	 * 对非主键索引检索时，需要首先按照BTree做因找到offset文件的索引，接着读取offset
	 * 文件，从中要到在源文件中真正的索引，接着读取源文件，找到记录。
	 * (non-Javadoc)
	 * @see com.alibaba.middleware.race.OrderSystem#construct(java.util.Collection, java.util.Collection, java.util.Collection, java.util.Collection)
	 */
	@Override
	public void construct(Collection<String> orderFiles,
			Collection<String> buyerFiles, Collection<String> goodFiles,
			Collection<String> storeFolders) throws IOException,
			InterruptedException {
		storeFolders_order_sorttedByOrderid = toArrayFromBaseDir(storeFolders, "order_orderid_secondary");
		storeFolders_order_sorttedByBuyerid = toArrayFromBaseDir(storeFolders, "order_sorttedByBuyerid");
		storeFolders_order_sorttedByGoodid = toArrayFromBaseDir(storeFolders, "order_sorttedByGoodid");
		storeFolders_buyer = toArrayFromBaseDir(storeFolders, "buyer");		
		storeFolders_good = toArrayFromBaseDir(storeFolders, "good");	
//		outMtab();
		System.out.println("###method construct()---");
		System.out.println("orderFiles.size()=" + orderFiles.size() + ", orderFiles=");
		long total_size = 0;
		for(String str : orderFiles){
			File f = new File(str);
			total_size += f.length();
			System.out.println(f.getAbsolutePath() + "\t" + f.length());
		}
		System.out.println("orderFiles total_size=" + total_size);
		
		System.out.println("\nbuyerFiles.size()=" + buyerFiles.size() + ", buyerFiles=");
		total_size = 0;
		for(String str : buyerFiles){
			File f = new File(str);
			total_size += f.length();
			System.out.println(f.getAbsolutePath() + "\t" + f.length());
		}
		System.out.println("buyerFiles total_size=" + total_size);
		
		System.out.println("\ngoodFiles.size()=" + goodFiles.size() + ", goodFiles=");
		total_size = 0;
		for(String str : goodFiles){
			File f = new File(str);
			total_size += f.length();
			System.out.println(f.getAbsolutePath() + "\t" + f.length());
		}
		System.out.println("goodFiles total_size=" + total_size);
		
		System.out.println("\nstoreFolders.size()=" + storeFolders.size() + ", storeFolders=");
		total_size = 0;
		for(String str : storeFolders){
			File f = new File(str);
			total_size += f.length();
			System.out.println(f.getAbsolutePath() + "\t" + f.length());
		}
		System.out.println("storeFolders total_size=" + total_size);
		
//		orderFiles = asShuffledCollection(orderFiles);// load balance, shuffle input file
//		buyerFiles = asShuffledCollection(buyerFiles);
//		goodFiles = asShuffledCollection(goodFiles);
		orderFiles = asShuffledCollection2(orderFiles);
		buyerFiles = asShuffledCollection2(buyerFiles);
		goodFiles = asShuffledCollection2(goodFiles);
		System.out.println("\norderFiles after suffle:" + orderFiles);
		System.out.println("\nbuyerFiles after suffle:" + buyerFiles);
		System.out.println("\ngoodFiles after suffle:" + goodFiles);
//		Collection<String> orderFiles,
//		Collection<String> buyerFiles, Collection<String> goodFiles,
//		Collection<String> storeFolders
		this.orderFiles = orderFiles;
		this.buyerFiles = buyerFiles;
		this.goodFiles = goodFiles;
		this.storeFolders = storeFolders;
		
//		System.out.println("totoal_query_count=" + totoal_query_count.incrementAndGet());
		long start_time = System.currentTimeMillis();
		
		try{
			this.dual_tagged_context = indexDriver.batch_DualTagging(orderFiles, 
					storeFolders_order_sorttedByGoodid, storeFolders_order_sorttedByBuyerid, 
					tagOutDirName, tagOutDirName, 
					"goodid", "buyerid", 
					bucket_size_in_B, freq, assumed_order_line_size_in_B,
					null, null, null, parallelism);
			// index order by goodid
			order_sorttedByGoodid_context = 
					indexDriver.batchTaggingSortingIndexingDualOffset(orderFiles, storeFolders_order_sorttedByGoodid, 
					tagOutDirName, sortOutDirName, offsetOutDirName, offsetOutDirName_secondary, 
					indexOutDirName, "goodid", "orderid", bucket_size_in_B, freq, 
					dual_tagged_context.samplerHalper, dual_tagged_context.keyNumber, dual_tagged_context.typeInference, 
					dual_tagged_context.dual_taggedFiles.key, parallelism);
			
			File[][] secondary_offsetFiles = order_sorttedByGoodid_context.secondary_offsetFiles;
			ArrayList<String> secondary_offsetFiles_list = new ArrayList<String>();
			int max_len = 0;
			for(int i=0; i<secondary_offsetFiles.length; ++i){
				max_len = Math.max(max_len, secondary_offsetFiles[i].length);
			}
			for(int j=0; j<max_len; ++j){// load balance
				for(int i=0; i<secondary_offsetFiles.length; ++i){
					if(j >= secondary_offsetFiles[i].length){
						continue;
					}
					secondary_offsetFiles_list.add(secondary_offsetFiles[i][j].toString());
				}
			}
			// index order by orderid
			order_sorttedByOrderid_context = indexDriver.batchTaggingSortingIndexingSecondaryOffset(
					secondary_offsetFiles_list, 
					storeFolders_order_sorttedByOrderid, tagOutDirName, 
					sortOutDirName, offsetOutDirName, indexOutDirName, "orderid", bucket_size_in_B,
					order_sorttedByGoodid_context.samplerHalper, 
					parallelism);
			
			// index order by buyerid
			order_sorttedByBuyerid_context = indexDriver.batchTaggingSecondarySortingIndexing(
					orderFiles, storeFolders_order_sorttedByBuyerid, tagOutDirName, 
					sortOutDirName, offsetOutDirName, indexOutDirName, 
					"buyerid", "createtime", bucket_size_in_B, freq, 
					order_sorttedByGoodid_context.samplerHalper,
					order_sorttedByGoodid_context.keyNumber,
					order_sorttedByGoodid_context.typeInference, 
					dual_tagged_context.dual_taggedFiles.value,
					parallelism);
			
			order_sorttedByOrderid_context.samplerHalper = null;//clear
			buyer_context = indexDriver.batchTaggingSortingIndexing(buyerFiles, storeFolders_buyer, tagOutDirName, 
					sortOutDirName, offsetOutDirName, indexOutDirName, 
					"buyerid", bucket_size_in_B, assumed_buyer_line_size_in_B, freq, null, null, null, null, parallelism);
			good_context = indexDriver.batchTaggingSortingIndexing(goodFiles, storeFolders_good, tagOutDirName, 
					sortOutDirName, offsetOutDirName, indexOutDirName, 
					"goodid", bucket_size_in_B, assumed_good_line_size_in_B, freq, null, null, null, null, parallelism);
			buyer_context.samplerHalper = null;
			good_context.samplerHalper = null;
			
		}catch(Exception e){
			e.printStackTrace();
			System.exit(-1);
		}
		order_sorttedByBuyerid_rootNode = new BTreeNodeLookUpMutiDir<Comparable<?>>(
				order_sorttedByBuyerid_context.leafNodeNumber_filaPath_map, 
				order_sorttedByBuyerid_context.keyType, medium_nodes_cache_length, leaf_nodes_cache_length);
		order_sorttedByOrderid_rootNode = new BTreeNodeLookUpMutiDir<Comparable<?>>(
				order_sorttedByOrderid_context.leafNodeNumber_filaPath_map, 
				order_sorttedByOrderid_context.keyType, medium_nodes_cache_length, leaf_nodes_cache_length);
		order_sorttedByGoodid_rootNode = new BTreeNodeLookUpMutiDir<Comparable<?>>(
				order_sorttedByGoodid_context.leafNodeNumber_filaPath_map, 
				order_sorttedByGoodid_context.keyType, medium_nodes_cache_length, leaf_nodes_cache_length);
		buyer_rootNode = new BTreeNodeLookUpMutiDir<Comparable<?>>(buyer_context.leafNodeNumber_filaPath_map, 
				buyer_context.keyType, medium_nodes_cache_length, leaf_nodes_cache_length);
		good_rootNode = new BTreeNodeLookUpMutiDir<Comparable<?>>(good_context.leafNodeNumber_filaPath_map, 
				good_context.keyType, medium_nodes_cache_length, leaf_nodes_cache_length);
		this.outTime(start_time, "construct time: ");
	}
	@Override
	public Result queryOrder(long orderId, Collection<String> keys) {
		System.out.println("---queryOrder()----");
		lateInit();
		/**如果订单不存在，一定要返回null，所以要先验证订单是否存在。*/
//		System.out.println("--queryOrder()--");
		// 得到二级索引地址
		Pair<Integer, Long> fileNumber_offset = order_sorttedByOrderid_rootNode.lookUp(orderId);// order table
		if(fileNumber_offset == null){// not found, return null
			return null;
		}
		ResultImp result = new ResultImp(orderId);
		if(keys!=null && keys.isEmpty()){// keys is empty, return empty
			return result;
		}
		try{
			// 使用二级索引得到原始索引
			fileNumber_offset =  readSecondaryIndex(
					order_sorttedByOrderid_context.sorttedFileNumber_filePath_map, fileNumber_offset);
			// 使用原始索引读取记录
			File oridiary_file = order_sorttedByGoodid_context.sorttedFileNumber_filePath_map.get(fileNumber_offset.key);
//			FileInputStream inputStream_order = new FileInputStream(oridiary_file);
            BufferPoolInputStream inputStream_order=new BufferPoolInputStream(oridiary_file);
			HashSet<String> matched_keyNames = new HashSet<String>();
			boolean is_all_matched = readLineFromSorttedBinFile(inputStream_order, fileNumber_offset.value, 
					order_sorttedByGoodid_context.keyNumber, order_sorttedByGoodid_context.typeInference, 
					keys, result, matched_keyNames);
			inputStream_order.close();
			if(!is_all_matched){// buyer table
				fileNumber_offset = buyer_rootNode.lookUp(result.getBuyerid());
				if(fileNumber_offset != null){
//					FileInputStream inputStream_buyer = new FileInputStream(
//							buyer_context.sorttedFileNumber_filePath_map.get(fileNumber_offset.key));
					BufferPoolInputStream inputStream_buyer = new BufferPoolInputStream(
							buyer_context.sorttedFileNumber_filePath_map.get(fileNumber_offset.key));
					is_all_matched = readLineFromSorttedBinFile(inputStream_buyer, fileNumber_offset.value,
							buyer_context.keyNumber, buyer_context.typeInference, 
							keys, result, matched_keyNames);
					inputStream_buyer.close();
				}
			}
			if(!is_all_matched){// good table
				fileNumber_offset = good_rootNode.lookUp(result.getGoodid());
				if(fileNumber_offset != null){
//					FileInputStream inputStream_good = new FileInputStream(
//							good_context.sorttedFileNumber_filePath_map.get(fileNumber_offset.key));
					BufferPoolInputStream inputStream_good = new BufferPoolInputStream(
							good_context.sorttedFileNumber_filePath_map.get(fileNumber_offset.key));
					readLineFromSorttedBinFile(inputStream_good, fileNumber_offset.value,
							good_context.keyNumber, good_context.typeInference, 
							keys, result, matched_keyNames);
					inputStream_good.close();
				}
			}
			inputStream_order.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		return result;
	}
	/**从二级索引中读取最终索引地址*/
	private Pair<Integer, Long> readSecondaryIndex(Map<Integer, File> number_path, Pair<Integer, Long> fileNumber_offset)throws Exception{
		FileInputStream inputStream = new FileInputStream(number_path.get(fileNumber_offset.key));
		inputStream.getChannel().position(fileNumber_offset.value);
//		inputStream.postion(fileNumber_offset.value);
		MyDecoder decoder = new MyDecoder(inputStream, 64);
		decoder.readLong();//skip orderid
		int fileNumber2 = decoder.readInt();// file number
		long offset2 = decoder.readLong();// offset
		inputStream.close();
		return new Pair<Integer, Long>(fileNumber2, offset2);
	}
	@Override
	public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime,
			String buyerid) {
		System.out.println("--queryOrdersByBuyer()--");
		lateInit();
		Iterator<Result> results = new ArrayList<Result>().iterator(); 
		Pair<Integer, Long> fileNumber_offset = order_sorttedByBuyerid_rootNode.lookUp(buyerid);
		if(fileNumber_offset == null){
			return results;
		}
		try{
//			FileInputStream inputStream = new FileInputStream(
//					order_sorttedByBuyerid_context.sorttedFileNumber_filePath_map.get(fileNumber_offset.key));
			BufferPoolInputStream inputStream = new BufferPoolInputStream(
			order_sorttedByBuyerid_context.sorttedFileNumber_filePath_map.get(fileNumber_offset.key));
			results = readLinesFromSorttedBinFile_ByBuyer(inputStream, fileNumber_offset.value, 
					order_sorttedByBuyerid_context.keyNumber, order_sorttedByBuyerid_context.typeInference, 
					buyerid, startTime, endTime);
			inputStream.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		return results;
	}

	@Override
	public Iterator<Result> queryOrdersBySaler(String salerid, String goodid,
			Collection<String> keys) {
		lateInit();
		System.out.println("--queryOrdersBySaler()--");
		Iterator<Result> results = new ArrayList<Result>().iterator(); 
		Pair<Integer, Long> fileNumber_offset = order_sorttedByGoodid_rootNode.lookUp(goodid);
		if(fileNumber_offset == null){
			return results;
		}
		try{
//			FileInputStream inputStream = new FileInputStream(
//					order_sorttedByGoodid_context.sorttedFileNumber_filePath_map.get(fileNumber_offset.key));
			BufferPoolInputStream inputStream = new BufferPoolInputStream(
					order_sorttedByGoodid_context.sorttedFileNumber_filePath_map.get(fileNumber_offset.key));
//			order_sorttedByGoodid_context.sorttedFileNumber_filePath_map.get(fileNumber_offset.key));
			results = readLinesFromSorttedBinFile_bySalerGood(inputStream, fileNumber_offset.value, 
					order_sorttedByGoodid_context.keyNumber, order_sorttedByGoodid_context.typeInference, goodid, keys);
			inputStream.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		return results;
	}

	@Override
	public KeyValue sumOrdersByGood(String goodid, String key) {
		lateInit();
		System.out.println("--sumOrdersByGood()--");
		Pair<Integer, Long> fileNumber_offset = order_sorttedByGoodid_rootNode.lookUp(goodid);
		if(fileNumber_offset == null){
			return null;
		}
		Object result = null;
		try{
//			FileInputStream inputStream = new FileInputStream(
//					order_sorttedByGoodid_context.sorttedFileNumber_filePath_map.get(fileNumber_offset.key));
			BufferPoolInputStream inputStream = new BufferPoolInputStream(
					order_sorttedByGoodid_context.sorttedFileNumber_filePath_map.get(fileNumber_offset.key));
			result = this.readLinesForSum_may_join(inputStream, fileNumber_offset.value, 
					order_sorttedByGoodid_context.keyNumber, order_sorttedByGoodid_context.typeInference, goodid, key);
			inputStream.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		if(result == null){
			return null;
		}
		return new KeyValueImp(key, result);
	}
	private String[] toArrayFromBaseDir(Collection<String> storeFolders, String childDir){
		String[] arr = new String[storeFolders.size()];
		int t = 0;
		for(String str : storeFolders){
			arr[t++] = str+"/"+childDir;
		}
		return arr;
	}
	private void outTime(long startTime, String note){
		long interval = System.currentTimeMillis()-startTime;
		long minute = interval/1000/60;
		double seconds = (double)(interval%(1000*60))/1000;
		System.out.println(note + " " + minute + " minutes, " + String.format("%2f seconds", seconds));
		
	}
	/**may join*/
//	private Object readLinesForSum_may_join(FileInputStream inputStream, long first_offset,
	private Object readLinesForSum_may_join(BufferPoolInputStream inputStream, long first_offset, 
			final KeysNumber keyNumber, final TypeInference typeInference,
			String target_goodid, final String key) throws Exception{
		TypeInference.Type key_type = typeInference.getType(key);
		double sum_double = 0.0;
		long sum_long = 0L;
		double value_double_gd = 0.0;
		long value_long_gd = 0L;
		boolean is_long_or_double = true;
		boolean is_found_at_leat_onces = false;
		boolean is_good_table_matched = false;
		final ArrayList<String> keys = new ArrayList<String>();
		keys.add(key);
		// look up good Table
		Pair<Integer, Long> fileNumber_offset_good = good_rootNode.lookUp((Comparable<?>)target_goodid);
		if(fileNumber_offset_good != null){
			ResultImp result_2 = new ResultImp();
//			FileInputStream inputStream_good = new FileInputStream(
//					good_context.sorttedFileNumber_filePath_map.get(fileNumber_offset_good.key));
			BufferPoolInputStream inputStream_good = new BufferPoolInputStream(
					good_context.sorttedFileNumber_filePath_map.get(fileNumber_offset_good.key));
			readLineFromSorttedBinFile(inputStream_good, fileNumber_offset_good.value,
					good_context.keyNumber, good_context.typeInference, 
					keys, result_2, new HashSet<String>());
			inputStream_good.close();
			Object rs = result_2.getValue(key);
			if(rs != null){
				is_good_table_matched = true;
				is_found_at_leat_onces = true;
				if(rs instanceof Long){
					value_long_gd += (Long)rs;
				}else if(rs instanceof Double){
					value_double_gd += (Double)rs;
				}else{
					String rs_str = String.valueOf(rs);
					if(TypeInference.isLong(rs_str)){
						value_long_gd += Long.parseLong(rs_str);
					}else if(TypeInference.isDouble(rs_str)){
						value_double_gd += Double.parseDouble(rs_str);
					}else{
						is_long_or_double = false;
					}
				}
			}
			
		}
		if(!is_long_or_double){
			return null;
		}
//		inputStream.getChannel().position(first_offset);
		inputStream.postion(first_offset);
//		MyDecoder decoder = new MyDecoder(inputStream, 1024*512);
		MyDecoder decoder = new MyDecoder(inputStream);
		ExecutorService executors = Executors.newFixedThreadPool(threadPool_size_for_this);
		ExecutorCompletionService<Trinal<Pair<Long, Double>, Boolean, Boolean>> completionService = 
				new ExecutorCompletionService<Trinal<Pair<Long, Double>, Boolean, Boolean>>(executors);
		final ConcurrentHashMap<Comparable<?>, Pair<Long, Double>> value_cache = 
				new ConcurrentHashMap<Comparable<?>, Pair<Long, Double>>();
		int submit_times = 0;
		while(is_long_or_double && !decoder.isEnd()){// while every line
//			int line_size = decoder.readInt();
			int line_size=decoder.readLineLength();
			MyDecoder line_decoder = new MyDecoder(decoder.buf, decoder.readFixed(line_size), line_size);
			String cur_goodid = null;
			Object cur_value = null;
			String cur_buyerid = null;
			while(!line_decoder.isEnd()){
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
					if(key.equals(key_name)){
						v_value = line_decoder.readDouble();
					}else{
						line_decoder.skipFixed(8);
					}
					break;
				case STR_UUID:
					if(key.equals(key_name) || "goodid".equals(key_name) || "buyerid".equals(key_name)){
						String uuid_prefix = line_decoder.readString();
						v_value = MyUUID.byte16ToUUID_WithPrefix(uuid_prefix, line_decoder.buf, line_decoder.readFixed(16));
					}else{// skip
						line_decoder.skipString();
						line_decoder.skipFixed(16);
					}
					break;
				case STR_HALFUUID:
					if(key.equals(key_name) || "goodid".equals(key_name) || "buyerid".equals(key_name)){
						String uuid_prefix = line_decoder.readString();
						v_value = MyUUID.byte8ToHALFUUID_WithPrefix(uuid_prefix, line_decoder.buf, line_decoder.readFixed(8));
					}else{// skip
						line_decoder.skipString();
						line_decoder.skipFixed(8);
					}
					break;
				case UUID:
					if(key.equals(key_name) || "goodid".equals(key_name) || "buyerid".equals(key_name)){
						v_value = MyUUID.byte16ToUUID(line_decoder.buf, line_decoder.readFixed(16));
					}else{
						line_decoder.skipFixed(16);
					}
					break;
				default:// STRING
					if(key.equals(key_name) || "goodid".equals(key_name) || "buyerid".equals(key_name)){
						v_value = line_decoder.readString();
					}else{
						line_decoder.skipString();
					}
					
				}
				if(key.equals(key_name)){
					cur_value = v_value;
				}else if("goodid".equals(key_name)){
					cur_goodid = (String)v_value;
				}else if("buyerid".equals(key_name)){
					cur_buyerid = (String)v_value;
				}
			}// while !line_decoder.isEnd()
			if(!target_goodid.equals(cur_goodid)){// will return
				break;
			}
			if(is_good_table_matched){
				sum_long += value_long_gd;
				sum_double += value_double_gd;
			}else if(cur_value != null){
				is_found_at_leat_onces = true;
				if(key_type == TypeInference.Type.LONG){
					sum_long += (Long)cur_value;
				}else if(key_type == TypeInference.Type.DOUBLE){
					sum_double += (Double)cur_value;
				}else{
					String str_value = String.valueOf(cur_value);
					if(TypeInference.isLong(str_value)){
						sum_long += Long.parseLong(str_value);
					}else if(TypeInference.isDouble(str_value)){
						sum_double += Double.parseDouble(str_value);
					}else{
						is_long_or_double = false;
					}
				}
			}else{// join buyer table
				final String cur_bid = cur_buyerid; 
				Pair<Long, Double> cache = value_cache.get(cur_bid);
				if(cache != null){
					sum_long += cache.key;
					sum_double += cache.value;
					continue;
				}
				
				submit_times++;
				completionService.submit(new Callable<Trinal<Pair<Long, Double>, Boolean, Boolean>>(){
					@Override
					public Trinal<Pair<Long, Double>, Boolean, Boolean> call(){
						Pair<Integer, Long> fileNumber_offset_buyer = buyer_rootNode.lookUp(cur_bid);
						boolean is_found = false;
						boolean is_l_or_d = true;
						long s_long = 0L;
						double s_double = 0.0;
						if(fileNumber_offset_buyer != null){
							try{
							ResultImp result_3 = new ResultImp();
//							FileInputStream inputStream_buyer = new FileInputStream(
//									buyer_context.sorttedFileNumber_filePath_map.get(fileNumber_offset_buyer.key));
							BufferPoolInputStream inputStream_buyer = new BufferPoolInputStream(
									buyer_context.sorttedFileNumber_filePath_map.get(fileNumber_offset_buyer.key));
							readLineFromSorttedBinFile(inputStream_buyer, fileNumber_offset_buyer.value,
									buyer_context.keyNumber, buyer_context.typeInference, 
									keys, result_3, new HashSet<String>());
							inputStream_buyer.close();
							Object rs = result_3.getValue(key);
							if(rs != null){
								is_found = true;
								if(rs instanceof Long){
									s_long += (Long)rs;
								}else if(rs instanceof Double){
									s_double += (Double)rs;
								}else{
									String rs_str = String.valueOf(rs);
									if(TypeInference.isLong(rs_str)){
										s_long += Long.parseLong(rs_str);
									}else if(TypeInference.isDouble(rs_str)){
										s_double += Double.parseDouble(rs_str);
									}else{
										is_l_or_d = false;
									}
								}
							}// rs != null
							}catch(Exception e){
								e.printStackTrace();
//								System.exit(-1);
							}
						}
						value_cache.put(cur_bid, new Pair<Long, Double>(s_long, s_double));
						return new Trinal<Pair<Long, Double>, Boolean, Boolean>(
								new Pair<Long, Double>(s_long, s_double), is_found, is_l_or_d);
					}// call()
				});// submit
			}// join buyer table
		}// while every line
		
//		System.out.println("join_times=" + join_times);
		for(int i=0; i<submit_times; i++){
			Trinal<Pair<Long, Double>, Boolean, Boolean> rs = completionService.take().get();
			if(!rs.third){// !is_long_or_double
				executors.shutdownNow();
				return null;
			}
			is_found_at_leat_onces |= rs.second; // is_found
			sum_long += rs.first.key;// s_long
			sum_double += rs.first.value; // s_double
		}
		executors.shutdownNow();
		
		if(!is_long_or_double || !is_found_at_leat_onces){
			return null;
		}
		if(sum_long == 0){
			return Double.valueOf(sum_double);
		}else if(sum_double == 0){
			return Long.valueOf(sum_long);
		}
		return Double.valueOf(sum_double+sum_long);
	}
	/**
	 * 读取多行，并指定字段. 可能会做jion.
	 * @param inputStream
	 * @param first_offset
	 * @param keyNumber
	 * @param typeInference
	 * @param target_goodid
	 * @param keys
	 * @return 结果集的迭代器，按照orderid升序排列
	 */
//	private Iterator<Result> readLinesFromSorttedBinFile_bySalerGood(FileInputStream inputStream, long first_offset, 
	private Iterator<Result> readLinesFromSorttedBinFile_bySalerGood(BufferPoolInputStream inputStream, long first_offset, 
			final KeysNumber keyNumber, final TypeInference typeInference, 
			String target_goodid, final Collection<String> keys)throws Exception{

		// lookup good table
		Pair<Integer, Long> fileNumber_offset_gd = good_rootNode.lookUp((Comparable<?>)target_goodid);
		ResultImp result_good = new ResultImp();
		HashSet<String> matched_set = new HashSet<String>();
		if(fileNumber_offset_gd != null){
//			FileInputStream inputStream_good = new FileInputStream(
//					good_context.sorttedFileNumber_filePath_map.get(fileNumber_offset_gd.key));
			BufferPoolInputStream inputStream_good = new BufferPoolInputStream(
					good_context.sorttedFileNumber_filePath_map.get(fileNumber_offset_gd.key));
			readLineFromSorttedBinFile(inputStream_good, fileNumber_offset_gd.value,
					good_context.keyNumber, good_context.typeInference, 
					keys, result_good, matched_set);
			inputStream_good.close();
		}
		
//		inputStream.getChannel().position(first_offset);
		inputStream.postion(first_offset);
//		MyDecoder decoder = new MyDecoder(inputStream, 1024*512);
		MyDecoder decoder = new MyDecoder(inputStream);
		TreeMap<Long, Result> result_set = new TreeMap<Long, Result>();
		ExecutorService executors = Executors.newFixedThreadPool(threadPool_size_for_this);
		ExecutorCompletionService<Integer> completionService = new ExecutorCompletionService<Integer>(executors);
		final ConcurrentHashMap<Comparable<?>, ResultImp> buyer_cache = new ConcurrentHashMap<Comparable<?>, ResultImp>();
		int submit_times = 0;
		while(!decoder.isEnd()){// while have more line matching
//			int line_size = decoder.readInt();
			int line_size = decoder.readLineLength();
			
			MyDecoder line_decoder = new MyDecoder(decoder.buf, decoder.readFixed(line_size), line_size);
			final ResultImp result = new ResultImp();
			Long cur_orderid = null;
			String cur_goodid = null;
//			matched_keyNames.clear();
			final HashSet<String> matched_keyNames = new HashSet<String>();
			Comparable<?> buyerid_to_lookup = null;
			while(!line_decoder.isEnd()){// every key value in the line
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
					v_value = line_decoder.readLong();	
					break;
				case DOUBLE:
					if(keys==null || keys.contains(key_name)){
						v_value = line_decoder.readDouble();break;
					}else{
						line_decoder.skipFixed(8);
					}
				case STR_UUID:
					if(keys==null || "goodid".equals(key_name) || "orderid".equals(key_name) || "buyerid".equals(key_name)
							|| keys.contains(key_name)){
						String uuid_prefix = line_decoder.readString();
						v_value = MyUUID.byte16ToUUID_WithPrefix(uuid_prefix, line_decoder.buf, line_decoder.readFixed(16));
					}else{
						line_decoder.skipString();
						line_decoder.skipFixed(16);
					}
					break;
				case STR_HALFUUID:
					if(keys==null || "goodid".equals(key_name) || "orderid".equals(key_name) || "buyerid".equals(key_name)
							|| keys.contains(key_name)){
						String uuid_prefix = line_decoder.readString();
						v_value = MyUUID.byte8ToHALFUUID_WithPrefix(uuid_prefix, line_decoder.buf, line_decoder.readFixed(8));
					}else{
						line_decoder.skipString();
						line_decoder.skipFixed(8);
					}
					break;
						
				case UUID:
					if(keys==null || "goodid".equals(key_name) || "orderid".equals(key_name) || "buyerid".equals(key_name)
							|| keys.contains(key_name)){
						v_value = MyUUID.byte16ToUUID(line_decoder.buf, line_decoder.readFixed(16));
					}else{
						line_decoder.skipFixed(16);
					}
					break;
				default:// STRING
					if(keys==null || "goodid".equals(key_name) || "orderid".equals(key_name) || "buyerid".equals(key_name)
							|| keys.contains(key_name)){
						v_value = line_decoder.readString();						
					}else{
						line_decoder.skipString();
					}
				}
				if(keys==null || keys.contains(key_name)){
					matched_keyNames.add(key_name);
					result.add(key_name, v_value);
				}
				if("goodid".equals(key_name)){
					cur_goodid = (String)v_value;
				}else if("orderid".equals(key_name)){
					cur_orderid = (Long)v_value;
					result.setOrderId(cur_orderid);
				}
				if("buyerid".equals(key_name)){
					buyerid_to_lookup = (Comparable<?>)v_value;
				}
			}// while !line_decoder.isEnd()
			if(target_goodid.equals(cur_goodid)){
				result_set.put(cur_orderid, result);
			}else{
				break;// will return
			}
			boolean is_all_matched = true;
			result.merge(result_good);// merge result_good with result 
			matched_keyNames.addAll(result_good.keySet());
			if(keys==null || matched_keyNames.size()<keys.size()){
				is_all_matched = false;
			}
			if(!is_all_matched){// buyer table
				final Comparable<?> b_id = buyerid_to_lookup;
				ResultImp b_cache = buyer_cache.get(b_id);
				if(b_cache != null){// cache hit
					result.merge(b_cache);
					continue;
				}
				submit_times++;
				completionService.submit(new Callable<Integer>(){
					@Override
					public Integer call() throws Exception{
						Pair<Integer, Long> fileNumber_offset = buyer_rootNode.lookUp(b_id);
						if(fileNumber_offset != null){
							ResultImp rs_b = new ResultImp();
//							FileInputStream inputStream_buyer = new FileInputStream(
//									buyer_context.sorttedFileNumber_filePath_map.get(fileNumber_offset.key));
                            BufferPoolInputStream inputStream_buyer = new BufferPoolInputStream(
                                    buyer_context.sorttedFileNumber_filePath_map.get(fileNumber_offset.key));
							readLineFromSorttedBinFile(inputStream_buyer, fileNumber_offset.value,
									buyer_context.keyNumber, buyer_context.typeInference, 
									keys, rs_b, new HashSet<String>());
							inputStream_buyer.close();
							result.merge(rs_b);
							buyer_cache.put(b_id, rs_b);// put into cache
						}
						return 0;
					}
				});
			}// if(!is_all_matched)
		}// while have more line matching
//		System.out.println("join_times=" + join_times);
		for(int i=0; i<submit_times; i++){
			completionService.take();
		}
		executors.shutdownNow();
		return result_set.values().iterator(); 
	}
	/**
	 * 读取多行，做级联查询
	 * @param inputStream
	 * @param first_offset
	 * @param keyNumber
	 * @param typeInference
	 * @param target_buyerid
	 * @param startTime
	 * @param endTime
	 * @return 结果集的迭代器，结果按照createtime逆序排列
	 */
//	private Iterator<Result> readLinesFromSorttedBinFile_ByBuyer(FileInputStream inputStream, long first_offset, 
	private Iterator<Result> readLinesFromSorttedBinFile_ByBuyer(BufferPoolInputStream inputStream, long first_offset, 
			final KeysNumber keyNumber, final TypeInference typeInference, 
			String target_buyerid, long startTime, long endTime)throws Exception{
//		inputStream.getChannel().position(first_offset);
		inputStream.postion(first_offset);
		MyDecoder decoder = new MyDecoder(inputStream);
		TreeMap<Long, Result> result_map = new TreeMap<Long, Result>();
		
		ResultImp cached_buyer = null;
		if(buyer_result_cache != null){
			cached_buyer = buyer_result_cache.get(target_buyerid);
		}
		ResultImp result_buyer_s = null;
		if(cached_buyer == null){
			result_buyer_s = new ResultImp();
			Pair<Integer, Long> fileNumber_offset_buyer = buyer_rootNode.lookUp(target_buyerid);// query buyer table
			if(fileNumber_offset_buyer != null){
//				FileInputStream inputStream_buyer = new FileInputStream(
//						buyer_context.sorttedFileNumber_filePath_map.get(fileNumber_offset_buyer.key));
				BufferPoolInputStream inputStream_buyer = new BufferPoolInputStream(
						buyer_context.sorttedFileNumber_filePath_map.get(fileNumber_offset_buyer.key));
				readLineFromSorttedBinFile(inputStream_buyer, fileNumber_offset_buyer.value,
						buyer_context.keyNumber, buyer_context.typeInference, 
						null, result_buyer_s, null);
				inputStream_buyer.close();
				if(buyer_result_cache != null){
					buyer_result_cache.put(target_buyerid, result_buyer_s);
				}
			}
		}
		final ResultImp result_buyer;
		if(cached_buyer != null){
			result_buyer = cached_buyer;
		}else{
			result_buyer = result_buyer_s;
		}
		ExecutorService executors = Executors.newFixedThreadPool(threadPool_size_for_this);
		ExecutorCompletionService<Integer> completionService = new ExecutorCompletionService<Integer>(executors);
//		final ConcurrentHashMap<Comparable<?>, ResultImp> good_cache = new ConcurrentHashMap<Comparable<?>, ResultImp>();
		int submit_times = 0;
		outWhile: 
		while(!decoder.isEnd()){
//			int line_size = decoder.read
			int line_size=decoder.readLineLength();
			MyDecoder line_decoder = new MyDecoder(decoder.buf, decoder.readFixed(line_size), line_size);
			final ResultImp result = new ResultImp();
			result.merge(result_buyer);
			String cur_buyerid = null;
			long cur_createtime = -1;
			while(!line_decoder.isEnd()){
				int key_number = line_decoder.readInt();
				String key_name;
				if(key_number == KeysNumber.NOT_A_KEYNUMBER){
					key_name = line_decoder.readString();
				}else{
					key_name = keyNumber.getKey(key_number);
				}
				Object v_value;
				switch(typeInference.getType(key_name)){
				case BOOLEAN:
					v_value = line_decoder.readBoolean();break;
				case LONG:
					v_value = line_decoder.readLong();break;
				case DOUBLE:
					v_value = line_decoder.readDouble();break;
				case STR_UUID:
					String uuid_prefix = line_decoder.readString();
					v_value = MyUUID.byte16ToUUID_WithPrefix(uuid_prefix, line_decoder.buf, line_decoder.readFixed(16));
					break;
				case STR_HALFUUID:
					String uuid_prefix2 = line_decoder.readString();
					v_value = MyUUID.byte8ToHALFUUID_WithPrefix(uuid_prefix2, line_decoder.buf, line_decoder.readFixed(8));
					break;
				case UUID:
					v_value = MyUUID.byte16ToUUID(line_decoder.buf, line_decoder.readFixed(16));
					break;
				default:// STRING
					v_value = line_decoder.readString();
				}
				result.add(key_name, v_value);
				if("buyerid".equals(key_name)){
					cur_buyerid = (String)v_value;
					if(!target_buyerid.equals(cur_buyerid)){
						break outWhile;
					}
				}else if("createtime".equals(key_name)){
					cur_createtime = (Long)v_value;
					if(cur_createtime < startTime){
						continue outWhile;
					}else if(cur_createtime >= endTime){
						break outWhile;
					}
				}else if("orderid".equals(key_name)){
					result.setOrderId((Long)v_value);
				}
			}// while !line_decoder.isEnd()
			result_map.put(Long.MAX_VALUE-cur_createtime, result);
			final String g_id = (String)result.getValue("goodid");
//			ResultImp rs_cache = good_cache.get(g_id);
//			if(rs_cache != null){// chache hit
//				result.merge(rs_cache);
//				continue;
//			}
			submit_times++;
			completionService.submit(new Callable<Integer>(){
				@Override
				public Integer call() throws Exception{
					try{// for debug reasons, any Exception will cause EXIT
						ResultImp cached_good = null;
						if(good_result_cache != null){
							cached_good = good_result_cache.get(g_id);
						}
						if(cached_good != null){
							result.merge(cached_good);
							return 0;
						}
						
						ResultImp rs_t = new ResultImp();
						Pair<Integer, Long> fileNumber_offset_good = good_rootNode.lookUp(g_id);
						if(fileNumber_offset_good != null){
//							FileInputStream inputStream_good = new FileInputStream(
//									good_context.sorttedFileNumber_filePath_map.get(fileNumber_offset_good.key));
							BufferPoolInputStream inputStream_good = new BufferPoolInputStream(
									good_context.sorttedFileNumber_filePath_map.get(fileNumber_offset_good.key));
							readLineFromSorttedBinFile(inputStream_good, fileNumber_offset_good.value,
									good_context.keyNumber, good_context.typeInference, 
									null, rs_t, null);
							inputStream_good.close();
							result.merge(rs_t);
							if(good_result_cache != null){
								good_result_cache.put(g_id, rs_t);
							}
						}
					}catch(Exception e){
						e.printStackTrace();
//						System.exit(-1);
					}
					return 0;
				}
			});
		}// while(!decoder.isEnd())
		for(int i=0; i<submit_times; i++){
			completionService.take();
		}
		executors.shutdownNow();

//        System.out.println("!!!!!!buyerID "+target_buyerid);
//        System.out.println("!!!!!!starttime "+startTime+"  endtimie "+endTime);
//        for (Map.Entry<Long,Result> entry:result_map.entrySet()) {
//            System.out.print("orderid:"+entry.getValue().orderId()+" ");
//            System.out.print("KV:{");
//            for (KeyValue kv:
//                 entry.getValue().getAll()) {
//                System.out.print(kv.key()+":"+kv.valueAsString()+"\t");
//            }
//            System.out.println("}");
//        }
        return result_map.values().iterator();
	}
	/**
	 * 从二进制文件中读取一行，并保留keys指定的值
	 * @param inputStream
	 * @param offset
	 * @param keyNumber
	 * @param typeInference
	 * @param keys
	 * @param result 用于保存查询结果
	 * @param matched_keyNames 保存已经匹配的keyName
	 * @return 如果keys指定的字段都已找到，返回true。
	 *         如果keys==null, 或者没有找到所有字段，放回false
	 * @throws Exception
	 */
//	private boolean readLineFromSorttedBinFile(FileInputStream inputStream, long offset,
	private boolean readLineFromSorttedBinFile(BufferPoolInputStream inputStream, long offset,
                                               final KeysNumber keyNumber, final TypeInference typeInference,
                                               Collection<String> keys, ResultImp result, HashSet<String> matched_keyNames) throws Exception{
//		inputStream.getChannel().position(offset);
		inputStream.postion(offset);
//		MyDecoder decoder = new MyDecoder(inputStream, 1024*1024);
//		int line_size = decoder.readInt();
		MyDecoder decoder = new MyDecoder(inputStream);
		int line_size = decoder.readLineLength();
		MyDecoder line_decoder = new MyDecoder(decoder.buf, decoder.readFixed(line_size), line_size);
		while(!line_decoder.isEnd()){
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
				if(keys==null || keys.contains(key_name)){
					v_value = line_decoder.readDouble();
				}else{
					line_decoder.skipFixed(8);
				}
				break;
			case STR_UUID:
				if(keys==null || "goodid".equals(key_name) || "buyerid".equals(key_name) 
						|| keys.contains(key_name)){
					String uuid_prefix = line_decoder.readString();
					v_value = MyUUID.byte16ToUUID_WithPrefix(uuid_prefix, line_decoder.buf, line_decoder.readFixed(16));
				}else{
					line_decoder.skipString();
					line_decoder.skipFixed(16);
				}
				break;
			case STR_HALFUUID:
				if(keys==null || "goodid".equals(key_name) || "buyerid".equals(key_name) 
						|| keys.contains(key_name)){
					String uuid_prefix = line_decoder.readString();
					v_value = MyUUID.byte8ToHALFUUID_WithPrefix(uuid_prefix, line_decoder.buf, line_decoder.readFixed(8));
				}else{
					line_decoder.skipString();
					line_decoder.skipFixed(8);
				}
				break;
			case UUID:
				if(keys==null || "goodid".equals(key_name) || "buyerid".equals(key_name) 
						|| keys.contains(key_name)){
					v_value = MyUUID.byte16ToUUID(line_decoder.buf, line_decoder.readFixed(16));
				}else{
					line_decoder.skipFixed(16);
				}
				break;
			default:// STRING
				if(keys==null || "goodid".equals(key_name) || "buyerid".equals(key_name) 
						|| keys.contains(key_name)){
					v_value = line_decoder.readString();
				}else{
					line_decoder.skipString();
				}
			}
			if(keys==null || keys.contains(key_name)){
				result.add(key_name, v_value);
				if(matched_keyNames != null){
					matched_keyNames.add(key_name);
				}
			}
			if("goodid".equals(key_name)){
				result.setGoodid((String)v_value);
			}else if("buyerid".equals(key_name)){
				result.setBuyerid((String)v_value);
			}
		}
		if(keys==null || (matched_keyNames!=null && matched_keyNames.size()!=keys.size())){
			return false;
		}
		return true;
	}
	private Collection<String> asShuffledCollection(Collection<String> files){
		ArrayList<String> list = new ArrayList<String>();
		for(String f : files){
			list.add(f);
		}
		Collections.shuffle(list);
		return list;
	}
	private Collection<String> asShuffledCollection2(Collection<String> files){
		System.out.println("###method asShuffledCollection2():");
		ArrayList<TreeSet<Pair<Long, String>>> list_all = new ArrayList<TreeSet<Pair<Long, String>>>();
		ArrayList<ArrayList<Pair<Long, String>>> list_all2 = new ArrayList<ArrayList<Pair<Long, String>>>();
		for(int i=0; i<=DistinctDisk.distinct_disks.length; i++){
			list_all.add(new TreeSet<Pair<Long, String>>());
			list_all2.add(new ArrayList<Pair<Long, String>>());
			
		}
		for(String str : files){
			File f = new File(str);
			String ab_path = f.getAbsolutePath();
			boolean matched = false;
			for(int i=0; i<DistinctDisk.distinct_disks.length; i++){
				if(ab_path.startsWith(DistinctDisk.distinct_disks[i])){
					matched = true;
					list_all.get(i).add(new Pair<Long, String>(-f.length(), ab_path));
				}
			}
			if(!matched){
				list_all.get(list_all.size()-1).add(new Pair<Long, String>(-f.length(), ab_path));
			}
		}
		int max_len = 0;
		for(int i=0; i<DistinctDisk.distinct_disks.length; i++){
			max_len = Math.max(max_len, list_all.get(i).size());
			list_all2.get(i).addAll(list_all.get(i));
		}
		ArrayList<String> list_rs = new ArrayList<String>();
		for(int i=0; i<max_len; i++){
			for(int j=0; j<DistinctDisk.distinct_disks.length; j++){
				ArrayList<Pair<Long, String>> list = list_all2.get(j);
				if(i < list.size()){
					System.out.println(list.get(i));
					list_rs.add(list.get(i).value);
				}
			}
		}
		return list_rs;
	}
//	private void outMtab(){
//		System.out.println("###method outMtab()");
//		try{
//			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("/etc/mtab")));
//			String line;
//			while((line=reader.readLine()) != null){
//				System.out.println(line);
//			}
//			reader.close();
//		}catch(Exception e){
//			e.printStackTrace();
//		}
//	}
	private void lateInit(){
		if(!isAllInit){
			synchronized(this){
				if(!isAllInit){
					System.out.println("###method isAllInit(), initing...");
					long start_time = System.currentTimeMillis();
					try{
//						this.dual_tagged_context = indexDriver.batch_DualTagging(orderFiles, 
//								storeFolders_order_sorttedByGoodid, storeFolders_order_sorttedByBuyerid, 
//								tagOutDirName, tagOutDirName, 
//								"goodid", "buyerid", 
//								bucket_size_in_B, freq, order_line_size_in_B, 
//								null, null, null, parallelism);
//						// index order by goodid
//						order_sorttedByGoodid_context = 
//								indexDriver.batchTaggingSortingIndexingDualOffset(orderFiles, storeFolders_order_sorttedByGoodid, 
//								tagOutDirName, sortOutDirName, offsetOutDirName, offsetOutDirName_secondary, 
//								indexOutDirName, "goodid", "orderid", bucket_size_in_B, freq, 
//								dual_tagged_context.samplerHalper, dual_tagged_context.keyNumber, dual_tagged_context.typeInference, 
//								dual_tagged_context.dual_taggedFiles.key, parallelism);
						
//						File[][] secondary_offsetFiles = order_sorttedByGoodid_context.secondary_offsetFiles;
//						ArrayList<String> secondary_offsetFiles_list = new ArrayList<String>();
//						int max_len = 0;
//						for(int i=0; i<secondary_offsetFiles.length; ++i){
//							max_len = Math.max(max_len, secondary_offsetFiles[i].length);
//						}
//						for(int j=0; j<max_len; ++j){// load balance
//							for(int i=0; i<secondary_offsetFiles.length; ++i){
//								if(j >= secondary_offsetFiles[i].length){
//									continue;
//								}
//								secondary_offsetFiles_list.add(secondary_offsetFiles[i][j].toString());
//							}
//						}
//						// index order by orderid
//						order_sorttedByOrderid_context = indexDriver.batchTaggingSortingIndexingSecondaryOffset(
//								secondary_offsetFiles_list, 
//								storeFolders_order_sorttedByOrderid, tagOutDirName, 
//								sortOutDirName, offsetOutDirName, indexOutDirName, "orderid", bucket_size_in_B,
//								order_sorttedByGoodid_context.samplerHalper, 
//								parallelism);
						
						// index order by buyerid
//						order_sorttedByBuyerid_context = indexDriver.batchTaggingSecondarySortingIndexing(
//								orderFiles, storeFolders_order_sorttedByBuyerid, tagOutDirName, 
//								sortOutDirName, offsetOutDirName, indexOutDirName, 
//								"buyerid", "createtime", bucket_size_in_B, freq, 
//								order_sorttedByGoodid_context.samplerHalper,
//								order_sorttedByGoodid_context.keyNumber,
//								order_sorttedByGoodid_context.typeInference, 
//								dual_tagged_context.dual_taggedFiles.value,
//								parallelism);
						
//						order_sorttedByOrderid_context.samplerHalper = null;//clear
//						buyer_context = indexDriver.batchTaggingSortingIndexing(buyerFiles, storeFolders_buyer, tagOutDirName, 
//								sortOutDirName, offsetOutDirName, indexOutDirName, 
//								"buyerid", bucket_size_in_B, assumed_buyer_line_size_in_B, freq, null, null, null, null, parallelism);
//						good_context = indexDriver.batchTaggingSortingIndexing(goodFiles, storeFolders_good, tagOutDirName, 
//								sortOutDirName, offsetOutDirName, indexOutDirName, 
//								"goodid", bucket_size_in_B, assumed_good_line_size_in_B, freq, null, null, null, null, parallelism);
//						buyer_context.samplerHalper = null;
//						good_context.samplerHalper = null;
						
					}catch(Exception e){
						e.printStackTrace();
						System.exit(-1);
					}
//					order_sorttedByBuyerid_rootNode = new BTreeNodeLookUpMutiDir<Comparable<?>>(
//							order_sorttedByBuyerid_context.leafNodeNumber_filaPath_map, 
//							order_sorttedByBuyerid_context.keyType, medium_nodes_cache_length, leaf_nodes_cache_length);
//					order_sorttedByOrderid_rootNode = new BTreeNodeLookUpMutiDir<Comparable<?>>(
//							order_sorttedByOrderid_context.leafNodeNumber_filaPath_map, 
//							order_sorttedByOrderid_context.keyType, medium_nodes_cache_length, leaf_nodes_cache_length);
//					order_sorttedByGoodid_rootNode = new BTreeNodeLookUpMutiDir<Comparable<?>>(
//							order_sorttedByGoodid_context.leafNodeNumber_filaPath_map, 
//							order_sorttedByGoodid_context.keyType, medium_nodes_cache_length, leaf_nodes_cache_length);
//					buyer_rootNode = new BTreeNodeLookUpMutiDir<Comparable<?>>(buyer_context.leafNodeNumber_filaPath_map, 
//							buyer_context.keyType, medium_nodes_cache_length, leaf_nodes_cache_length);
//					good_rootNode = new BTreeNodeLookUpMutiDir<Comparable<?>>(good_context.leafNodeNumber_filaPath_map, 
//							good_context.keyType, medium_nodes_cache_length, leaf_nodes_cache_length);
					
					isAllInit = true;
					outTime(start_time, "isAllInit() time consumed: ");
				}//if(!isAllInit)
			}// synchronized
		}
		
	}
}