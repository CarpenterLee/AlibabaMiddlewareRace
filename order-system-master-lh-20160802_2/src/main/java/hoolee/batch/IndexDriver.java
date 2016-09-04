package hoolee.batch;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import hoolee.sort.Sampler;
import hoolee.sort.SamplerHelper;
import hoolee.sort.KeysNumber;
import hoolee.sort.TypeInference;
import hoolee.util.Pair;
import hoolee.util.SearchContext;
/**
 * 采用二进制的方式进行存储，将UUID也转换成二进制
 * 使用Sampler2
 * 使用Comparable增加了算法的通用性
 */
public class IndexDriver {
	/***
	 * 用于对非主键索引的secondary offset文件进行排序，索引。
	 */
	public SearchContext batchTaggingSortingIndexingSecondaryOffset(
			Collection<String> inFiles_clt, String[] outBaseDirs, String tagOutDirName, 
			String sortOutDirName, String offsetOutDirName, 
			String indexOutDirName, 
			String sortKey, 
			long bucket_size_in_B, 
			SamplerHelper samplerHalper, 
			final int parallelism) throws Exception{
		System.out.println("\n--------------batchTaggingSortingIndexingSecondaryOffset()-----------------");
		System.out.println("inFiles_clt=" + inFiles_clt);
		System.out.println("outBaseDirs=" + Arrays.toString(outBaseDirs));
		System.out.println("tagOutDirName=" + tagOutDirName);
		System.out.println("sortOutDirName=" + sortOutDirName);
		System.out.println("offsetOutDirName=" + offsetOutDirName);
		System.out.println("indexOutDirName=" + indexOutDirName);
		System.out.println("bucket_size_in_B=" + bucket_size_in_B);
		System.out.println("parallelism=" + parallelism);
		
		final int assumed_line_size = 200;
		System.out.println("assumed_line_size=" + assumed_line_size);
		long begin_time = System.currentTimeMillis();
		File[] inFiles = new File[inFiles_clt.size()];
		int t0 = 0;
		for(String f : inFiles_clt){
			inFiles[t0++] = new File(f);
		}
		long startTime = System.currentTimeMillis();
		final long lines_per_bucket = bucket_size_in_B/assumed_line_size;
		final int buckets = (int)(samplerHalper.getLinesReadCount()/lines_per_bucket)+1;
		System.out.println("Secondary buckets=" + buckets);
		KeysNumber keyNumber = new KeysNumber(samplerHalper.getKeysValuesMap(), 10000);
		TypeInference typeInference = new TypeInference(samplerHalper.getKeysValuesMap());
		System.out.println("typeInference=\n" + typeInference);
		startTime = System.currentTimeMillis();
		System.out.println("...tagging...");
		Tagging tagging = new Tagging();
		Comparable<Object>[] tags = tagging.getTags(
				samplerHalper.getValues(sortKey).toArray(new String[0]), 
				typeInference.getType(sortKey), buckets);
		File[] tagOutDirs = new File[outBaseDirs.length];
		for(int i=0; i<tagOutDirs.length; ++i){
			tagOutDirs[i] = new File(outBaseDirs[i]+"/"+tagOutDirName);
		}
		File[][] taggedFiles = tagging.tagSecondaryOffsetFiles(
				inFiles, tags, tagOutDirs, typeInference.getType(sortKey), parallelism);// tagging
		outTime(startTime, "tagging time consumed");
		startTime = System.currentTimeMillis();
		System.out.println("...sorting...");
		Pair<Map<Integer, File>, File[][]> sort_result =
				new Sorting().sortSecondaryOffsetFiles(
					taggedFiles, sortOutDirName, offsetOutDirName, typeInference.getType(sortKey), parallelism);// sorting
		Map<Integer, File> sorttedFileNumber_filePath_map = sort_result.key;
		File[][] offsetFiles = sort_result.value;
		outTime(startTime, "sorting time consumed:");
		startTime = System.currentTimeMillis();
		System.out.println("...indexing...");
		File offsetSizeFile = new File(offsetFiles[0][0].getParent(), "0.size");
		Map<Integer, File> leafNodeNumber_filaPath_map = 
				new Indexing().indexing(
					offsetFiles, offsetSizeFile, indexOutDirName, typeInference.getType(sortKey), parallelism);// indexing
		outTime(startTime, "indexing time consumed:");
		
		outTotalFileSize(leafNodeNumber_filaPath_map, "BTree_leaf_node_total_size_in_B:");
		long in_file_total_size = outTotalFileSize(inFiles, "inFiles_total_size_in_B:");
		long sortted_file_total_size = outTotalFileSize(sorttedFileNumber_filePath_map, "all_sortted_file_total_size_in_B:");
		double compress_ratio = 100.0*sortted_file_total_size/in_file_total_size;
//		System.out.println("compress_ratio=" + compress_ratio + "%");
		System.out.println(String.format("compress_ratio=%.2f%%", compress_ratio));
		
		outTime(begin_time, "all time consumed:");
		SearchContext context = new SearchContext();
		context.leafNodeNumber_filaPath_map = leafNodeNumber_filaPath_map;
		context.sorttedFileNumber_filePath_map = sorttedFileNumber_filePath_map;
		context.keyNumber = keyNumber;
		context.typeInference = typeInference;
		context.keyType = typeInference.getType(sortKey);
		context.samplerHalper = samplerHalper;
		return context;
	}
	/**
	 * 在排序过程中会生成两种offset文件。
	 * @param inFiles_clt
	 * @param outBaseDirs
	 * @param tagOutDirName
	 * @param sortOutDirName
	 * @param offsetOutDirName_primary
	 * @param offsetOutDirName_secondary
	 * @param indexOutDirName
	 * @param sortKey
	 * @param sedcondKey 指定第二种offset的key
	 * @param bucket_size_in_B
	 * @param freq
	 * @param samplerHalper
	 * @return
	 * @throws Exception
	 */
	public SearchContext batchTaggingSortingIndexingDualOffset(
			Collection<String> inFiles_clt, String[] outBaseDirs, 
			String tagOutDirName, 
			String sortOutDirName, String offsetOutDirName_primary, 
			String offsetOutDirName_secondary, 
			String indexOutDirName, String sortKey, String secondKey,  
			long bucket_size_in_B, double freq, 
			SamplerHelper samplerHalper, KeysNumber keyNumber, TypeInference typeInference, 
			File[][] taggedFiles,
			final int parallelism) throws Exception{
		System.out.println("\n--------------batchTaggingSortingIndexingDualOffset()-----------------");
		System.out.println("inFiles=" + inFiles_clt);
		System.out.println("outBaseDirs=" + Arrays.toString(outBaseDirs));
		System.out.println("tagOutDirName=" + tagOutDirName);
		System.out.println("sortOutDirName=" + sortOutDirName);
		System.out.println("offsetOutDirName=" + offsetOutDirName_primary);
		System.out.println("indexOutDirName=" + indexOutDirName);
		System.out.println("sortKey=" + sortKey);
		System.out.println("bucket_size_in_B=" + bucket_size_in_B);
		System.out.println("freq=" + freq);
		System.out.println("parallelism=" + parallelism);
		
		long begin_time = System.currentTimeMillis();
		File[] inFiles = new File[inFiles_clt.size()];
		int t0 = 0;
		for(String f : inFiles_clt){
			inFiles[t0++] = new File(f);
		}
		long startTime = System.currentTimeMillis();
		if(samplerHalper != null){
			System.out.println("sampled before, skip sampling...");
		}else{
			System.out.println("...sampling...");
			ArrayList<Sampler> samplers = new Sampling().sampleAll_Interval(inFiles, freq, parallelism);// sampling
//			ArrayList<Sampler> samplers = new Sampling().sampleAll_Split(inFiles, 5000, parallelism);// sampling
			samplerHalper = new SamplerHelper(samplers);
			keyNumber = new KeysNumber(samplerHalper.getKeysValuesMap(), 10000);
			typeInference = new TypeInference(samplerHalper.getKeysValuesMap());
			samplers = null;
			System.out.println("all_lines_read_count=" + samplerHalper.getLinesReadCount());
			System.out.println("lines_sampled_count=" + samplerHalper.getLinesSampledCount());
			outTime(startTime, "sampling time consumed:");
		}
		if(taggedFiles != null){
			System.out.println("tagged before, skip tagging...");
		}else{
			final long lines_per_bucket = bucket_size_in_B/200;// assume 200B per line
			final int buckets = (int)(samplerHalper.getLinesReadCount()/lines_per_bucket)+1;
			System.out.println("buckets=" + buckets);
			System.out.println("typeInference=\n" + typeInference);
			startTime = System.currentTimeMillis();
			System.out.println("...tagging...");
			Tagging tagging = new Tagging();
			Comparable<Object>[] tags = tagging.getTags(
					samplerHalper.getValues(sortKey).toArray(new String[0]), 
					typeInference.getType(sortKey), buckets);
			File[] tagOutDirs = new File[outBaseDirs.length];
			for(int i=0; i<tagOutDirs.length; ++i){
				tagOutDirs[i] = new File(outBaseDirs[i]+"/"+tagOutDirName);
			}
			taggedFiles = tagging.tagOrdinaryFiles(
					inFiles, tags, tagOutDirs, sortKey, keyNumber, typeInference, parallelism);// tagging
//			taggedFiles = tagging.tagOrdinaryFiles_SequenceKey(
//					inFiles, tags, tagOutDirs, sortKey, keyNumber, typeInference, parallelism);// tagging
			outTime(startTime, "tagging time consumed");
		}
		startTime = System.currentTimeMillis();
		System.out.println("...sorting...");
		Pair<ArrayList<Map<Integer, File>>, ArrayList<File[][]>> sort_result_dualOffset = // sorting
				new Sorting().sortWithDualOffsetOut(taggedFiles, sortOutDirName, offsetOutDirName_primary, 
						sortKey, offsetOutDirName_secondary, secondKey, keyNumber, typeInference, parallelism);
		
		Map<Integer, File> sorttedFileNumber_filePath_map = sort_result_dualOffset.key.get(0);
		File[][] offsetFiles = sort_result_dualOffset.value.get(0);
		outTime(startTime, "sorting time consumed:");
		startTime = System.currentTimeMillis();
		System.out.println("...indexing...");
		File offsetSizeFile = new File(offsetFiles[0][0].getParent(), "0.size");
		Map<Integer, File> leafNodeNumber_filaPath_map = 
				new Indexing().indexing(offsetFiles, offsetSizeFile, 
					indexOutDirName, typeInference.getType(sortKey), parallelism);// indexing
		outTime(startTime, "indexing time consumed:");

		outTotalFileSize(leafNodeNumber_filaPath_map, "BTree_leaf_node_total_size_in_B:");
		outTotalFileSize(sort_result_dualOffset.value.get(1), "secondary_offsetFiles_total_size_in_B:");
		long in_file_total_size = outTotalFileSize(inFiles, "inFiles_total_size_in_B:");
		long sortted_file_total_size = outTotalFileSize(sorttedFileNumber_filePath_map, "all_sortted_file_total_size_in_B:");
		double compress_ratio = 100.0*sortted_file_total_size/in_file_total_size;
//		System.out.println("compress_ratio=" + compress_ratio + "%");
		System.out.println(String.format("compress_ratio=%.2f%%", compress_ratio));
		
		outTime(begin_time, "all time consumed:");
		SearchContext context = new SearchContext();
		context.leafNodeNumber_filaPath_map = leafNodeNumber_filaPath_map;
		context.sorttedFileNumber_filePath_map = sorttedFileNumber_filePath_map;
//		context.secondaryOffsetFileNumber_filePath_map = sort_result_dualOffset.key.get(1);
		context.keyNumber = keyNumber;
		context.typeInference = typeInference;
		context.keyType = typeInference.getType(sortKey);
		context.samplerHalper = samplerHalper;
		context.secondary_offsetFiles = sort_result_dualOffset.value.get(1);
		context.taggedFiles = taggedFiles;
		return context;
	}
	/***
	 * 用于对正常文件的处理
	 */
	public SearchContext batchTaggingSortingIndexing(
			Collection<String> inFiles_clt, String[] outBaseDirs, String tagOutDirName, 
			String sortOutDirName, String offsetOutDirName, 
			String indexOutDirName, String sortKey, 
			final long bucket_size_in_B, final int assumed_line_size, 
			double freq, 
			SamplerHelper samplerHalper, KeysNumber keyNumber, TypeInference typeInference,
			File[][] taggedFiles,
			final int parallelism) throws Exception{
		System.out.println("\n--------------batchTaggingSortingIndexing()-----------------");
		System.out.println("inFiles=" + inFiles_clt);
		System.out.println("outBaseDirs=" + Arrays.toString(outBaseDirs));
		System.out.println("tagOutDirName=" + tagOutDirName);
		System.out.println("sortOutDirName=" + sortOutDirName);
		System.out.println("offsetOutDirName=" + offsetOutDirName);
		System.out.println("indexOutDirName=" + indexOutDirName);
		System.out.println("sortKey=" + sortKey);
		System.out.println("bucket_size_in_B=" + bucket_size_in_B);
		System.out.println("assumed_line_size=" + assumed_line_size);
		System.out.println("freq=" + freq);
		System.out.println("parallelism=" + parallelism);
		
		long begin_time = System.currentTimeMillis();
//		File[] inFiles = inFiles_clt.toArray(new File[0]);
		File[] inFiles = new File[inFiles_clt.size()];
		int t0 = 0;
		for(String f : inFiles_clt){
			inFiles[t0++] = new File(f);
		}
		long startTime = System.currentTimeMillis();
		if(samplerHalper != null){
			System.out.println("sampled before, skip sampling...");
		}else{
			System.out.println("...sampling...");
			ArrayList<Sampler> samplers = new Sampling().sampleAll_Interval(inFiles, freq, parallelism);// sampling
//			ArrayList<Sampler> samplers = new Sampling().sampleAll_Split(inFiles, 5000, parallelism);// sampling
			samplerHalper = new SamplerHelper(samplers);
			keyNumber = new KeysNumber(samplerHalper.getKeysValuesMap(), 10000);
			typeInference = new TypeInference(samplerHalper.getKeysValuesMap());
			samplers = null;
			System.out.println("all_lines_read_count=" + samplerHalper.getLinesReadCount());
			System.out.println("lines_sampled_count=" + samplerHalper.getLinesSampledCount());
			outTime(startTime, "sampling time consumed:");
		}
		if(taggedFiles != null){
			System.out.println("tagged before, skip tagging...");
		}else{
			final long lines_per_bucket = bucket_size_in_B/assumed_line_size;
			final int buckets = (int)(samplerHalper.getLinesReadCount()/lines_per_bucket)+1;
			System.out.println("buckets=" + buckets);
			System.out.println("typeInference=\n" + typeInference);
			startTime = System.currentTimeMillis();
			System.out.println("...tagging...");
			Tagging tagging = new Tagging();
			Comparable<Object>[] tags = tagging.getTags(
					samplerHalper.getValues(sortKey).toArray(new String[0]), 
					typeInference.getType(sortKey), buckets);
			File[] tagOutDirs = new File[outBaseDirs.length];
			for(int i=0; i<tagOutDirs.length; ++i){
				tagOutDirs[i] = new File(outBaseDirs[i]+"/"+tagOutDirName);
			}
			taggedFiles = tagging.tagOrdinaryFiles(inFiles, tags, tagOutDirs, 
					sortKey, keyNumber, typeInference, parallelism);// tagging
//			taggedFiles = tagging.tagOrdinaryFiles_SequenceKey(inFiles, tags, tagOutDirs, 
//					sortKey, keyNumber, typeInference, parallelism);// tagging
			outTime(startTime, "tagging time consumed");
		}
		startTime = System.currentTimeMillis();
		System.out.println("...sorting...");
		Pair<Map<Integer, File>, File[][]> sort_result =
				new Sorting().sortTaggedFiles(taggedFiles, sortOutDirName, offsetOutDirName, 
						sortKey, keyNumber, typeInference, parallelism);// sorting
		Map<Integer, File> sorttedFileNumber_filePath_map = sort_result.key;
		File[][] offsetFiles = sort_result.value;
		outTime(startTime, "sorting time consumed:");
		startTime = System.currentTimeMillis();
		System.out.println("...indexing...");
		File offsetSizeFile = new File(offsetFiles[0][0].getParent(), "0.size");
		Map<Integer, File> leafNodeNumber_filaPath_map = 
				new Indexing().indexing(offsetFiles, offsetSizeFile, indexOutDirName, 
						typeInference.getType(sortKey), parallelism);// indexing
		outTime(startTime, "indexing time consumed:");
		
		outTotalFileSize(leafNodeNumber_filaPath_map, "BTree_leaf_node_total_size_in_B:");
		long in_file_total_size = outTotalFileSize(inFiles, "inFiles_total_size_in_B:");
		long sortted_file_total_size = outTotalFileSize(sorttedFileNumber_filePath_map, "all_sortted_file_total_size_in_B:");
		double compress_ratio = 100.0*sortted_file_total_size/in_file_total_size;
//		System.out.println("compress_ratio=" + compress_ratio + "%");
		System.out.println(String.format("compress_ratio=%.2f%%", compress_ratio));
		
		outTime(begin_time, "all time consumed:");
		SearchContext context = new SearchContext();
		context.leafNodeNumber_filaPath_map = leafNodeNumber_filaPath_map;
		context.sorttedFileNumber_filePath_map = sorttedFileNumber_filePath_map;
		context.keyNumber = keyNumber;
		context.typeInference = typeInference;
		context.keyType = typeInference.getType(sortKey);
		context.samplerHalper = samplerHalper;
		return context;
	}
	/**二级排序*/
	public SearchContext batchTaggingSecondarySortingIndexing(
			Collection<String> inFiles_clt, String[] outBaseDirs, String tagOutDirName, 
			String sortOutDirName, String offsetOutDirName, 
			String indexOutDirName, String sortKey, String secondary_key, 
			long bucket_size_in_B, double freq, 
			SamplerHelper samplerHalper, KeysNumber keyNumber, TypeInference typeInference,
			File[][] taggedFiles,
			final int parallelism) throws Exception{
		System.out.println("\n--------------batchTaggingSecondarySortingIndexing()-----------------");
		System.out.println("inFiles=" + inFiles_clt);
		System.out.println("outBaseDirs=" + Arrays.toString(outBaseDirs));
		System.out.println("tagOutDirName=" + tagOutDirName);
		System.out.println("sortOutDirName=" + sortOutDirName);
		System.out.println("offsetOutDirName=" + offsetOutDirName);
		System.out.println("indexOutDirName=" + indexOutDirName);
		System.out.println("sortKey=" + sortKey);
		System.out.println("bucket_size_in_B=" + bucket_size_in_B);
		System.out.println("freq=" + freq);
		System.out.println("parallelism=" + parallelism);
		
		long begin_time = System.currentTimeMillis();
		File[] inFiles = new File[inFiles_clt.size()];
		int t0 = 0;
		for(String f : inFiles_clt){
			inFiles[t0++] = new File(f);
		}
		long startTime = System.currentTimeMillis();
		if(samplerHalper != null){
			System.out.println("sampled before, skip sampling...");
		}else{
			System.out.println("...sampling...");
			ArrayList<Sampler> samplers = new Sampling().sampleAll_Interval(inFiles, freq, parallelism);// sampling
//			ArrayList<Sampler> samplers = new Sampling().sampleAll_Split(inFiles, 5000, parallelism);// sampling
			samplerHalper = new SamplerHelper(samplers);
			keyNumber = new KeysNumber(samplerHalper.getKeysValuesMap(), 10000);
			typeInference = new TypeInference(samplerHalper.getKeysValuesMap());
			samplers = null;
			System.out.println("all_lines_read_count=" + samplerHalper.getLinesReadCount());
			System.out.println("lines_sampled_count=" + samplerHalper.getLinesSampledCount());
			outTime(startTime, "sampling time consumed:");
		}
		if(taggedFiles != null){
			System.out.println("tagged before, skip tagging...");
		}else{
			final long lines_per_bucket = bucket_size_in_B/200;// asume 200B per line
			final int buckets = (int)(samplerHalper.getLinesReadCount()/lines_per_bucket)+1;
			System.out.println("buckets=" + buckets);
			System.out.println("typeInference=\n" + typeInference);
			startTime = System.currentTimeMillis();
			System.out.println("...tagging...");
			Tagging tagging = new Tagging();
			Comparable<Object>[] tags = tagging.getTags(
					samplerHalper.getValues(sortKey).toArray(new String[0]), 
					typeInference.getType(sortKey), buckets);
			File[] tagOutDirs = new File[outBaseDirs.length];
			for(int i=0; i<tagOutDirs.length; ++i){
				tagOutDirs[i] = new File(outBaseDirs[i]+"/"+tagOutDirName);
			}
			taggedFiles = tagging.tagOrdinaryFiles(inFiles, tags, tagOutDirs, 
					sortKey, keyNumber, typeInference, parallelism);// tagging
//			taggedFiles = tagging.tagOrdinaryFiles_SequenceKey(inFiles, tags, tagOutDirs, 
//					sortKey, keyNumber, typeInference, parallelism);// tagging
			outTime(startTime, "tagging time consumed");
		}
		startTime = System.currentTimeMillis();
		System.out.println("...sorting...");
		Pair<Map<Integer, File>, File[][]> sort_result =
				new Sorting().secondarySortTaggedFiles(taggedFiles, sortOutDirName, offsetOutDirName, 
						sortKey, secondary_key, keyNumber, typeInference, parallelism);// sorting
		Map<Integer, File> sorttedFileNumber_filePath_map = sort_result.key;
		File[][] offsetFiles = sort_result.value;
		outTime(startTime, "sorting time consumed:");
		startTime = System.currentTimeMillis();
		System.out.println("...indexing...");
		File offsetSizeFile = new File(offsetFiles[0][0].getParent(), "0.size");
		Map<Integer, File> leafNodeNumber_filaPath_map = 
				new Indexing().indexing(offsetFiles, offsetSizeFile, indexOutDirName, 
						typeInference.getType(sortKey), parallelism);// indexing
		outTime(startTime, "indexing time consumed:");
		
		outTotalFileSize(leafNodeNumber_filaPath_map, "BTree_leaf_node_total_size_in_B:");
		long in_file_total_size = outTotalFileSize(inFiles, "inFiles_total_size_in_B:");
		long sortted_file_total_size = outTotalFileSize(sorttedFileNumber_filePath_map, "all_sortted_file_total_size_in_B:");
		double compress_ratio = 100.0*sortted_file_total_size/in_file_total_size;
		System.out.println(String.format("compress_ratio=%.2f%%", compress_ratio));
		
		outTime(begin_time, "all time consumed:");
		SearchContext context = new SearchContext();
		context.leafNodeNumber_filaPath_map = leafNodeNumber_filaPath_map;
		context.sorttedFileNumber_filePath_map = sorttedFileNumber_filePath_map;
		context.keyNumber = keyNumber;
		context.typeInference = typeInference;
		context.keyType = typeInference.getType(sortKey);
		context.samplerHalper = samplerHalper;
		return context;
	}
	/**读一次，按照tags_a, tags_b分类两次。只会用于order table。
	 * @return 返回分类之后的文件，以及采样和类型推断等信息
	 * */
	public SearchContext batch_DualTagging(
			Collection<String> inFiles_clt, 
			String[] outBaseDirs_a, String[] outBaseDirs_b, 
			String tagOutDirName_a, String tagOutDirName_b, 
			String taggedBy_a, String taggedBy_b, 
			long bucket_size_in_B, double freq, final int assumed_line_size_in_B, 
			SamplerHelper samplerHalper, KeysNumber keyNumber, TypeInference typeInference,
			final int parallelism) throws Exception{
		System.out.println("\n--------------batch_DualTagging()-----------------");
		System.out.println("inFiles_clt=" + inFiles_clt);
		System.out.println("outBaseDirs_a=" + Arrays.toString(outBaseDirs_a));
		System.out.println("outBaseDirs_b=" + Arrays.toString(outBaseDirs_b));
		System.out.println("tagOutDirName_a=" + tagOutDirName_a);
		System.out.println("tagOutDirName_b=" + tagOutDirName_b);
		System.out.println("taggedBy_a=" + taggedBy_a);
		System.out.println("taggedBy_b=" + taggedBy_b);
		System.out.println("bucket_size_in_B=" + bucket_size_in_B);
		System.out.println("freq=" + freq);
		
		File[] inFiles = new File[inFiles_clt.size()];
		int t0 = 0;
		for(String f : inFiles_clt){
			inFiles[t0++] = new File(f);
		}
		long startTime = System.currentTimeMillis();
		if(samplerHalper != null){
			System.out.println("sampled before, skip sampling...");
		}else{
			System.out.println("...sampling...");
//			ArrayList<Sampler> samplers = new Sampling().sampleAll_Interval(inFiles, freq, parallelism);//interval sampling
			ArrayList<Sampler> samplers = new Sampling().sampleAll_Split(inFiles, 
					freq, assumed_line_size_in_B, parallelism);//split sampling
			samplerHalper = new SamplerHelper(samplers);
			keyNumber = new KeysNumber(samplerHalper.getKeysValuesMap(), 10000);
			typeInference = new TypeInference(samplerHalper.getKeysValuesMap());
			samplers = null;
			System.out.println("all_lines_read_count=" + samplerHalper.getLinesReadCount());
			System.out.println("lines_sampled_count=" + samplerHalper.getLinesSampledCount());
			outTime(startTime, "sampling time consumed:");
		}
		final long lines_per_bucket = bucket_size_in_B/assumed_line_size_in_B;
		final int buckets = (int)(samplerHalper.getLinesReadCount()/lines_per_bucket)+1;
		
		System.out.println("buckets=" + buckets);
		System.out.println("typeInference=\n" + typeInference);
		startTime = System.currentTimeMillis();
		System.out.println("...tagging...");
		Tagging tagging = new Tagging();
		Comparable<Object>[] tags_b = tagging.getTags(
				samplerHalper.getValues(taggedBy_b).toArray(new String[0]), 
				typeInference.getType(taggedBy_b), buckets);
		Comparable<Object>[] tags_a = tagging.getTags(
				samplerHalper.getValues(taggedBy_a).toArray(new String[0]), 
				typeInference.getType(taggedBy_a), buckets);
//		System.out.println("tags_a=" + Arrays.toString(tags_a));
//		System.out.println("tags_b=" + Arrays.toString(tags_b));
//		if(true){
//			System.exit(-1);
//		}
		
		File[] tagOutDirs_b = new File[outBaseDirs_b.length];
		for(int i=0; i<tagOutDirs_b.length; ++i){
			tagOutDirs_b[i] = new File(outBaseDirs_b[i]+"/"+tagOutDirName_b);
		}
		File[] tagOutDirs_a = new File[outBaseDirs_a.length];
		for(int i=0; i<tagOutDirs_a.length; ++i){
			tagOutDirs_a[i] = new File(outBaseDirs_a[i]+"/"+tagOutDirName_a);
		}
		
		Pair<File[][], File[][]> taggedResult = tagging.tagOrdinaryFiles_DualTag(inFiles, tags_a, tags_b, 
				tagOutDirs_a, tagOutDirs_b, taggedBy_a, taggedBy_b, 
				keyNumber, typeInference, parallelism);// tagging
		outTime(startTime, "dual tagging time consumed");
		startTime = System.currentTimeMillis();
		SearchContext context = new SearchContext();
//		context.leafNodeNumber_filaPath_map = leafNodeNumber_filaPath_map;
//		context.sorttedFileNumber_filePath_map = sorttedFileNumber_filePath_map;
		context.keyNumber = keyNumber;
		context.typeInference = typeInference;
//		context.keyType = typeInference.getType(sortKey);
		context.samplerHalper = samplerHalper;
		context.dual_taggedFiles = taggedResult;
		
		return context;
//		return taggedResult;
	}
	private void outTime(long startTime, String note){
		long interval = System.currentTimeMillis()-startTime;
		long minute = interval/1000/60;
		double seconds = (double)(interval%(1000*60))/1000;
		System.out.println(note + " " + minute + " minutes, " + String.format("%2f seconds", seconds));
		
	}
	private long outTotalFileSize(File[] inFiles, String note){
		long total_size = 0;
		for(File f : inFiles){
			total_size += f.length();
		}
		System.out.println(note + " " + total_size);
		return total_size;
	}
	private long outTotalFileSize(Map<Integer, File> fileNumber_filaPath, String note){
		long total_size = 0;
		for(Map.Entry<Integer, File> entry : fileNumber_filaPath.entrySet()){
			total_size += entry.getValue().length();
		}
		System.out.println(note + " " + total_size);
		return total_size;
	}
	private long outTotalFileSize(File[][] inFiles, String note){
		long total_size = 0;
		for(int i=0; i<inFiles.length; i++){
			for(int j=0; j<inFiles[i].length; j++){
				total_size += inFiles[i][j].length();
			}
		}
		System.out.println(note + " " + total_size);
		return total_size;
	}
}






