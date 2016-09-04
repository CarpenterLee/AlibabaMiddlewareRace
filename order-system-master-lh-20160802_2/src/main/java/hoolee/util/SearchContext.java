package hoolee.util;

import hoolee.sort.SamplerHelper;
import hoolee.sort.KeysNumber;
import hoolee.sort.TypeInference;

import java.io.File;
import java.util.Map;
/**
 * 记录用于检索的上下文
 */
public class SearchContext {
	public volatile Map<Integer, File> leafNodeNumber_filaPath_map;
	public volatile Map<Integer, File> sorttedFileNumber_filePath_map;
	public volatile KeysNumber keyNumber;
	public volatile TypeInference typeInference;
	public volatile TypeInference.Type keyType;
	public volatile SamplerHelper samplerHalper;
	/**用于存储dual Offset*/
	public volatile File[][] secondary_offsetFiles;
	public volatile File[][] taggedFiles;
	
	public volatile Pair<File[][], File[][]> dual_taggedFiles;
	
}
