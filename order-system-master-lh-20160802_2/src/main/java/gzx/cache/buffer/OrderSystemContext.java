package gzx.cache.buffer;

/**
 * Created by gzx on 16-7-22.
 */
public class OrderSystemContext {
//    public static int BLOCK_SIZE=512;
	private static final int MAX_CACHE_SIZE_B = 1024*1024*800;
    public static final int BLOCK_SIZE = 4096;
    public static final int BUFFER_NUM = MAX_CACHE_SIZE_B/BLOCK_SIZE;
//    public static int BUFFER_NUM=256;
//    public static int BUFFER_NUM=20;
}