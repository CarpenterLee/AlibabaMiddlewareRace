package gzx.cache.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import gzx.cache.file.Block;
import gzx.cache.file.FileManager;

/**
 * Created by gzx on 16-7-13.
 */
public class Page {
    public static final int BLOCK_SIZE = OrderSystemContext.BLOCK_SIZE;

    public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;

    public static FileManager fileMgr = FileManager.getInstance();

    public static final int STR_SIZE(int n) {
        float bytesPerChar = Charset.defaultCharset().newEncoder().maxBytesPerChar();
        return INT_SIZE + (n * (int) bytesPerChar);
    }

    private ByteBuffer contents;

    public ByteBuffer getContents() {
        return contents;
    }

    public Page() {
        contents = ByteBuffer.allocate(BLOCK_SIZE);
//        contents = ByteBuffer.allocateDirect(BLOCK_SIZE);
    }

    public synchronized int read(Block blk) throws IOException {
        return fileMgr.read(blk, contents);
    }


    public synchronized boolean getContent(int offsetInBlock, byte[] buf, int offsetInBuf, int len) {
        try {
            if (offsetInBlock > BLOCK_SIZE || offsetInBlock + len > BLOCK_SIZE) {
                return false;
            }
            contents.position(offsetInBlock);
            contents.get(buf, offsetInBuf, len);
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    public synchronized boolean setContent(int offsetInBlock, byte[] buf, int offsetInBuf, int len) {
        if (offsetInBlock > BLOCK_SIZE || offsetInBlock + len > BLOCK_SIZE) {
            return false;
        }
        contents.position(offsetInBlock);
        contents.put(buf, offsetInBuf, len);
        return true;
    }
}