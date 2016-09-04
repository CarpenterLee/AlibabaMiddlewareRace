package gzx.cache.buffer;


import gzx.cache.file.Block;

import java.io.IOException;

/**
 * Created by gzx on 16-7-13. 49
 */
public class Buffer {
    Block block;


    Page contents = new Page();
    private volatile boolean isReaded = false;
    public int readBytes;
    int readTimes = -1;

    public synchronized Block getBlock() {
        return block;
    }

//    public synchronized boolean isReaded() {
//        return isReaded;
//    }

    public int getReadBytes() {
        return readBytes;
    }

    public synchronized int assignToBlock(Block blk) throws IOException {
        if (isReaded == false) {
            block = blk;
            this.readBytes = contents.read(block);
            isReaded = true;
            return readBytes;
        }else {
            return readBytes;
        }
    }


    public synchronized int incReadTimes() {
        readTimes++;
        return readTimes;
    }

    public synchronized int getReadTimes() {
        return readTimes;
    }

    // used by BufferManager
    public synchronized int getInt(int offsetInBlock) {
        byte[] bytes = new byte[10];
        getContent(offsetInBlock, bytes, 0, 10);
        String s = new String(bytes);
        int idx = s.indexOf("\t");
        if (idx == -1) {
            return -1;
        }
        return Integer.parseInt(s.substring(0, idx));
    }

    public synchronized boolean getContent(int offsetInBlock, byte[] buf, int offsetInBuf, int len) {
        return contents.getContent(offsetInBlock, buf, offsetInBuf, len);
    }
}