package gzx.cache.buffer;


import java.beans.Statement;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.alibaba.middleware.race.OrderSystem;
import gzx.cache.file.Block;

/**
 * Created by gzx on 16-7-13.
 */
public class BufferManager {

    private static String[] DiskPrefix = {
            "/disk1/",
            "/disk2/",
            "/disk3/",
    		"/home",
    		"/mnt/sdb",
    		"/mnt/sdc",
            
    };
    LinkedHashMap<Block, Buffer> bufferPool;
    private int available;
    //    private int leatestRead = 0;
    private int maxSize = 0;
    private int readTimes = 0;
    private int uncached = 0;
    //    private volatile static BufferManager manager = null;
    private volatile static HashMap<String, BufferManager> managers = new HashMap<String, BufferManager>();
//    private volatile static ConcurrentHashMap<String,BufferManager> managers = new ConcurrentHashMap<String, BufferManager>();


    private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    public static BufferManager getManager(String fileName) {
        for (int i = 0; i < DiskPrefix.length; i++) {
            if(fileName.startsWith(DiskPrefix[i])){
//            if (fileName.contains(DiskPrefix[i])) {
                if (managers.get(DiskPrefix[i]) != null) {
                    return managers.get(DiskPrefix[i]);
                } else {

//                   return managers.put(DiskPrefix[i],new BufferManager(OrderSystemContext.BUFFER_NUM));
                    synchronized (BufferManager.class) {
                        if (managers.get(DiskPrefix[i]) == null) {
                            managers.put(DiskPrefix[i], new BufferManager(OrderSystemContext.BUFFER_NUM));
//                            = new BufferManager(OrderSystemContext.BUFFER_NUM);
                        }
                        return managers.get(DiskPrefix[i]);
                    }
                }
            }
        }
        return null;

//        if (managers!= null) {
//            return manager;
//        }
//        synchronized (BufferManager.class) {
//            if (manager == null) {
//                manager = new BufferManager(OrderSystemContext.BUFFER_NUM);
//            }
//            return manager;
//        }
    }

    private BufferManager(int numbuffs) {
        maxSize = numbuffs;
        bufferPool = new MyLinkedHashMap(maxSize, maxSize);
    }

    //   public synchronized Buffer getBlockBuffer(Block blk) throws IOException {
    public Buffer getBlockBuffer(Block blk) throws IOException {
        Buffer buff = null;
//        boolean needReadNew = false;
        readTimes++;
//        synchronized (BufferManager.class) {
        rwLock.readLock().lock();
        try {
            buff = findExistingBuffer(blk);
//            if (buff != null) {
//                rwLock.readLock().unlock();
//                try {
//                    rwLock.writeLock().lock();
//                    Buffer buf = bufferPool.remove(blk);
//                    bufferPool.put(blk, buf);
//                    rwLock.writeLock().unlock();
//                }finally {
//                    rwLock.readLock().lock();
//                    rwLock.writeLock().unlock();
//                }
//            } else
        if (buff == null) {
                uncached++;
                rwLock.readLock().unlock();
                rwLock.writeLock().lock();
                try {
                    if (buff == null) {
                        buff = new Buffer();
                        bufferPool.put(blk, buff);
                    }
                } finally {
                    rwLock.readLock().lock();
                    rwLock.writeLock().unlock();
                }
//                if (bufferPool.size() < maxSize) {
//                    buff = new Buffer();
//                    bufferPool.put(blk, buff);
//                    needReadNew = true;
//                } else {
//                buff=chooseLeastUsedBuffer();
//                    Map.Entry<Block, Buffer> entry = chooseLeastUsedBuffer();
//                    bufferPool.remove(entry.getKey());
//                buff = entry.getValue();
//                    needReadNew = true;
//                }
//                if (buff == null)
//                    return null;
            }
        } finally {
            rwLock.readLock().unlock();
        }
//        }
//        buff.incReadTimes();
        if (readTimes % 100000 == 0) {
            System.out.println(" readtimes : cached : " + readTimes + " " + (readTimes - uncached) + " ratio " + ((double) (readTimes - uncached) / readTimes));
//            printReadTime();
        }
//        if (needReadNew) {
//        if(!buff.isReaded()){
        buff.assignToBlock(blk);
//        }
        return buff;
    }


    int available() {
        return available;
    }

    private Buffer findExistingBuffer(Block blk) {
        Buffer buf = bufferPool.get(blk);
//        move the block buffer to first;
//        Buffer buf = bufferPool.remove(blk);
//        if (buf != null) {
//            bufferPool.put(blk, buf);
//        }
        return buf;
    }

    private Map.Entry<Block, Buffer> chooseLeastUsedBuffer() {
//        return bufferPool.entrySet().iterator().next();
//            TODO find the last
//        return null;

//        return bufferPool.entrySet().iterator().next().getValue();
        return bufferPool.entrySet().iterator().next();
    }

    private void printReadTime() {
        for (Buffer bu :
                bufferPool.values()) {
            System.out.println(" block " + bu.getBlock().fileName() + " : " + bu.getBlock().number() + " : " + bu.getReadTimes());
        }
    }


    class MyLinkedHashMap extends LinkedHashMap<Block, Buffer> {
        int maxSize = 0;

        public MyLinkedHashMap(int initialCapacity, int maxSize) {
            super(initialCapacity);
            this.maxSize = maxSize;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<Block, Buffer> eldest) {
            return size() > maxSize;
        }

    }
}