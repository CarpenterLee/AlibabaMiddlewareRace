package gzx.cache.file;

import static gzx.cache.buffer.OrderSystemContext.BLOCK_SIZE;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;


public class FileManager {
    private Map<String, FileChannel> openFiles = new HashMap<String, FileChannel>();

    private static FileManager instance = null;

    private FileManager() {
    }

    public static FileManager getInstance() {
        if(instance!=null){
            return instance;
        }
        synchronized (FileManager.class){
            if (instance == null) {
                instance = new FileManager();
            }
            return instance;
        }
    }

    public int read(String fileName, int offsetInFile, ByteBuffer bb) throws IOException {
        int readNum;
        bb.clear();
        FileChannel fc = getFile(fileName);
        readNum=fc.read(bb, offsetInFile);
        bb.rewind();
        return readNum;

    }

    public int read(Block blk, ByteBuffer bb) throws IOException {
        int readNum;
        bb.clear();
        FileChannel fc = getFile(blk.fileName());
        readNum=fc.read(bb, blk.number() * BLOCK_SIZE);
        bb.rewind();
        return readNum;
    }

    public int write(Block blk, ByteBuffer bb) throws IOException {
        int writeNum;
        bb.rewind();
        FileChannel fc = getFile(blk.fileName());
        writeNum=fc.write(bb, blk.number() * BLOCK_SIZE);
        return writeNum;
    }

    public int write(String fileName, int offsetInFile, ByteBuffer bb) throws IOException {
        int writeNum;
        bb.rewind();
        FileChannel fc = getFile(fileName);
        writeNum=fc.write(bb, offsetInFile);
        return writeNum;
    }


    public long size(String filename) throws IOException {
        FileChannel fc = getFile(filename);
        return fc.size();
    }

    private synchronized FileChannel getFile(String filename) throws IOException {
        FileChannel fc = openFiles.get(filename);
        if (fc == null) {
            File dbTable = new File(filename);
            RandomAccessFile f = new RandomAccessFile(dbTable, "r");
            fc = f.getChannel();
            openFiles.put(filename, fc);
        }
        return fc;
    }

    public synchronized boolean closeFile(String fileName) throws IOException {
        FileChannel fc = openFiles.get(fileName);
        if (fc == null) {
            return true;
        }
        fc.close();
        openFiles.remove(fileName);
        return true;
    }
}