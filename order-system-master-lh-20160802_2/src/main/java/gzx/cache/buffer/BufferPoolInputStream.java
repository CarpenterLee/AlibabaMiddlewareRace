package gzx.cache.buffer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import gzx.cache.file.Block;
import gzx.cache.file.FileManager;

/**
 * Created by gzx on 16-7-22.
 */
public class BufferPoolInputStream extends InputStream {
    private String fileName;
    BufferManager bufferManager;
    long offset = 0;

    long fileSize = 0;

    public BufferPoolInputStream(String fileName) {
        this.fileName = fileName;
        bufferManager = BufferManager.getManager(fileName);
        try {
            fileSize = FileManager.getInstance().size(fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public BufferPoolInputStream(File file) {
        this(file.getAbsolutePath());
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    private int readOneBlock(byte[] b, int off, int len) throws IOException {
        try {

            long blkNum = offset / OrderSystemContext.BLOCK_SIZE;
            // TODO  cast long to int
            int offsetInBlock = (int) (offset % OrderSystemContext.BLOCK_SIZE);
            Buffer buffer = bufferManager.getBlockBuffer(new Block(fileName, blkNum));
            int res = 0;
            if (buffer.getReadBytes() == -1) {
                return -1;
            }
            if (buffer.getReadBytes() >= offsetInBlock + len) {
                res = len;
                buffer.getContent(offsetInBlock, b, off, len);
                offset += len;
            } else if (buffer.getReadBytes() < offsetInBlock + len) {
                res = buffer.getReadBytes() - offsetInBlock;
                buffer.getContent(offsetInBlock, b, off, res);
                offset += res;
            }

            return res;
        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }

    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {

        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || off + len > b.length) {
            throw new IndexOutOfBoundsException();
        }
        if (offset >= fileSize) {
            return -1;
        }
        long nextBound = ((offset + OrderSystemContext.BLOCK_SIZE) / OrderSystemContext.BLOCK_SIZE) * OrderSystemContext.BLOCK_SIZE;
        if (offset + len < nextBound) {
//            read only one block
            return readOneBlock(b, off, len);

        } else {
            //TODO cast long to int

            int res = 0;
            long shouldRead = nextBound - offset;
            int actuallyRead = readOneBlock(b, off, (int) shouldRead);
            if (actuallyRead < shouldRead) {
                return actuallyRead;
            }
            off += actuallyRead;
            len = len - actuallyRead;
            res += actuallyRead;
            while (len > OrderSystemContext.BLOCK_SIZE) {
                actuallyRead = readOneBlock(b, off, OrderSystemContext.BLOCK_SIZE);
                if (actuallyRead < OrderSystemContext.BLOCK_SIZE) {
                    res += actuallyRead;
                    return res;
                }
                off += OrderSystemContext.BLOCK_SIZE;
                len -= OrderSystemContext.BLOCK_SIZE;
                res += OrderSystemContext.BLOCK_SIZE;
            }
            actuallyRead = readOneBlock(b, off, len);
            res += actuallyRead;
            return res;

        }
    }

    @Override
    public int read() throws IOException {
        byte[] bytes = new byte[10];
        int res = read(bytes);
        if (res == -1) {
            return -1;
        } else {
            return (int) bytes[0];
        }
    }

    @Override
    public long skip(long n) throws IOException {
        if (n < 0) {
            return 0;
        }
        long res;
        if (offset + n > fileSize) {
            res = fileSize - offset;
            offset = fileSize;
            return res;
        }
        offset += n;
        return n;
    }


    @Override
    public int available() throws IOException {
//        throw new RuntimeException("no suported function avaiable!");
        return (int) (fileSize - offset);
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public synchronized void mark(int readlimit) {
        throw new RuntimeException("no suported function avaiable!");
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new RuntimeException("no suported function avaiable!");
    }

    @Override
    public boolean markSupported() {
        return false;
    }


    public long postion(long offset) {
        if (offset >= fileSize) {
            this.offset = fileSize;
        } else {
            this.offset = offset;
        }
        return offset;
    }

    public boolean isFileEnd() {
        return offset >= fileSize;
    }
}