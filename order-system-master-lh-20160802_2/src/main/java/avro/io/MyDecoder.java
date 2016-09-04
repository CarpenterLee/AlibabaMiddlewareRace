package avro.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import gzx.cache.buffer.OrderSystemContext;

public class MyDecoder {
    private InputStream in_stream = null;
    private int bufferSize;
    public byte[] buf;
    private int pos = 0;// data begin at buf[pos]
    private int limit = 0;// data end at buf[limit-1]
    private Object in_stream_lock;
    private static final Charset utf8 = Charset.forName("utf-8");

    public MyDecoder(InputStream in_stream, int bufferSize) {
        this.in_stream = in_stream;
        this.bufferSize = bufferSize;
        this.buf = new byte[bufferSize];

    }

    public MyDecoder(InputStream in_stream) {
        this.in_stream = in_stream;
        this.bufferSize = 8;
        this.buf = new byte[8];

    }

    public MyDecoder(InputStream in_stream, int bufferSize, Object in_stream_lock) {
        this.in_stream = in_stream;
        this.bufferSize = bufferSize;
        this.buf = new byte[bufferSize];
        this.in_stream_lock = in_stream_lock;

    }

    public MyDecoder(byte[] buf, int offset, int length) {
        this.buf = buf;
        this.pos = offset;
        this.limit = offset + length;
//		System.out.println("new MyDecoder(buf, , ), pos=" + pos + ", limit=" + limit + ", buf.length=" + buf.length);
    }

    //	public void readInStream(){
//		if(in_stream == null){
//			return;
//		}
//		try{
//			this.ensureBounds(bufferSize);
//		}catch(Exception e){
//			e.printStackTrace();
//			System.exit(-1);
//		}
//	}
    public boolean readBoolean() throws IOException {
//		System.out.println("---readBoolean()---");
        ensureBounds(1);
        int n = buf[pos++] & 0xff;
        return n == 1;
    }

    public int readInt() throws IOException {
//		System.out.println("---readInt()---");
        ensureBounds(5); // won't throw index out of bounds
        int len = 1;
        int b = buf[pos] & 0xff;
        int n = b & 0x7f;
        if (b > 0x7f) {
            b = buf[pos + len++] & 0xff;
            n ^= (b & 0x7f) << 7;
            if (b > 0x7f) {
                b = buf[pos + len++] & 0xff;
                n ^= (b & 0x7f) << 14;
                if (b > 0x7f) {
                    b = buf[pos + len++] & 0xff;
                    n ^= (b & 0x7f) << 21;
                    if (b > 0x7f) {
                        b = buf[pos + len++] & 0xff;
                        n ^= (b & 0x7f) << 28;
                        if (b > 0x7f) {
                            throw new IOException("Invalid int encoding");
                        }
                    }
                }
            }
        }
        pos += len;
        if (pos > limit) {
            throw new EOFException("bufferSize=" + bufferSize + ", pos=" + pos + ", limit=" + limit);
        }
        return (n >>> 1) ^ -(n & 1); // back to two's-complement
    }

    public long readLong() throws IOException {
//		System.out.println("---readLong()---");
        ensureBounds(10);
        int b = buf[pos++] & 0xff;
        int n = b & 0x7f;
        long l;
        if (b > 0x7f) {
            b = buf[pos++] & 0xff;
            n ^= (b & 0x7f) << 7;
            if (b > 0x7f) {
                b = buf[pos++] & 0xff;
                n ^= (b & 0x7f) << 14;
                if (b > 0x7f) {
                    b = buf[pos++] & 0xff;
                    n ^= (b & 0x7f) << 21;
                    if (b > 0x7f) {
                        // only the low 28 bits can be set, so this won't carry
                        // the sign bit to the long
                        l = innerLongDecode((long) n);
                    } else {
                        l = n;
                    }
                } else {
                    l = n;
                }
            } else {
                l = n;
            }
        } else {
            l = n;
        }
        if (pos > limit) {
            throw new EOFException();
        }
        return (l >>> 1) ^ -(l & 1); // back to two's-complement
    }

    public double readDouble() throws IOException {
//		System.out.println("---readDouble()---");
        ensureBounds(8);
        int len = 1;
        int n1 = (buf[pos] & 0xff) | ((buf[pos + len++] & 0xff) << 8)
                | ((buf[pos + len++] & 0xff) << 16) | ((buf[pos + len++] & 0xff) << 24);
        int n2 = (buf[pos + len++] & 0xff) | ((buf[pos + len++] & 0xff) << 8)
                | ((buf[pos + len++] & 0xff) << 16) | ((buf[pos + len++] & 0xff) << 24);
        if ((pos + 8) > limit) {
            throw new EOFException();
        }
        pos += 8;
        return Double.longBitsToDouble((((long) n1) & 0xffffffffL)
                | (((long) n2) << 32));
    }

    private long innerLongDecode(long l) throws IOException {
        int len = 1;
        int b = buf[pos] & 0xff;
        l ^= (b & 0x7fL) << 28;
        if (b > 0x7f) {
            b = buf[pos + len++] & 0xff;
            l ^= (b & 0x7fL) << 35;
            if (b > 0x7f) {
                b = buf[pos + len++] & 0xff;
                l ^= (b & 0x7fL) << 42;
                if (b > 0x7f) {
                    b = buf[pos + len++] & 0xff;
                    l ^= (b & 0x7fL) << 49;
                    if (b > 0x7f) {
                        b = buf[pos + len++] & 0xff;
                        l ^= (b & 0x7fL) << 56;
                        if (b > 0x7f) {
                            b = buf[pos + len++] & 0xff;
                            l ^= (b & 0x7fL) << 63;
                            if (b > 0x7f) {
                                throw new IOException("Invalid long encoding");
                            }
                        }
                    }
                }
            }
        }
        pos += len;
        return l;
    }

    public String readString() throws IOException {
//		System.out.println("---readString()---");
        int length = readInt();
//		System.out.println("length=" + length);
//		byte[] str_bytes = new byte[length];
//		readFixed(str_bytes, 0, str_bytes.length);
//		return new String(str_bytes, utf8);
        return new String(buf, readFixed(length), length, utf8);
    }

    public void skipString() throws IOException {
        skipFixed(readInt());
    }

    public void readFixed(byte[] bytes) throws IOException {
//		System.out.println("---readFixed(byte[] bytes)---");
        readFixed(bytes, 0, bytes.length);
    }

    /**
     * 0拷贝的readFix()，返回在buf[]的起始地址
     */
    public int readFixed(int length) throws IOException {
//		System.out.println("---readFixed(int length)---");
        if (length < 0)
            throw new RuntimeException("Malformed data. Length is negative: " + length);
        ensureBounds(length);
        int old_pos = pos;
        pos += length;
        return old_pos;
    }

    public void readFixed(byte[] bytes, int start, int length) throws IOException {
//		System.out.println("---readFixed(byte[] bytes, int start, int length)---");
        if (length < 0)
            throw new RuntimeException("Malformed data. Length is negative: " + length);
        ensureBounds(length);
        System.arraycopy(buf, pos, bytes, start, length);
        pos += length;
    }

    public void skipFixed(int length) throws IOException {
//		System.out.println("---skipFixed(int length)---");
        ensureBounds(length);
        pos += length;
    }

    public boolean isEnd() throws IOException {
//		System.out.println("isEnd() pos=" + pos + ", limit=" + limit);
        if (pos < limit) {
            return false;
        }
        if (in_stream == null) {
            return true;
        }
        return in_stream.available() == 0;
    }

//    TODO
    public int readLineLength() throws IOException {
        int res = readInt();
        expandBuffer(res);
        return res;
    }

    private void expandBuffer(int num) {
        int remaining=limit-pos;
        if (remaining < num) {
            //           TODO
            int newBufLength = OrderSystemContext.BLOCK_SIZE > num ? OrderSystemContext.BLOCK_SIZE : num;
            byte[] newBuf = new byte[newBufLength];
            System.arraycopy(buf, pos, newBuf, 0, remaining);
            this.buf = newBuf;
            this.pos = 0;
//            this.limit = newBufLength-remaining;
            this.limit=remaining;
        }

    }

    private void ensureBounds(int num) throws IOException {
        if (in_stream == null) {
            return;
        }
        int remaining = limit - pos;
        if (remaining < num) {
            if (remaining == 0) {
                pos = 0;
                limit = in_stream.read(buf);
            } else {
                System.arraycopy(buf, pos, buf, 0, remaining);
                pos = 0;
                limit = remaining;
                int n = -1;
                if (in_stream_lock != null) {
                    synchronized (in_stream_lock) {
                        n = in_stream.read(buf, remaining, buf.length - remaining);
                    }
                } else {
                    n = in_stream.read(buf, remaining, buf.length - remaining);
                }
                if (n != -1) {
                    limit += n;
                }
            }
        }
    }
}