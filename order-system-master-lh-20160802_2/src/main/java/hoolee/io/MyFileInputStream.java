package hoolee.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**inputStream with lock*/
public class MyFileInputStream extends InputStream {
	private FileInputStream in;
	private Object lock;
	
	public MyFileInputStream(File inFile, Object lock){
		try {
			this.in = new FileInputStream(inFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		this.lock = lock;
	}
	
	@Override
	public int read() throws IOException {
		synchronized(lock){
			return in.read();
		}
	}

	@Override
	public int read(byte[] b) throws IOException {
		synchronized(lock){
			return in.read(b);
		}
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		synchronized(lock){
			return in.read(b, off, len);
		}
	}

	@Override
	public long skip(long n) throws IOException {
		synchronized(lock){
			return in.skip(n);
		}
	}

	@Override
	public int available() throws IOException {
		synchronized(lock){
			return in.available();
		}
	}

	@Override
	public void close() throws IOException {
		synchronized(lock){
			in.close();
		}
	}

	@Override
	public synchronized void mark(int readlimit) {
		synchronized(lock){
			in.mark(readlimit);
		}
	}

	@Override
	public synchronized void reset() throws IOException {
		synchronized(lock){
			in.reset();
		}
	}

	@Override
	public boolean markSupported() {
		return in.markSupported();
	}


}
