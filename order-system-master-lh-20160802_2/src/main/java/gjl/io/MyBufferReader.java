/**
 * @author gjl
 * @time 2016-7-16
 */
package gjl.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class MyBufferReader extends Reader {
    private String fileName;
    private FileInputStream fileInputStream;
    private FileChannel fChannel;
    private ByteBuffer contents;
    private long realOffset;
    private int offset;
    private int bufferSize;
    private long realSize;
    
    public MyBufferReader(File file, int bufferSize) throws IOException {
        if (bufferSize <= 0)
            throw new IllegalArgumentException("Buffer size <= 0");
        this.fileName = file.getAbsolutePath();
        this.bufferSize = bufferSize;
        this.contents = ByteBuffer.allocate(bufferSize);
        this.fileInputStream = new FileInputStream(this.fileName);
        this.fChannel = fileInputStream.getChannel();
        this.offset = 0;
        this.realOffset = 0;
        getContents();
    }
    
    private boolean getContents() throws IOException {
    	if(this.realOffset >= fChannel.size()) {
    		return false;
    	}
    	this.contents.clear();
    	if(this.realOffset + this.bufferSize < this.fChannel.size()) {
    		this.realSize = this.bufferSize;
    		this.contents = this.fChannel.map(FileChannel.MapMode.READ_ONLY, 
    						this.realOffset, this.realSize); 
    	} else {
    		this.realSize = this.fChannel.size() - this.realOffset;
    		this.contents = this.fChannel.map(FileChannel.MapMode.READ_ONLY, 
							this.realOffset, this.realSize); 
    	}
    	return true;
    }
    
    public byte[] readLine() throws IOException {
    	try {
	    	int len = getLength();
	    	if(len == -1) {
	    		return null;
	    	}
	    	byte[] result = new byte[len];
	    	this.contents.position(this.offset);
	//    	System.out.println(offset + ":" + len + ":" + result.length);
	        contents.get(result, 0, len);
	        offset += len + 1;
	        realOffset += len + 1;
	    	return result;
    	} catch (Exception e){
    		System.out.println("offset: " + offset + ", realoffset = " + realOffset);
    		e.printStackTrace();
    		return null;
    	}
    }
    
    private int getLength() throws IOException {
    	int len = 0;
    	contents.mark();
    	while(this.offset + len < this.realSize && contents.get() != '\n') {
    		len++;
    	}
    	contents.reset();
    	if(this.realSize < this.bufferSize && len > 0) {
    		return len;
    	}
    	if(this.offset + len >= this.realSize) {
    		if(getContents()) {
	            this.offset = 0;
	            int new_len = 0;
	            contents.mark();
	        	while(new_len < this.realSize && contents.get() != '\n') {
	        		new_len++;
	        	} 
	        	contents.reset();
	        	return new_len;
    		} else {
    			return -1;
    		}
    	} else {
    		return len;
    	}
    }

	@Override
	public int read(char[] cbuf, int off, int len) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		this.fChannel.close();
		this.fileInputStream.close();
	} 
}


