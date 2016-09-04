package hoolee.io;

import java.io.ByteArrayOutputStream;
/**
 * 2016.07.22
 */
public class MyByteArrayOutputStream extends ByteArrayOutputStream{
	public byte[] getByteBuf(){
		return buf;
	}
}
