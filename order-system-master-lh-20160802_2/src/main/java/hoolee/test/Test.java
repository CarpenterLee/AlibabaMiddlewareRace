package hoolee.test;

import java.nio.ByteBuffer;
import sun.nio.ch.DirectBuffer;

@SuppressWarnings("restriction")
public class Test {

	public static void main(String[] args) throws Exception{
		ByteBuffer buf = ByteBuffer.allocateDirect(1024*1024);
		((DirectBuffer)buf).cleaner().clean();
		System.out.println("end~~");
	}
}
