package hoolee.index;

public class NumberUtil {

	public static void main(String[] args) {
		System.out.println("NumberUtil---");
//		for(int i=Integer.MIN_VALUE; i<0; ++i){
//			int t = byteArrayToInt(intToByteArray(i));
//			if(t != i){
//				System.out.println("fail i="+i+", t="+t);
//			}
//		}
		for(long i=-10; i<10000; ++i){
			long t = byteArrayToLong(longToByteArray(i));
			if(t != i){
				System.out.println("fail i="+i+", t="+t);
			}
		}
		System.out.println("end~~");
	}
	public static byte[] intToByteArray(int n){
		byte[] bytes = new byte[4];
		bytes[3] = (byte)n;
		bytes[2] = (byte)(n>>>8);
		bytes[1] = (byte)(n>>>16);
		bytes[0] = (byte)(n>>>24);
		return bytes;
	}
	public static int byteArrayToInt(byte[] bytes){
		int n = 0;
		for(int i=0; i<4; ++i){
			n <<= 8;
			n |= bytes[i]&0xff;
		}
		return n;
	}
	public static byte[] longToByteArray(long n){
		byte[] bytes = new byte[8];
		bytes[7] = (byte)n;
		bytes[6] = (byte)(n>>>8);
		bytes[5] = (byte)(n>>>16);
		bytes[4] = (byte)(n>>>24);
		bytes[3] = (byte)(n>>>32);
		bytes[2] = (byte)(n>>>40);
		bytes[1] = (byte)(n>>>48);
		bytes[0] = (byte)(n>>>56);
		return bytes;
	}
	public static long byteArrayToLong(byte[] bytes){
		long n = 0;
		for(int i=0; i<8; ++i){
			n <<= 8;
			n |= bytes[i]&0xff;
		}
		return n;
	}
	
}
