package hoolee.sort;
/**
 *	将字符串表示的UUID和byte[16]相互转换的工具类
 */
public class MyUUID {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String uuid = "66b09dfc-2244-4699-bb12-2f40b3720c54";
		System.out.println(uuid);
		System.out.println(byte16ToUUID(UUIDToByte16(uuid)));
	}
	/**
	 * 将形如 66b09dfc-8a29-4699-bb12-2f40b3720c54 转换成byte[16]数组
	 */
	public static byte[] UUIDToByte16(String uuid){
//		System.out.println("uuid=" + uuid);
		byte[] bytes = new byte[16];
		int j = 0;
		char[] char_arr = uuid.toCharArray();
		for(int i=0; i<36; ){
			if(char_arr[i]=='-')
				i++;
			int a = charToInt(char_arr[i++]);
			int b = charToInt(char_arr[i++]);
			bytes[j++] = (byte)((a<<4)|b);
		}
		return bytes;
	}
	/**
	 * 将形如 babb-ddf10c07850d 转换成byte[8]数组
	 */
	public static byte[] HALFUUIDToByte8(String uuid){
		byte[] bytes = new byte[8];
		int j = 0;
		char[] char_arr = uuid.toCharArray();
		for(int i=0; i<17; ){
			if(char_arr[i]=='-')
				i++;
			int a = charToInt(char_arr[i++]);
			int b = charToInt(char_arr[i++]);
			bytes[j++] = (byte)((a<<4)|b);
		}
		return bytes;
	}
	public static String byte16ToUUID(byte[] bytes){
		StringBuilder buf = new StringBuilder(64);
		for(int i=0; i<16; i++){
			byte b = bytes[i];
			buf.append(intToChar((b&0xf0)>>>4));
			buf.append(intToChar(b&0x0f));
			if(i==3 || i==5 || i==7 || i==9)
				buf.append('-');
		}
		return buf.toString();
	}
	public static String byte16ToUUID_WithPrefix(String pre, byte[] bytes, int offset){
		StringBuilder buf = new StringBuilder(64);
		buf.append(pre).append('_');
		for(int i=0; i<16; i++){
			byte b = bytes[i+offset];
			buf.append(intToChar((b&0xf0)>>>4));
			buf.append(intToChar(b&0x0f));
			if(i==3 || i==5 || i==7 || i==9)
				buf.append('-');
		}
		return buf.toString();
	}
	public static String byte8ToHALFUUID_WithPrefix(String pre, byte[] bytes, int offset){
		StringBuilder buf = new StringBuilder(32);
		buf.append(pre).append('-');
		// babb-ddf10c07850d
		for(int i=0; i<8; i++){
			byte b = bytes[i+offset];
			buf.append(intToChar((b&0xf0)>>>4));
			buf.append(intToChar(b&0x0f));
			if(i == 1)
				buf.append('-');
		}
		return buf.toString();
	}
	public static String byte16ToUUID(byte[] bytes, int offset){
		StringBuilder buf = new StringBuilder(64);
		for(int i=0; i<16; i++){
			byte b = bytes[i+offset];
			buf.append(intToChar((b&0xf0)>>>4));
			buf.append(intToChar(b&0x0f));
			if(i==3 || i==5 || i==7 || i==9)
				buf.append('-');
		}
		return buf.toString();
	}
	private static int charToInt(char c){
		if(c>='0' && c<='9')
			return c-'0';
		if(c>='a' && c<='f')
			return c-'a'+10;
		else
			throw new RuntimeException("not a valid hex char: " + c);
	}
	private static char intToChar(int t){
		if(t>=0 && t<=9)
			return (char)(t+'0');
		if(t>=10 && t<=15)
			return (char)(t-10+'a');
		else
			throw new RuntimeException("not a valid hex value: " + t);
	}
}
