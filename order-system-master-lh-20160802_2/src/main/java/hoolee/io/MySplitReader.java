package hoolee.io;//package hoolee.io;
//
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.InputStreamReader;
//
//public class MySplitReader {
//	public static void main(String[] args) throws IOException{
//		File inFile = new File("/home/lh/Desktop/prerun_data/order.0.3");
//		MySplitReader reader = new MySplitReader(inFile, 26805100, 1024*1024);
//		System.out.println(reader.nextLine());
//	}
//	private BufferedReader reader;
//	
//	public MySplitReader(File inFile, long begin_offset, int buf_size){
//		try{
//			FileInputStream in_stream = new FileInputStream(inFile);
//			in_stream.getChannel().position(begin_offset);
//			reader = new BufferedReader(new InputStreamReader(in_stream), buf_size);
//		}catch(Exception e){
//			e.printStackTrace();
//			System.exit(-1);
//		}
//		
//	}
//	public String nextLine() throws IOException{
//		return reader.readLine();
//	}
//	public void close(){
//		try {
//			reader.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}
//}
