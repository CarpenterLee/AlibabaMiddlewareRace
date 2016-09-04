package gjl.io;///**
// * 
// * @author gjl
// * @time 2016-7-16
// */
//package gjl.io;
//
//import java.io.*;
//import java.util.*;
//import com.alibaba.middleware.OrderSystemContext;
//
//public class GJLSampler{
//	private File sampleFile;
//	private double freq;
//	private long readLine = 0;
//    private long sampleLine = 0;
//	private ArrayList<String> goodValues = new ArrayList<String>();
////	private ArrayList<String> buyerValues = new ArrayList<String>();
//	
//	public GJLSampler(File sampleFile, double freq){
//		this.sampleFile = sampleFile;
//		this.freq = freq;
//		try {
//			doSample();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//	
//	private void doSample() throws Exception{
//		MyBufferReader reader = new MyBufferReader(this.sampleFile, OrderSystemContext.sampleInputBuffer);
//		byte[] bytes;
//		while((bytes = reader.readLine()) != null){
//			++this.readLine;
////			if(sampleLine > 1000) {
////				break;
////			}
//			if ((double) this.sampleLine / this.readLine < this.freq) {// sample this line
//				boolean finish = false;
//				String line = new String(bytes);
//				for(String pairStr : line.split("\t")){
//					if(finish) {
//						break;
//					}
//					int idx = pairStr.indexOf(':');
//					String key = pairStr.substring(0, idx);
//					String value = pairStr.substring(idx + 1);
//					if(key.equals("goodid")) {
//						this.goodValues.add(value);
//						finish = true;
//					}
////					if(key.equals("buyerid")) {
////						this.buyerValues.add(value);
////						count++;
////					}
//				}
//				++this.sampleLine;
//			}
//		}
//		reader.close();
//	}
//	
//	public ArrayList<String> getGoodValues(){
//		return this.goodValues;
//	}
//	
////	public ArrayList<String> getBuyerValues(){
////		return this.buyerValues;
////	}
//}
