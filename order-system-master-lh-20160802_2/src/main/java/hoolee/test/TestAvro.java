package hoolee.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import avro.io.BinaryDecoder;
import avro.io.BufferedBinaryEncoder;


public class TestAvro {

	public static void main(String[] args) throws Exception{
		File encoderin = new File("order_records.txt");
		File encoderout = new File("avroout.bin");
		File decoderout = new File("decoderout.txt");
		TestAvro test = new TestAvro();
		test.encoder(encoderin, encoderout, decoderout);
		System.out.println("end~~");
//		int min = 0x80000000;
//		System.out.println(min == Integer.MIN_VALUE);
	}
	long min_orderid = Long.MAX_VALUE;
	long min_createtime = Long.MAX_VALUE;
	public void decoder(File in, File decoderout, HashMap<Integer, String> number2key) throws Exception{
		FileInputStream in_tream = new FileInputStream(in);
		BinaryDecoder decoder = new BinaryDecoder(in_tream, 1024*64);
//		decoder.readFixed(bytes);
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(decoderout)));
		try {
			int keynumber;
			while(true){
				while((keynumber=decoder.readInt()) != 0){
					String key = number2key.get(keynumber);
					writer.append(key);
					writer.append(":");
					if(key.equals("orderid")){
						long value = decoder.readLong();
						writer.append(String.valueOf(value+min_orderid));
					}else if(key.equals("createtime")){
						long value = decoder.readLong();
						writer.append(String.valueOf(value+min_createtime));
					}else{
						String value = decoder.readString();
						writer.append(value);
					}
					writer.append('\t');
				}
				writer.newLine();
			}
		} catch (IOException e) {
			System.out.println("end of decode!!");
		}
		in_tream.close();
		writer.close();
		
	}
	/**
	 * 目前能够压缩到原来的63%左右。 
	 */
	public void encoder(File encoderin, File encoderout, File decoderout) throws Exception{
		HashMap<String, Integer> key2number = key2Number(encoderin);
//		for(Map.Entry<String, Integer> entry : key2number.entrySet()){
//			System.out.println(entry.getKey() + "->" + entry.getValue());
//		}
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(encoderin)));
		String line;
		FileOutputStream encoderout_stream = new FileOutputStream(encoderout);
		BufferedBinaryEncoder encoder = new BufferedBinaryEncoder(encoderout_stream, 1024*64);
		while((line = reader.readLine()) != null){
			String[] pairs = line.split("\t");
			for(String p : pairs){
				String[] kv = p.split(":");
				String key = kv[0];
				String value = kv[1];
//				System.out.println(key);
				int keyNumber = key2number.get(key);
				encoder.writeInt(keyNumber);//字段编号
				if(key.equals("orderid")){
					long valueLong = Long.parseLong(value);
					encoder.writeLong(valueLong-min_orderid);//字段本身
				}else if(key.equals("createtime")){
					long valueLong = Long.parseLong(value);
					encoder.writeLong(valueLong-min_createtime);//字段本身
				}else{
					encoder.writeString(value);//字段本身
				}
			}
			encoder.writeArrayEnd();//行结束
		}
		encoder.flush();
		encoderout_stream.close();
		reader.close();
		///
		System.out.println("end of encoder~~");
		System.out.println("distinct keys: " + key2number.size());
		HashMap<Integer, String> number2key = new HashMap<Integer, String>();
		for(Map.Entry<String, Integer> entry : key2number.entrySet()){
			number2key.put(entry.getValue(), entry.getKey());
		}
		decoder(encoderout, decoderout, number2key);
	}
	public HashMap<String, Integer> key2Number(File in) throws Exception{
		HashMap<String, Integer> frequency = new HashMap<String, Integer>();
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(in)));
		String line;
		while((line = reader.readLine()) != null){
			String[] pairs = line.split("\t");
			for(String p : pairs){
				String kv[] = p.split(":");
				String key = kv[0];
				Integer f = frequency.get(key);
				f = (f==null) ? 1 : f+1;
				frequency.put(key, f);
				if(key.equals("orderid")){
					long value = Long.parseLong(kv[1]);
					if(value < min_orderid)
						min_orderid = value;
				}else if(key.equals("createtime")){
					long value = Long.parseLong(kv[1]);
					min_createtime = Math.min(min_createtime, value);
				}
			}
		}
		////
		System.out.println("min_orderid=" + min_orderid);
		System.out.println("min_createtime=" + min_createtime);
		reader.close();
		TreeSet<KeyFrequency> set = new TreeSet<KeyFrequency>();
		for(Map.Entry<String, Integer> entry : frequency.entrySet()){
			set.add(new KeyFrequency(entry.getKey(), entry.getValue()));
		}
		HashMap<String, Integer> key2number = new HashMap<String, Integer>();
		int count = 1;
		for(KeyFrequency kf : set){
			System.out.println(kf.key + "->" + count);
			key2number.put(kf.key, count++);
//			System.out.println(kf);
		}
		return key2number;
	}
	class KeyFrequency implements Comparable<KeyFrequency>{
		public String key;
		public int f;
		public KeyFrequency(String key, int f){
			this.key = key;
			this.f = f;
		}
		@Override
		public int compareTo(KeyFrequency ok){
			int rs =  ok.f-f;
			return rs==0 ? ok.key.compareTo(key) : rs;
					
		}
		@Override
		public String toString() {
			return "KeyFrequency [key=" + key + ", f=" + f + "]";
		}
		
	}
}
