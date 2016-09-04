package hoolee.test;

import java.io.FileWriter;
import java.util.Random;

public class TestGen {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		FileWriter writer = new FileWriter("orderid.txt");
		Random random = new Random();
		int range = 1000000;
		for(int i=0; i<range; i++){
			writer.append(String.format("orderid:%05d", random.nextInt(range)));
			writer.append('\n');
		}
		writer.close();
		System.out.println("end gen");
	}

}
