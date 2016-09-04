package hoolee.util;

import java.util.ArrayList;

public class MyStringSpliter {
	public static void main(String[] args){
		String line = "orderid:612361635	createtime:1476175297"
				+ "	buyerid:tp-953a-0a9350ecd636	goodid:aye-b3c4-622e1aa79a5b	";
		split(line, '\t');
	}
	public static ArrayList<String> split(String line, char c){
		ArrayList<String> rs = new ArrayList<String>(16);
		int last_bg = 0;
		while(true){
			int idx = line.indexOf(c, last_bg);
			if(idx == -1){
				String str = line.substring(last_bg);
				if(!str.isEmpty()){
					rs.add(str);
				}
				break;
			}
			String str = line.substring(last_bg, idx);
//			if(!str.isEmpty()){
//				rs.add(str);
//			}
			rs.add(str);
			last_bg=idx+1;
		}
//		for(String s : rs){
//			System.out.println(s+"|");
//		}
//		System.out.println("rs.size()=" + rs.size());
		return rs;
	}
}
