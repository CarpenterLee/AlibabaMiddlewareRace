package com.alibaba.middleware.race;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Scanner;
import java.util.concurrent.Semaphore;
public class OrderSystemImpl implements OrderSystem {
	public static void main(String args[]) throws Exception{
		OrderSystemAgency os = new OrderSystemAgency();
		Collection<String> orderFiles = new ArrayList<String>();
		
		final String baseDir = "/home/gjl_workspace/lh_workspace/data/prerun_data/";
		orderFiles.add(baseDir+"order.0.0");
		orderFiles.add(baseDir+"order.0.3");
		orderFiles.add(baseDir+"order.1.1");
		orderFiles.add(baseDir+"order.2.2");
		Collection<String> buyerFiles = new ArrayList<String>();
		buyerFiles.add(baseDir+"buyer.0.0");
		buyerFiles.add(baseDir+"buyer.1.1");
		Collection<String> goodFiles = new ArrayList<String>();
		goodFiles.add(baseDir+"good.0.0");
		goodFiles.add(baseDir+"good.1.1");
		goodFiles.add(baseDir+"good.2.2");
		Collection<String> storeFolders = new ArrayList<String>();
		
//		for(int i=0; i<=8; i++){
//			orderFiles.add("/home/alibaba-data/order_records.txt_" + i);
//		}
//		for(int i=9; i<=16; i++){
//			orderFiles.add("/mnt/sdb/lh/orderdata/order_records.txt_" + i);
//		}
//		for(int i=17; i<=24; i++){
//			orderFiles.add("/mnt/sdc/lh2/orderdata/order_records.txt_" + i);
//		}
//		Collection<String> buyerFiles = new ArrayList<String>();
//		buyerFiles.add("/home/alibaba-data/buyer_records.txt");
//		Collection<String> goodFiles = new ArrayList<String>();
//		goodFiles.add("/home/alibaba-data/good_records.txt");
//		Collection<String> storeFolders = new ArrayList<String>();
		
		
		storeFolders.add("/home/alibaba-data/dir1/");
		storeFolders.add("/mnt/sdb/lh/orderdata/dir2/");
		storeFolders.add("/mnt/sdc/lh2/orderdata/dir3/");
		Scanner sc = new Scanner(System.in);
		String line;
		os.construct(orderFiles, buyerFiles, goodFiles, storeFolders);
		System.out.println("input orderid to query-------");
		System.out.print(">>> ");
		ArrayList<String> keys = new ArrayList<String>();
		keys.add("a_g_12146");
//		keys.add("a_g_32587");
		

		while((line=sc.nextLine()) != null){
//			Result rs = null;
//			try{
//                rs = os.queryOrder(Long.parseLong(line), keys);
//			}catch(Exception e){
//				e.printStackTrace();
//			}
////			Result rs = os.testLookUpGood(line);
//			System.out.println(rs);
			Iterator<Result> results = os.queryOrdersByBuyer(1462018520, 1473999229, line);
//			Iterator<Result> results = os.queryOrdersBySaler(null, line, keys);
			int count = 0;
			while(results.hasNext()){
				System.out.println(results.next());
				count++;
			}
			System.out.println("result lines: " + count);
			
//			String key = sc.nextLine();
//			KeyValue rs = os.sumOrdersByGood(line, key);
//			System.out.println("sum_result=" + rs);
			
//			String key = sc.nextLine();
//			KeyValue rs = os.sumOrdersByGood(line, key);
//			System.out.println(rs);
			System.out.print(">>> ");
		}
		System.out.println("end~~");
	}
	private OrderSystemAgency agency = new OrderSystemAgency();
	
	@Override
	public void construct(Collection<String> orderFiles,
			Collection<String> buyerFiles, Collection<String> goodFiles,
			Collection<String> storeFolders) throws IOException,
			InterruptedException {
		agency.construct(orderFiles, buyerFiles, goodFiles, storeFolders);
	}
	@Override
	public Result queryOrder(long orderId, Collection<String> keys) {
		return agency.queryOrder(orderId, keys);
	}
	@Override
	public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime,
			String buyerid) {
		return agency.queryOrdersByBuyer(startTime, endTime, buyerid);
	}

	@Override
	public Iterator<Result> queryOrdersBySaler(String salerid, String goodid,
			Collection<String> keys) {
		return agency.queryOrdersBySaler(salerid, goodid, keys);
	}

	@Override
	public KeyValue sumOrdersByGood(String goodid, String key) {
		return agency.sumOrdersByGood(goodid, key);
	}

}
