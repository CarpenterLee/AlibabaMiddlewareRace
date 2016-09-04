package com.alibaba.middleware.race;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.alibaba.middleware.race.OrderSystem.KeyValue;
import com.alibaba.middleware.race.OrderSystem.Result;

public class ResultImp implements Result{
	private HashMap<String, Object> keyValues = new HashMap<String, Object>();
	long orderid = -1;
	String buyerid;
	String goodid;
	public ResultImp(){
	}
	public ResultImp(long orderid){
		this.orderid = orderid;
	}
	/**加入新的key, value对，相同的key值只会保留最后一个*/
	public void add(String key, Object value){
		keyValues.put(key, value);
	}
	public Object getValue(String key){
		return keyValues.get(key);
	}
	@Override
	public KeyValue get(String key) {
		Object value = keyValues.get(key);
		return value==null ? null : new KeyValueImp(key, value);
	}

	@Override
	public KeyValue[] getAll() {
		KeyValueImp rs[] = new KeyValueImp[keyValues.size()];
		int count = 0;
		for(Map.Entry<String, Object> entry : keyValues.entrySet()){
			rs[count++] = new KeyValueImp(entry.getKey(), entry.getValue());
		}
		return rs;
	}
	@Override
	public long orderId(){
		if(orderid == -1){
			throw new RuntimeException("orderid is unset!!");
		}
		return orderid;
	}
	@Override
	public String toString(){
		return "orderid:" + orderid + ", KV:" + keyValues.toString();
	}
	public void setOrderId(long orderid){
		this.orderid = orderid;
	}
	public String getBuyerid() {
		return buyerid;
	}
	public void setBuyerid(String buyerid) {
		this.buyerid = buyerid;
	}
	public String getGoodid() {
		return goodid;
	}
	public void setGoodid(String goodid) {
		this.goodid = goodid;
	}
	public void merge(ResultImp other){
		for(Map.Entry<String, Object> en : other.keyValues.entrySet()){
			this.keyValues.put(en.getKey(), en.getValue());
		}
	}
	public Set<String> keySet(){
		return keyValues.keySet();
	}
}
