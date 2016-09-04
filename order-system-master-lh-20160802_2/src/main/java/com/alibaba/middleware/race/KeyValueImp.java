package com.alibaba.middleware.race;

import com.alibaba.middleware.race.OrderSystem.KeyValue;
import com.alibaba.middleware.race.OrderSystem.TypeException;

public class KeyValueImp implements KeyValue{
	private String key;
	private Object value;

	public KeyValueImp(String key, Object value){
		this.key = key;
		this.value = value;
	}
	@Override
	public String toString(){
		return key+":"+value;
	}
	/**值计算key值的hashCode*/
	@Override
	public int hashCode(){
		return key==null ? 0 : key.hashCode();
	}
	/**key值相等则认为相等*/
	@Override
	public boolean equals(Object o){
		if(o instanceof KeyValueImp){
			KeyValueImp ot = (KeyValueImp)o;
			return this.key.equals(ot.key);
		}
		return false;
	}
	@Override
	public String key() {
		return key;
	}
	@Override
	public String valueAsString() {
		return value.toString();
	}

	@Override
	public long valueAsLong() throws TypeException {
		if(value instanceof Long){
			return (Long)value;
		}else if(value instanceof Double){
			return ((Double)value).longValue();
		}else if(value instanceof Float){
			return ((Float)value).longValue();
		}
		
		long rs;
		try{
			rs = Long.parseLong(value.toString());
		}catch(Exception e){
			throw new TypeException();
		}
		return rs;
	}

	@Override
	public double valueAsDouble() throws TypeException {
		if(value instanceof Double){
			return (Double)value;
		}else if(value instanceof Float){
			return ((Float)value).doubleValue();
		}else if(value instanceof Long){
			return ((Long)value).doubleValue();
		}
		double rs;
		try{
			rs = Double.parseDouble(value.toString());
		}catch(Exception e){
			throw new TypeException();
		}
		return rs;
	}

	@Override
	public boolean valueAsBoolean() throws TypeException {
		if(value instanceof Boolean){
			return (Boolean)value;
		}
		boolean rs;
		try{
			rs = Boolean.parseBoolean(value.toString());
		}catch(Exception e){
			throw new TypeException();
		}
		return rs;
	}

}
