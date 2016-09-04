package hoolee.sort;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TypeInference {

	public static void main(String[] args) {
		String uuid = "goodon_f6c8b619-9cb0-46a5-904c-b1be3d4bf146";
		String half_uuid = "wx-babb-ddf10c07850d";
		System.out.println(isSTR_HALFUUID(half_uuid));
	}
	public static enum Type{
		BOOLEAN,
		LONG,
		DOUBLE,
		STR_UUID,
		STR_HALFUUID,
		UUID,
		STRING,
	}
	private Map<String, Type> typeMap = new HashMap<String, Type>();
	public TypeInference(Map<String, ArrayList<String>> allKeysMap){
		for(Map.Entry<String, ArrayList<String>> entry : allKeysMap.entrySet()){
			Type type = Type.STRING;
			ArrayList<String> values = entry.getValue();
			if(isAllBoolean(values)){
				type = Type.BOOLEAN;
			}else if(isAllLong(values)){
				type = Type.LONG;
			}else if(isAllDouble(values)){
				type = Type.DOUBLE;
			}else if(isAllSTR_UUID(values)){
				type = Type.STR_UUID;
			}else if(isAllSTR_HALFUUID(values)){
				type = Type.STR_HALFUUID;
			}else if(isAllUUID(values)){
				type = Type.UUID;
			}
			typeMap.put(entry.getKey(), type);
		}
//		for(Map.Entry<String, Type> entry : typeMap.entrySet()){
//			System.out.println(entry.getKey() + " -> " + entry.getValue());
//		}
			
	}
	/**
	 * 返回key指定的值对应的类型，如果未找到则返回STRING
	 */
	public Type getType(String key){
//		Type t = typeMap.get(key);
//		return t==null ? Type.STRING : t;
		if("orderid".equals(key) || "createtime".equals(key)){
			return Type.LONG;
		}
		Type t = typeMap.get(key);
		if(t==Type.UUID || t==Type.STR_UUID || t==Type.STR_HALFUUID){
			return t;
		}
		return Type.STRING;
//		return t==null ? Type.STRING : t;
	}
	public static boolean isAllBoolean(List<String> list){
		for(String str : list)
			if(!isBoolean(str))
				return false;
		return true;
	}
	public static boolean isAllLong(List<String> list){
		for(String str : list)
			if(!isLong(str))
				return false;
		return true;
	}
	public static boolean isAllDouble(List<String> list){
		for(String str : list)
			if(!isDouble(str))
				return false;
		return true;
	}
	public static boolean isAllSTR_UUID(List<String> list){
		for(String str : list)
			if(!isSTR_UUID(str))
				return false;
		return true;
	}
	public static boolean isAllSTR_HALFUUID(List<String> list){
		for(String str : list)
			if(!isSTR_HALFUUID(str))
				return false;
		return true;
	}
	public static boolean isAllUUID(List<String> list){
		for(String str : list)
			if(!isUUID(str))
				return false;
		return true;
	}
	public static boolean isBoolean(String str){
		return str.matches("true|false");
	}
	public static boolean isLong(String str){
		return str.matches("([0-9]{1,19})|([-+][0-9]{1,19})");
	}
	public static boolean isDouble(String str){
		return str.matches("([+-]?[0-9]+)|([+-]?[0-9]+\\.[0-9]*([eE][+-]?[0-9]+)?)");
	}
	public static boolean isSTR_UUID(String str){
		return str.matches("[a-zA-Z]+_[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}");
	}
	/**形如 wx-babb-ddf10c07850d*/
	public static boolean isSTR_HALFUUID(String str){
		return str.matches("[a-zA-Z]+-[0-9a-f]{4}-[0-9a-f]{12}");
	}
	public static boolean isUUID(String str){
		return str.matches("[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}");
	}
	@Override
	public String toString(){
		StringBuilder buf = new StringBuilder();
		for(Map.Entry<String, Type> entry : typeMap.entrySet()){
			buf.append(entry.toString()).append('\n');
		}
		return buf.toString();
	}
}
