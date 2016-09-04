package hoolee.index;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

import avro.io.BufferedBinaryEncoder;
import hoolee.io.MyByteArrayOutputStream;
import hoolee.sort.MyUUID;
import hoolee.sort.TypeInference;
/**
 * 加入了Type
 */
public class BTreeNodeMakeTree2<E>{
	private ArrayList<E> division_value_list = new ArrayList<E>();
	private ArrayList<Integer> file_number_list = new ArrayList<Integer>();
	private ArrayList<Long> offset_list = new ArrayList<Long>();
	private int treeLevel = 0;
	private FileOutputStream fileOutputStream;
	private FileChannel fileChannel;
	private BufferedOutputStream bufferedOutputStream;
	private TypeInference.Type type;
	
	public BTreeNodeMakeTree2(FileOutputStream fileOutputStream, int treeLevel, TypeInference.Type type){
		this.fileOutputStream = fileOutputStream;
		bufferedOutputStream = new BufferedOutputStream(fileOutputStream, 1024*1024);
		fileChannel = fileOutputStream.getChannel();
		this.treeLevel = treeLevel;
		this.type = type;
	}
	public void add(E division, long offset){
		division_value_list.add(division);
		offset_list.add(offset);
	}
	public void add(E division, int fileNumber, long offset){
		if(treeLevel != BTreeNodeLevel.LEVEL_LEAF){
			throw new RuntimeException("this tree node is not LEAF!!");
		}
		division_value_list.add(division);
		file_number_list.add(fileNumber);
		offset_list.add(offset);
	}
	public int size(){
		return offset_list.size();
	}
	public E getMaxDivision(){
		return division_value_list.get(division_value_list.size()-1);
	}
	public FileOutputStream getFileOutputStream(){
		return fileOutputStream;
	}
	public long writeOut() throws IOException{
//		System.out.println("---level=" + treeLevel + ", in writeOut() division_value_list "+ division_value_list.size() 
//				+ ", file_number_list.siz(): " + file_number_list.size() +  ", offset_list "+ offset_list.size());
//		System.out.println(division_value_list);
//		System.out.println(file_number_list);
//		System.out.println(offset_list);
		long old_size = fileChannel.size();
		MyByteArrayOutputStream buf_stream = new MyByteArrayOutputStream();
		BufferedBinaryEncoder encoder = new BufferedBinaryEncoder(buf_stream, 1024*64);
		encoder.writeInt(division_value_list.size());
		for(E e : division_value_list){
			switch(type){
			case BOOLEAN:
				encoder.writeBoolean((Boolean)e);
				break;
			case LONG:
				encoder.writeLong((Long)e);
				break;
			case DOUBLE:
				encoder.writeDouble((Double)e);
				break;
			case STR_UUID:
				String uuid_str = (String)e;
				int idx = uuid_str.indexOf('_');
				encoder.writeString(uuid_str.substring(0, idx));
				encoder.writeFixed(MyUUID.UUIDToByte16(uuid_str.substring(idx+1)));
				break;
			case STR_HALFUUID:
				String uuid_str2 = (String)e;
				int idx2 = uuid_str2.indexOf('-');
				encoder.writeString(uuid_str2.substring(0, idx2));
				encoder.writeFixed(MyUUID.HALFUUIDToByte8(uuid_str2.substring(idx2+1)));
				break;
			case UUID:
				encoder.writeFixed(MyUUID.UUIDToByte16((String)e));
				break;
			default: // STRING
				encoder.writeString((String)e);
			}
		}
		encoder.writeInt(file_number_list.size());
		for(int fileNumber : file_number_list){
			encoder.writeInt(fileNumber);
		}
		encoder.writeInt(offset_list.size());
		for(long offset : offset_list){
			encoder.writeLong(offset);
		}
		encoder.flush();
//		byte[] bytes = buf_stream.toByteArray();
//		bufferedOutputStream.write(bytes);
		bufferedOutputStream.write(buf_stream.getByteBuf(), 0, buf_stream.size());
		bufferedOutputStream.flush();
		return old_size;
	}
}

















