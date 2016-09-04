package hoolee.util;

import java.io.File;

public class DistinctDisk {
	/**输入输出文件所在的所有不同磁盘*/
	public static final String[] distinct_disks = {
			"/disk1",
			"/disk2",
			"/disk3",
			"/home",
			"/mnt/sdb",
			"/mnt/sdc",
	};
	public static Object[] newLocks(){
		Object[] locks = new Object[DistinctDisk.distinct_disks.length+1];
		for(int i=0; i<locks.length; i++){
			locks[i] = new Object();
		}
		return locks;
	}
	public static Object getLockOfFile(Object[] locks, File f){
		for(int i=0; i<DistinctDisk.distinct_disks.length; i++){
			if(f.getAbsolutePath().toString().startsWith(DistinctDisk.distinct_disks[i])){
				return locks[i];
			}
		}
		return locks[locks.length-1];//not find, lock on the last Object
	}
}
