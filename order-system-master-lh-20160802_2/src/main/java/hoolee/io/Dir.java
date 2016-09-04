package hoolee.io;

import java.io.File;

public class Dir {

//	public static void main(String[] args) {
////		mkDirsAndClean(new File("sort/offset2"));
//	}
	/**创建dir指定的目录，并保证目录是空的*/
	public static File mkDirsAndClean(File dir){
		delete(dir);
		dir.mkdirs();
		return dir;
	}
	private static void delete(File file){
//		System.out.println("delete file: " + file);
		if(!file.exists()){
			return;
		}
		if(file.isDirectory()){
			for(String f : file.list()){
				delete(new File(file, f));
			}
		}
		file.delete();
	}

}
