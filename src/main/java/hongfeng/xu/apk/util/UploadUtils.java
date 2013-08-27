/**
 * @(#)UploadUtils.java, Aug 18, 2013. 
 * 
 */

package hongfeng.xu.apk.util;

import java.io.File;
import java.io.FileNotFoundException;

import hongfeng.xu.apk.service.MainService;

public class UploadUtils {
	//upload multiple apk files in the same directory
	public static boolean uploadApks(String sdirpath) throws java.io.FileNotFoundException
	{
		MainService mainservice = new MainService();
		File filedir = new File(sdirpath);
		File[] files = filedir.listFiles();
		for(int i=0; i<files.length;i++){
			if(files[i].isFile()){
				if(files[i].getName().lastIndexOf(".") !=-1){
					String ext=files[i].getName().substring(files[i].getName().lastIndexOf(".")).toLowerCase();
					if(ext.equals(".apk")){
						System.out.println(files[i].getName());
						mainservice.addLocalApk(files[i]);				
					}
				}
			}
		}
		return true;
	}
	public static void main(String[] args) throws FileNotFoundException {		
		UploadUtils.uploadApks("/home/chenatu");
	}

}
