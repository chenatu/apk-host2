/**
 * @(#)MainService.java, May 27, 2013. 
 * 
 */
package hongfeng.xu.apk.service;

import hongfeng.xu.apk.data.Protobuf.ApkInfo;
import hongfeng.xu.apk.store.HdfsStore;
import hongfeng.xu.apk.store.RedisStore;
import hongfeng.xu.apk.util.MD5Utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

/**
 * @author xuhongfeng
 * 
 */
@Service("mainService")
public class MainService {
	private static final Logger LOG = LoggerFactory
			.getLogger(MainService.class);

	@Autowired
	private HdfsStore hdfsStore;

	@Autowired
	private RedisStore redisStore;

	@Autowired
	private MD5Utils md5Utils;

	/**
	 * return false if is already exists
	 * 
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public boolean addApk(MultipartFile file) throws IOException {
		InputStream input = file.getInputStream();
		try {
			Path tmpPath = new Path("tmp/"
					+ file.getOriginalFilename().hashCode() + "-"
					+ System.currentTimeMillis());
			// temporary save to HDFS, and input is closed
			hdfsStore.put(input, tmpPath);

			// compute md5
			String md5 = null;
			InputStream tmpInput = null;
			try {
				tmpInput = hdfsStore.open(tmpPath);
				md5 = md5Utils.md5(tmpInput);
				LOG.info("computing md5 " + file.getOriginalFilename() + " "
						+ md5);
			} finally {
				tmpInput.close();
				// hdfsStore.remove(tmpPath);
			}

			// check if exists
			if (redisStore.exists(md5)) {
				LOG.info(tmpPath + " " + md5 + " already exists");
				hdfsStore.remove(tmpPath);
				return false;
			}

			Path finalPath = new Path("apk/" + md5 + ".apk");
			LOG.info(tmpPath + " " + md5 + " is a new apk");
			tmpInput = hdfsStore.open(tmpPath);
			hdfsStore.put(tmpInput, finalPath);
			// tmpInput is closed now and start to delete tmpPath
			hdfsStore.remove(tmpPath);
			// save ApkInfo to redis
			try {
				String fileName = file.getOriginalFilename();
				ApkInfo info = ApkInfo.newBuilder().setName(fileName)
						.setMd5(md5).setSize(file.getSize()).build();
				LOG.info(info.toString());
				redisStore.put(info);
			} catch (Throwable e) {
				hdfsStore.remove(finalPath);
				if (e instanceof IOException) {
					throw (IOException) e;
				} else {
					throw new IOException(e);
				}
			}
			return true;
		} finally {
			input.close();
		}
	}
	public boolean addDirApks(String sapkDir){
		File filedir = new File(sapkDir);
		File[] files = filedir.listFiles();
		for(int i=0; i<files.length;i++){
			if(files[i].isFile()){
				if(files[i].getName().lastIndexOf(".") !=-1){
					String ext=files[i].getName().substring(files[i].getName().lastIndexOf(".")).toLowerCase();
					if(ext.equals(".apk")){
						addLocalApk(files[i]);				
					}
				}
			}
		}						
		return true;
	}
	public boolean addLocalApk(File file) {
		try{
			Path tmpPath = new Path("tmp/" + file.getName().hashCode() + "-"
					+ System.currentTimeMillis());
			// temporary save to HDFS, and input is closed
			hdfsStore.put(file, tmpPath);
			// compute md5
			String md5 = null;
			InputStream tmpInput = null;
			try {
				tmpInput = hdfsStore.open(tmpPath);
				md5 = md5Utils.md5(tmpInput);
				LOG.info("computing md5 " + file.getName() + " " + md5);
			} finally {
				tmpInput.close();
				// hdfsStore.remove(tmpPath);
			}
	
			// check if exists
			if (redisStore.exists(md5)) {
				LOG.info(tmpPath + " " + md5 + " already exists");
				hdfsStore.remove2(tmpPath);
				return false;
			}
	
			Path finalPath = new Path("apk/" + md5 + ".apk");
			LOG.info(tmpPath + " " + md5 + " is a new apk");
			tmpInput = hdfsStore.open(tmpPath);
			hdfsStore.put(tmpInput, finalPath);
			// tmpInput is closed now and start to delete tmpPath
			hdfsStore.remove(tmpPath);
			// save ApkInfo to redis
			try {
				String fileName = file.getName();
				ApkInfo info = ApkInfo.newBuilder().setName(fileName).setMd5(md5)
						.setSize(file.length()).build();
				LOG.info(info.toString());
				redisStore.put(info);
			} catch (Throwable e) {
				hdfsStore.remove(finalPath);
				if (e instanceof IOException) {
					throw (IOException) e;
				} else {
					throw new IOException(e);
				}
			}
			return true;
		}catch(Exception e){
			LOG.info(e.getMessage());
			return false;
		}
	}
}
