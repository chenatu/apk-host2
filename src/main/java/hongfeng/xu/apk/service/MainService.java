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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.python.core.PyFunction;
import org.python.core.PyInteger;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

/**
 * @author xuhongfeng
 * 
 */
@Service("mainService")
public class MainService {
	private static final Logger LOG = LoggerFactory
			.getLogger(MainService.class);
	
	@Value("${androguard.home}")
    private String androhome;
	
	@Autowired
	private HdfsStore hdfsStore;

	@Autowired
	private RedisStore redisStore;

	@Autowired
	private MD5Utils md5Utils;
	
	public Path apkHDFSPath;
	
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
			this.apkHDFSPath = finalPath;
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
	
	public boolean androApkInfo(Path apkHDFSPath){
		LOG.info("androhome: "+androhome);
		LOG.info("androApkInfo: "+apkHDFSPath.getName());
		// Load the apk from hdfs to local fs
		Path apkLocalPath = new Path(androhome+"/tmpAPK");
		File apkLocalFile = new File(androhome+"/tmpAPK");
		if(!hdfsStore.get(apkHDFSPath, apkLocalPath)){
			LOG.info("Can not get the apk from HDFS: " + apkHDFSPath.toString());
			return false;
		}
		LOG.info("tmp apk absolute path: "+apkLocalFile.getAbsolutePath());
		// Running Python androapkinfo and create temporary info file
		LOG.info("starting androapkinfo");
		/*PythonInterpreter interpreter = new PythonInterpreter(); 
		interpreter.exec("import sys;");
		LOG.info("import sys over");
		interpreter.exec("import subprocess;");
		LOG.info("import subprocess over");
		String fileOpenCmd = "out = file('"+ apkHDFSPath.getName() +".info', 'w');";
		LOG.info(fileOpenCmd);
		interpreter.exec(fileOpenCmd);
		LOG.info("Python file created success");
		String androApkInfoCmd="child = subprocess.Popen(['"+androhome+"/androapkinfo.py', '-i', '"+ apkLocalPath+ "'], stdout = out);";
		interpreter.exec(androApkInfoCmd);
		interpreter.exec("child.wait();");
		interpreter.exec("out.close();");
		LOG.info("complete androapkinfo");*/
		
		Runtime rt = Runtime.getRuntime();
		String slocalInfoPath = androhome+"/"+apkHDFSPath.getName()+".info";
		String slocalReportPath = androhome+"/"+apkHDFSPath.getName()+".report";
		File infoFile = new File(slocalInfoPath);
		File reportFile = new File(slocalReportPath);
		String cmd = "python " + androhome+"/androapkinfo.py -i "+apkLocalPath.toString()+" |"+slocalInfoPath;
		try {
			if(!infoFile.createNewFile()) return false;
			LOG.info(cmd);
			Process proc = Runtime.getRuntime().exec(cmd);
			proc.waitFor(); 
		} catch (IOException | InterruptedException e2) {
			LOG.info("Getting runtime error");
			LOG.info(e2.getMessage());
			return false;
		}
		// Save the temporary info file into temp report file
		
		FileChannel inChannel = null; 
		FileChannel outChannel = null; 
		FileInputStream inStream = null; 
		FileOutputStream outStream = null;
		try {
			RandomAccessFile out = new RandomAccessFile(reportFile.getPath().toString(),"rw");
			out.writeBytes("Apk info----->\n");
			out.close();
			inStream = new FileInputStream(infoFile);
			inChannel = inStream.getChannel();
			outStream = new FileOutputStream(reportFile, true);
			outChannel = outStream.getChannel();
			long bytesTransferred = 0;
			while(bytesTransferred < inChannel.size()){ 
		         bytesTransferred += inChannel.transferTo(0, inChannel.size(), outChannel); 
		    }
			//being defensive about closing all channels and streams  
			if (inChannel != null) inChannel.close(); 
		    if (outChannel != null) outChannel.close(); 
		    if (inStream != null) inStream.close(); 
		    if (outStream != null) outStream.close(); 
		} catch (IOException e1) {
			LOG.info("Report Append Error " + reportFile.getName()+ " error");
			LOG.info(e1.getMessage());
			return false;
		}
		// upload the temporary file into hdfs
		Path infoPath = new Path("info/"+apkHDFSPath.getName()+".info");
		Path reportPath = new Path("report/"+apkHDFSPath.getName()+".info");
		try {
			hdfsStore.put(infoFile, infoPath);
		} catch (IOException e) {
			LOG.info("Put file " + infoFile.getName()+ " error");
			LOG.info(e.getMessage());
			return false;
		}
		
		try {
			hdfsStore.put(reportFile, reportPath);
		} catch (IOException e) {
			LOG.info("Put file " + infoFile.getName()+ " error");
			LOG.info(e.getMessage());
			return false;
		}
		
		// remove the local temporary files
		if (!apkLocalFile.exists()) {
			return false;
		}else{
			apkLocalFile.delete(); 
		}
		if (!infoFile.exists()) {
			return false;
		}else{
			infoFile.delete(); 
		}
		if (!reportFile.exists()) {
			return false;
		}else{
			reportFile.delete(); 
		}
		//Set the apkinfo and apkreport path of hdfs into redis
		byte[] md5 = apkHDFSPath.getName().substring(0, apkHDFSPath.getName().lastIndexOf('.')-1).getBytes();
		redisStore.putPath(md5, infoPath);
		redisStore.putPath(md5, reportPath);
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
