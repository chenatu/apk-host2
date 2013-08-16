/**
 * @(#)HadoopStore.java, May 27, 2013. 
 * 
 */
package hongfeng.xu.apk.store;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.PostConstruct;

//import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

/**
 * @author xuhongfeng
 *
 */
@Repository("hdfsStore")
public class HdfsStore {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsStore.class);
    
    private Configuration conf;
    
    @Value("${hadoop.home}")
    private String hadoopHome;
    
    @PostConstruct
    private void init() {
        conf = new Configuration();
        conf.addResource(new Path(hadoopHome + "/conf/core-site.xml"));
        conf.addResource(new Path(hadoopHome + "/conf/hdfs-site.xml.xml"));
        LOG.info("configuration : " + conf);
    }
    
    public void put(InputStream input, Path path) throws IOException {
        FileSystem fs = borrowFS();
        FSDataOutputStream out = null;
        try {
            out = fs.create(path);
            byte[] buf = new byte[8192];
            int len;
            /*while((len=input.read(buf, 0, buf.length)) != -1) {
                LOG.info("len = " + len);
                out.write(buf, 0, len);
            }*/
            IOUtils.copyBytes(input, out, 4096, true);
        } finally {
        	FileStatus fileStatus = fs.getFileStatus(path);
            LOG.info(fileStatus.getPath()+" size: "+fileStatus.getLen());
            //out.close();
            fs.close();
        }
    }
    
    public boolean exists(Path path) throws IOException {
        FileSystem fs = borrowFS();
        try {
            return fs.exists(path);
        } finally {
            fs.close();
        }
    }
    
    public boolean remove(Path path) throws IOException {
        FileSystem fs = borrowFS();
        try {
            if (fs.exists(path)) {
                return fs.delete(path, true);
            }
        } finally {
            fs.close();
        }
        return false;
    }
    
    public InputStream open(Path path) throws IOException {
        FileSystem fs = borrowFS();
        return fs.open(path);
    }
    
    private FileSystem borrowFS() throws IOException {
        return FileSystem.get(conf);
    }
}
