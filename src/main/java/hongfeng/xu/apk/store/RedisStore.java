/**
 * @(#)RedisStore.java, May 29, 2013. 
 * 
 */
package hongfeng.xu.apk.store;

import hongfeng.xu.apk.data.Protobuf.ApkInfo;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import redis.clients.jedis.Jedis;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * @author xuhongfeng
 *
 */
@Repository("redisStore")
public class RedisStore {
	private static final Logger LOG = LoggerFactory.getLogger(RedisStore.class);
    public void put(ApkInfo info) throws InvalidProtocolBufferException {
        borrowJedis().rpush(info.getMd5().getBytes(), info.toByteArray());
    }
    // Save hdfs path into redis
    public void putPath(byte[] md5, Path path){
    	borrowJedis().rpush(md5, path.toString().getBytes());
    }
    
    public boolean exists(String md5) {
        return borrowJedis().exists(md5.getBytes());
    }
    
    public ApkInfo get(String md5) throws IOException {
        byte[] bytes = borrowJedis().get(md5.getBytes());
        try {
            return ApkInfo.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new IOException(e);
        }
    }
    
    private Jedis borrowJedis() {
        return new Jedis("localhost");
    }
}
