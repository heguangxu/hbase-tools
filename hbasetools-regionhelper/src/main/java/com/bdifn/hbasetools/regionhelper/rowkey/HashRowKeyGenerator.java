package com.bdifn.hbasetools.regionhelper.rowkey;

import java.util.Random;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

/**
 * 散列以后的rowkey不会是随机值，因为散列函数是固定的。查询时用同样的散列函数处理用户id，再用处理后的结果查询即可。
 * 
 * https://www.cnblogs.com/bdifn/p/3801737.html
 * 
 * @author Administrator
 *         hash就是rowkey前面由一串随机字符串组成,随机字符串生成方式可以由SHA或者MD5等方式生成，只要region所管理的start-end
 *         keys范围比较随机，那么就可以解决写热点问题。
 */
public class HashRowKeyGenerator implements RowKeyGenerator {
	private long currentId = 1;
	private long currentTime = System.currentTimeMillis();

	private Random random = new Random();

	public byte[] nextId() {
		try {
			currentTime += random.nextInt(1000);

			byte[] lowT = Bytes.copy(Bytes.toBytes(currentTime), 4, 4);
			byte[] lowU = Bytes.copy(Bytes.toBytes(currentId), 4, 4);

			return Bytes.add(MD5Hash.getMD5AsHex(Bytes.add(lowU, lowT)).substring(0, 8).getBytes(),
					Bytes.toBytes(currentId));
		} finally {
			currentId++;
		}
	}

}
