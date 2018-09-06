package com.bdifn.hbasetools.regionhelper.rowkey;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * @author Administrator
 * 
 *         partition故名思义，就是分区式，这种分区有点类似于mapreduce中的partitioner,
 *         将区域用长整数(Long)作为分区号，每个region管理着相应的区域数据，在rowKey生成时，将id取模后，
 *         然后拼上id整体作为rowKey.这个比较简单，不需要取样，splitKeys也非常简单，直接是分区号即可。直接上代码吧：
 */
public class PartitionRowKeyManager implements RowKeyGenerator, SplitKeysCalculator {

	public static final int DEFAULT_PARTITION_AMOUNT = 20;
	private long currentId = 1;
	private int partition = DEFAULT_PARTITION_AMOUNT;

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public byte[] nextId() {
		try {
			long partitionId = currentId % partition;
			return Bytes.add(Bytes.toBytes(partitionId), Bytes.toBytes(currentId));
		} finally {
			currentId++;
		}
	}

	/**
	 * calcSplitKeys方法比较单纯，splitKey就是partition的编号,我们看看测试类:
	 * 
	 * 
	 */
	public byte[][] calcSplitKeys() {
		byte[][] splitKeys = new byte[partition - 1][];
		for (int i = 1; i < partition; i++) {
			splitKeys[i - 1] = Bytes.toBytes((long) i);
		}
		return splitKeys;
	}
}
