package com.bdifn.hbasetools.regionhelper.rowkey;

import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;

import com.bdifn.hbasetools.regionhelper.factory.BeanFactory;

/**
 * 1.创建split计算器，用于从抽样数据中生成一个比较合适的splitKeys
 * 
 * @author Administrator
 *
 */
public class HashChoreWoker implements SplitKeysCalculator {
	// 随机取机数目
	private int baseRecord;
	// rowkey生成器
	private RowKeyGenerator rkGen;
	// 取样时，由取样数目及region数相除所得的数量
	private int splitKeysBase;
	// splitkeys个数
	private int splitKeysNumber;
	// 由抽样计算出来的splitkeys结果，splitKeys是分区号即可
	private byte[][] splitKeys;

	public HashChoreWoker(int baseRecord, int prepareRegions) {
		this.baseRecord = baseRecord;
		// 实例化rowkey生成器
		rkGen = BeanFactory.getInstance().getBeanInstance(RowKeyGenerator.class);
		splitKeysNumber = prepareRegions - 1;

		splitKeysBase = baseRecord / prepareRegions;
	}

	public byte[][] calcSplitKeys() {
		splitKeys = new byte[splitKeysNumber][];
		TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

		for (int i = 0; i < baseRecord; i++) {
			rows.add(rkGen.nextId());
		}
		int pointer = 0;

		Iterator<byte[]> rowKeyIter = rows.iterator();

		int index = 0;
		while (rowKeyIter.hasNext()) {
			byte[] tempRow = rowKeyIter.next();
			rowKeyIter.remove();
			if ((pointer != 0) && (pointer % splitKeysBase == 0)) {
				if (index < splitKeysNumber) {
					splitKeys[index] = tempRow;
					index++;
				}
			}
			pointer++;
		}

		rows.clear();
		rows = null;
		return splitKeys;
	}

	public static void main(String[] args) {
		/**
		 * https://www.cnblogs.com/bdifn/p/3801737.html
		 * 假设rowKey原本是自增长的long型，可以将rowkey转为hash再转为bytes，加上本身id
		 * 转为bytes,组成rowkey，这样就生成随便的rowkey。那么对于这种方式的rowkey设计，如何去进行预分区呢？
		 * 1.取样，先随机生成一定数量的rowkey,将取样数据按升序排序放到一个集合里
		 * 2.根据预分区的region个数，对整个集合平均分割，即是相关的splitKeys.
		 * 3.HBaseAdmin.createTable(HTableDescriptor tableDescriptor,byte[][]
		 * splitkeys)可以指定预分区的splitKey，即是指定region间的rowkey临界值.
		 */
		byte[][] temp = new HashChoreWoker(1000000, 38).calcSplitKeys();

		for (byte[] row : temp) {
			System.out.println(Bytes.toStringBinary(row));
		}
		System.out.println(temp.length);
	}

}
