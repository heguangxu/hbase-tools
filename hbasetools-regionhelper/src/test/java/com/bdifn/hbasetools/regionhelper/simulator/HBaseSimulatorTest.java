package com.bdifn.hbasetools.regionhelper.simulator;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.bdifn.hbasetools.regionhelper.factory.BeanFactory;
import com.bdifn.hbasetools.regionhelper.rowkey.HashChoreWoker;
import com.bdifn.hbasetools.regionhelper.rowkey.PartitionRowKeyManager;
import com.bdifn.hbasetools.regionhelper.rowkey.RowKeyGenerator;

/**
 * 
 * @author Administrator
 *
 *         最后题外话是我想分享我在github中建了一个project,希望做一些hbase一些工具：
 *         https://github.com/bdifn/hbase-tools,如果本地装了git的话，可以执行命令: git clone
 *         https://github.com/bdifn/hbase-tools.git
 *         目前加了一个region-helper子项目，也是目前唯一的一个子项目，项目使用maven管理,
 *         主要目的是帮助我们设计rowkey做一些参考，比如我们设计的随机写和预分区测试，提供了抽样的功能，提供了检测随机写的功能，
 *         然后统计按目前rowkey设计，随机写n条记录后，统计每个region的记录数，然后显示比例等。
 *         测试仿真模块我程为simualtor,主要是模拟hbase的region行为，simple的实现，仅仅是上面提到的预测我们rowkey设计后，
 *         建好预分区后，写数据的的分布比例，而emulation是比较逼真的仿真，设想是我们写数据时，会统计数目的大小，
 *         根据我们的hbase-site.xml设定，模拟memStore行为，模拟hfile的行为，最终会生成一份表的报表，比如分区的数据大小，
 *         是否split了，等等，以供我们去设计hbase表时有一个参考，但是遗憾的是，由于时间关系，我只花了一点业余时间简单搭了一下框架，
 *         目前没有更一步的实现，以后有时间再加以完善
 */
public class HBaseSimulatorTest {
	// 通过SPI方式获取HBaseSimulator实例,SPI的实现为simgple
	private HBaseSimulator hbase = BeanFactory.getInstance().getBeanInstance(HBaseSimulator.class);
	// 获取RowKeyGenerator实例，SPI的实现为hashRowkey
	private RowKeyGenerator rkGen = BeanFactory.getInstance().getBeanInstance(RowKeyGenerator.class);
	// 初如化苦工，去检测100w个抽样rowkey,然后生成一组splitKeys
	HashChoreWoker worker = new HashChoreWoker(1000000, 10);

	@Test
	public void testHash() {
		// 1.创建split计算器，用于从抽样数据中生成一个比较合适的splitKeys
		byte[][] splitKeys = worker.calcSplitKeys();
		hbase.createTable("user", splitKeys);
		TableName tableName = TableName.valueOf("user");
		// 插入1亿条记录，看数据分布
		for (int i = 0; i < 100000000; i++) {
			Put put = new Put(rkGen.nextId());
			hbase.put(tableName, put);
		}

		hbase.report(tableName);
	}

	@Test
	public void testPartition() {
		// default 20 partitions.
		PartitionRowKeyManager rkManager = new PartitionRowKeyManager();

		byte[][] splitKeys = rkManager.calcSplitKeys();

		hbase.createTable("person", splitKeys);

		TableName tableName = TableName.valueOf("person");
		// 插入1亿条记录，看数据分布
		for (int i = 0; i < 100000000; i++) {
			Put put = new Put(rkManager.nextId());
			hbase.put(tableName, put);
		}

		hbase.report(tableName);
	}

	@Test
	public void testHashAndCreateTable() throws Exception {
		HashChoreWoker worker = new HashChoreWoker(1000000, 10);
		byte[][] splitKeys = worker.calcSplitKeys();

		HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
		TableName tableName = TableName.valueOf("hash_split_table");

		if (admin.tableExists(tableName)) {
			try {
				admin.disableTable(tableName);
			} catch (Exception e) {
			}
			admin.deleteTable(tableName);
		}

		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		HColumnDescriptor columnDesc = new HColumnDescriptor(Bytes.toBytes("info"));
		columnDesc.setMaxVersions(1);
		tableDesc.addFamily(columnDesc);

		admin.createTable(tableDesc, splitKeys);

		admin.close();
	}

	@Test
	public void testPartitionAndCreateTable() throws Exception {

		PartitionRowKeyManager rkManager = new PartitionRowKeyManager();
		// 只预建10个分区
		rkManager.setPartition(10);

		byte[][] splitKeys = rkManager.calcSplitKeys();

		HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
		TableName tableName = TableName.valueOf("partition_split_table");

		if (admin.tableExists(tableName)) {
			try {
				admin.disableTable(tableName);

			} catch (Exception e) {
			}
			admin.deleteTable(tableName);
		}

		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		HColumnDescriptor columnDesc = new HColumnDescriptor(Bytes.toBytes("info"));
		columnDesc.setMaxVersions(1);
		tableDesc.addFamily(columnDesc);

		admin.createTable(tableDesc, splitKeys);

		admin.close();
	}

}
