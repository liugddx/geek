/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 *
 * @author: gdliu3
 * @Date: 2022/3/19 17:46
 */
package com.liugddx.hbase.test;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;

/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 * @Title HbaseTest
 * @Project geek
 * @Description TODO
 * @author gdliu3
 * @date 2022/3/19 17:46
 */
@Component
@Slf4j
public class HbaseTest {

	private static final String CF_INFO = "info";

	private static final String CF_INFO_STUDENT_ID = "student_id";

	private static final String CF_INFO_CLASS = "class";

	private static final String CF_SCORE_UNDERSTANDING = "understanding";

	private static final String CF_SCORE_PROGRAMMING = "programming";

	private static final String CF_SCORE = "score";

	private static final String NAMESPACE = "liugdx";

	private static final String TABLE_NAME = "student";


	@PostConstruct
	public void hbaseTest() throws IOException {
		//连接
		Connection connection = this.connection();
		//liugdx:student
		TableName tableName = TableName.valueOf(NAMESPACE+":"+TABLE_NAME);

		this.createTable(connection,tableName,CF_INFO,CF_SCORE);

		List<Student> studentIds = new ArrayList<>();
		studentIds.add(new Student("Tom","20210000000001","1","75","82"));
		studentIds.add(new Student("Jerry","20210000000001","1","85","67"));
		studentIds.add(new Student("Jack","20210000000001","1","80","80"));
		studentIds.add(new Student("Rose","20210000000001","1","60","61"));
		studentIds.add(new Student("刘广东","G20210616020055","1","90","90"));


		for (Student student : studentIds){
			put(connection,tableName,student.getName(), CF_INFO, CF_INFO_STUDENT_ID, student.getStudentId());
			put(connection,tableName,student.getName(), CF_INFO, CF_INFO_CLASS, student.getClazz());
			put(connection,tableName,student.getName(), CF_SCORE, CF_SCORE_UNDERSTANDING, student.getUnderstanding());
			put(connection,tableName,student.getName(), CF_SCORE, CF_SCORE_PROGRAMMING, student.getProgramming());

			log.info("student id is {}",this.getCell(connection, tableName, student.getName(), CF_INFO, CF_INFO_STUDENT_ID));
			log.info("class is {}",this.getCell(connection, tableName, student.getName(), CF_INFO, CF_INFO_CLASS));
			log.info("understanding is {}",this.getCell(connection, tableName, student.getName(), CF_SCORE, CF_SCORE_UNDERSTANDING));
			log.info("programming is {}",this.getCell(connection, tableName, student.getName(), CF_SCORE, CF_SCORE_PROGRAMMING));

			log.info("student info is {}",this.getRow(connection, tableName, student.getName()));
		}

		List<Map<String, String>> dataList = this.scan(connection, tableName, "Tom", "刘广东");
		log.info("扫描表结果-:\n{}", JSON.toJSONString(dataList));


		//删除表
		this.deleteTable(connection, tableName);

		}

	private Connection connection(){
		try {
			//获取配置
			Configuration configuration = this.getConfiguration();
			//检查配置
			HBaseAdmin.available(configuration);
			return ConnectionFactory.createConnection(configuration);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 获取配置
	 *
	 * @return
	 */
	private  Configuration getConfiguration() {
		try {
			Properties props = PropertiesLoaderUtils.loadAllProperties("hbase.properties");
			String clientPort = props.getProperty("hbase.zookeeper.property.clientPort");
			String quorum = props.getProperty("hbase.zookeeper.quorum");

			log.info("connect to zookeeper {}:{}", quorum, clientPort);

			Configuration config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.property.clientPort", clientPort);
			config.set("hbase.zookeeper.quorum", quorum);
			return config;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void createTable(Connection connection, TableName tableName, String... columnFamilies) throws IOException {
		Admin admin = null;
		try {
			admin = connection.getAdmin();
			if (admin.tableExists(tableName)) {
				log.warn("table:{} exists!", tableName.getName());
			} else {
				TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
				for (String columnFamily : columnFamilies) {
					builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(columnFamily));
				}
				admin.createTable(builder.build());
				log.info("create table:{} success!", tableName.getName());
			}
		} finally {
			if (admin != null) {
				admin.close();
			}
		}
	}

	private void put(Connection connection, TableName tableName,
	                 String rowKey, String columnFamily, String column, String data) throws IOException {

		Table table = null;
		try {
			table = connection.getTable(tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
			table.put(put);
		} finally {
			if (table != null) {
				table.close();
			}
		}
	}

	public String getCell(Connection connection, TableName tableName, String rowKey, String columnFamily, String column) throws IOException {
		Table table = null;
		try {
			table = connection.getTable(tableName);
			Get get = new Get(Bytes.toBytes(rowKey));
			get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));

			Result result = table.get(get);
			List<Cell> cells = result.listCells();

			if (CollectionUtils.isEmpty(cells)) {
				return null;
			}
			return new String(CellUtil.cloneValue(cells.get(0)), "UTF-8");
		} finally {
			if (table != null) {
				table.close();
			}
		}
	}

	public Map<String, String> getRow(Connection connection, TableName tableName, String rowKey) throws IOException {
		Table table = null;
		try {
			table = connection.getTable(tableName);
			Get get = new Get(Bytes.toBytes(rowKey));

			Result result = table.get(get);
			List<Cell> cells = result.listCells();

			if (CollectionUtils.isEmpty(cells)) {
				return Collections.emptyMap();
			}
			Map<String, String> objectMap = new HashMap<>();
			for (Cell cell : cells) {
				String qualifier = new String(CellUtil.cloneQualifier(cell));
				String value = new String(CellUtil.cloneValue(cell), "UTF-8");
				objectMap.put(qualifier, value);
			}
			return objectMap;
		} finally {
			if (table != null) {
				table.close();
			}
		}
	}

	public List<Map<String, String>> scan(Connection connection, TableName tableName, String rowkeyStart, String rowkeyEnd) throws IOException {
		Table table = null;
		try {
			table = connection.getTable(tableName);
			ResultScanner rs = null;
			try {
				Scan scan = new Scan();
				if (!StringUtils.isEmpty(rowkeyStart)) {
					scan.withStartRow(Bytes.toBytes(rowkeyStart));
				}
				if (!StringUtils.isEmpty(rowkeyEnd)) {
					scan.withStopRow(Bytes.toBytes(rowkeyEnd));
				}
				rs = table.getScanner(scan);

				List<Map<String, String>> dataList = new ArrayList<>();
				for (Result r : rs) {
					Map<String, String> objectMap = new HashMap<>();
					for (Cell cell : r.listCells()) {
						String qualifier = new String(CellUtil.cloneQualifier(cell));
						String value = new String(CellUtil.cloneValue(cell), "UTF-8");
						objectMap.put(qualifier, value);
					}
					dataList.add(objectMap);
				}
				return dataList;
			} finally {
				if (rs != null) {
					rs.close();
				}
			}
		} finally {
			if (table != null) {
				table.close();
			}
		}
	}

	public void deleteTable(Connection connection, TableName tableName) throws IOException {
		try (Admin admin = connection.getAdmin()) {
			if (admin.tableExists(tableName)) {
				//现执行disable
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}
		}
	}
}
