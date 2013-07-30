package org.yubing.datmv.olap.type.aggregation;

import org.yubing.datmv.core.DataField;
import org.yubing.datmv.core.DataType;
import org.yubing.datmv.core.internal.SimpleDataField;
import org.yubing.datmv.olap.type.JdbcAggregationQueryReader;
import org.yubing.datmv.util.DataSource;

/**
 *	JDBC 个数查询读取器
 *
 * @author Wu CongWen
 * @email yubing744@163.com
 * @date 2013-7-30
 */
public class JdbcCountQueryReader extends JdbcAggregationQueryReader {

	public JdbcCountQueryReader(DataSource dataSource, String dialectClass, String type,
			String typeVal) {
		super(dataSource, dialectClass, type, typeVal);
	}
	
	public JdbcCountQueryReader(DataSource dataSource, String dialectClass,
			String type, String tableName, String sql) {
		super(dataSource, dialectClass, type, tableName, sql);
	}

	protected DataField getAggregatoinResult() {
		SimpleDataField result = new SimpleDataField(this.getTableName() + "_count", DataType.INTEGER);
		result.setData(this.totalLine);
		return result;
	}
}
