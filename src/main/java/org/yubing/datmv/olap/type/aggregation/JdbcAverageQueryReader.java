package org.yubing.datmv.olap.type.aggregation;

import org.yubing.datmv.core.DataType;
import org.yubing.datmv.core.internal.SimpleDataField;
import org.yubing.datmv.util.DataSource;

/**
 *	JDBC 平均数查询器
 *
 * @author Wu CongWen
 * @email yubing744@163.com
 * @date 2013-7-30
 */
public class JdbcAverageQueryReader extends JdbcSummationQueryReader {
	
	public JdbcAverageQueryReader(DataSource dataSource, String dialectClass, String type,
			String typeVal) {
		super(dataSource, dialectClass, type, typeVal);
	}
	
	public JdbcAverageQueryReader(DataSource dataSource, String dialectClass,
			String type, String tableName, String sql) {
		super(dataSource, dialectClass, type, tableName, sql);
	}

	protected SimpleDataField getAggregatoinResult() {
		SimpleDataField result = new SimpleDataField(buildResultName(), DataType.NUMBER);
		
		if (this.totalLine != 0) {
			result.setData(this.summation().doubleValue() / this.totalLine);
		} else {
			result.setData(0);
		}
		
		return result;
	}
	
	protected String buildResultName() {
		return this.getTableName() + "_" + this.getSumColumn() + "_average";
	}
}

