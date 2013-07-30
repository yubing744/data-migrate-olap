package org.yubing.datmv.olap.type.aggregation;

import java.util.ArrayList;
import java.util.List;

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
public class JdbcSummationQueryReader extends JdbcAggregationQueryReader {

	protected String sumColumn = "val";
	
	public JdbcSummationQueryReader(DataSource dataSource, String dialectClass, String type,
			String typeVal) {
		super(dataSource, dialectClass, type, typeVal);
	}
	
	public JdbcSummationQueryReader(DataSource dataSource, String dialectClass,
			String type, String tableName, String sql) {
		super(dataSource, dialectClass, type, tableName, sql);
	}

	protected Number summation() {
		StringBuilder sql = new StringBuilder();
		List<Object> args = new ArrayList<Object>();
		
		sql.append(String.format("select sum(%s.%s) from %s %s ", getMainTableAlias(), getSumColumn(), buildMainView(args), getMainTableAlias()));
		appendQueryConditionSQL(sql, args, conditions); 
		
		return (Number)dbHelper.queryForObject(sql.toString(), args.toArray(new Object[args.size()]));
	}
	
	/**
	 * 获取求和字段名称
	 * 
	 * @return
	 */
	protected String getSumColumn() {
		return this.sumColumn;
	}

	protected DataField getAggregatoinResult() {
		SimpleDataField result = new SimpleDataField(buildResultName(), DataType.NUMBER);
		result.setData(this.summation());
		return result;
	}

	protected String buildResultName() {
		return this.getTableName() + "_" + this.getSumColumn() + "_sum";
	}
}
