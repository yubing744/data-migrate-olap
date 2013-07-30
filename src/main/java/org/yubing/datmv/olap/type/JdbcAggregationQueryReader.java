package org.yubing.datmv.olap.type;

import org.yubing.datmv.core.DataField;
import org.yubing.datmv.core.Record;
import org.yubing.datmv.core.RecordPage;
import org.yubing.datmv.core.internal.SimpleRecord;
import org.yubing.datmv.core.internal.SimpleRecordPage;
import org.yubing.datmv.util.DataSource;

public abstract class JdbcAggregationQueryReader extends JdbcQueryReader {

	protected boolean hasQuery = false;
	
	public JdbcAggregationQueryReader(DataSource dataSource, String dialectClass, String type,
			String typeVal) {
		super(dataSource, dialectClass, type, typeVal);
	}
	
	public JdbcAggregationQueryReader(DataSource dataSource,
			String dialectClass, String type, String tableName, String sql) {
		super(dataSource, dialectClass, type, tableName, sql);
	}

	public boolean hasNext() {
		if (!hasQuery) {
			return true;
		}
		
		return false;
	}
	
	public RecordPage readPage(int pageSize) {
		RecordPage page = new SimpleRecordPage(pageSize);
		
		Record record = new SimpleRecord();
		DataField cellData = getAggregatoinResult();
		record.addDataField(cellData);
		page.writeRecord(record);
		
		hasQuery = true;
		return page;
	}

	/**
	 * 获取聚集结果
	 * 
	 * @return
	 */
	protected abstract DataField getAggregatoinResult();
}
