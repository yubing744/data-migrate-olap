package org.yubing.datmv.olap.type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.yubing.datmv.core.MigrateContext;
import org.yubing.datmv.olap.query.Condition;
import org.yubing.datmv.olap.query.simple.SimpleQueryUtils;
import org.yubing.datmv.type.jdbc.Dialect;
import org.yubing.datmv.type.jdbc.JdbcReader;
import org.yubing.datmv.util.DBHelper;
import org.yubing.datmv.util.DataSource;
import org.yubing.datmv.util.ReflectUtils;

public class JdbcQueryReader extends JdbcReader {

	protected Map<String, Object> conditions;
	protected List<Object> baseSqlArgs;
	
	public JdbcQueryReader(DataSource dataSource, String dialectClass, String type,
			String typeVal) {
		super(dataSource, dialectClass, type, typeVal);
	}

	public JdbcQueryReader(DataSource dataSource, String dialectClass, String type,
			String tableName, String sql) {
		super(dataSource, dialectClass, "sql", sql);
		this.tableName = tableName;
	}
	
	public void open(MigrateContext context) {
		dbHelper = new DBHelper(dataSource);
		dialect = (Dialect)ReflectUtils.newInstance(dialectClass);
		
		if ("sql".equals(type)) {
			baseSqlArgs = new ArrayList<Object>();
			Map<String, Object> model = new HashMap<String, Object>();
			
			for (Iterator it = context.getParameterMap().entrySet().iterator(); it.hasNext();) {
				Entry entry = (Entry) it.next();
				model.put((String)entry.getKey(), entry.getValue());
			}
			
			model.putAll(context.getAttributeMap());
			
			baseSql = handleSqlRefVal(model, sql, baseSqlArgs);
		}
		
		conditions = new HashMap<String, Object>();
		SimpleQueryUtils.appendSimpleQueryConditions(conditions, context.getAttributeMap());
		
		totalLine = findTotalLine();
	}
	
	public String handleSqlRefVal(Map<String, Object> context, String value, List<Object> baseSqlArgs) {
		String captureKeyAndConstraint = "\\$\\{([a-zA-Z_0-9.]+)\\}";

		Matcher matcher = Pattern.compile(captureKeyAndConstraint).matcher(value);
		StringBuffer sb = new StringBuffer();
		
		while (matcher.find()) {
			String val = matcher.group(1);
			Object argVal = context.get(val);
			if (argVal != null) {
				matcher.appendReplacement(sb, "?");
				baseSqlArgs.add(argVal);
			} else {
				throw new RuntimeException("ref for ${" + val + "} not define!");
			}
		}
		
		matcher.appendTail(sb);
		return sb.toString();
	}
	
	protected int findTotalLine() {
		StringBuilder sql = new StringBuilder();
		List<Object> args = new ArrayList<Object>();
		
		sql.append(String.format("select count(%s.id) from %s %s ", getMainTableAlias(), buildMainView(args), getMainTableAlias()));
		appendQueryConditionSQL(sql, args, conditions); 
		
		int result = dbHelper.queryForInt(sql.toString(), args.toArray(new Object[args.size()]));
		return result;
	}

	protected String buildMainView(List<Object> args) {
		if ("sql".equals(type)) {
			args.addAll(this.baseSqlArgs);
			return "(" + this.baseSql + ")";
		} else {
			return this.getTableName();
		}
	}

	protected List<Map<String, Object>> queryByPage(int pageSize, int startLine) {
		StringBuilder sql = new StringBuilder();
		List<Object> args = new ArrayList<Object>();
		
		sql.append(String.format("select %s.* from %s %s ", getMainTableAlias(), buildMainView(args), getMainTableAlias()));
		appendQueryConditionSQL(sql, args, conditions); 
		
		appendOrderArgs(sql);
		appendPageArgs(sql, args, startLine, pageSize);
		
		return dbHelper.queryBySQL(sql.toString(), args.toArray(new Object[args.size()]));
	}
	
	/**
	 * 附加查询条件
	 * 
	 * @param sql
	 * @param args
	 * @param conditions
	 */
	protected void appendQueryConditionSQL(StringBuilder sql, List<Object> args, Map<String, Object> conditions) {
		if (conditions != null) {
			// 1。 关联表
			for (Iterator<Entry<String, Object>> iterator = conditions.entrySet().iterator(); iterator.hasNext();) {
				Entry<String, Object> entry = iterator.next();
				Object condition = entry.getValue();
				
				if (condition instanceof Condition) {
					Condition c = (Condition)condition;
					
					c.joinTable(getMainTableAlias(), sql, args);
				}
			}
			
			appendCommonsCondition(sql, args);
	
			// 2. 附加条件
			for (Iterator<Entry<String, Object>> iterator = conditions.entrySet().iterator(); iterator.hasNext();) {
				Entry<String, Object> entry = iterator.next();
				Object condition = entry.getValue();
				
				if (condition instanceof Condition) {
					Condition c = (Condition)condition;
					
					c.appendCondition(getMainTableAlias(), sql, args);
				}
			}
		}
	}
	
	/**
	 * 附加公共查询条件
	 * 
	 * @param sql
	 * @param args
	 */
	protected void appendCommonsCondition(StringBuilder sql, List<Object> args) {
		sql.append("where 1=1 ");
	}
	
	/**
	 * 附加排序字段
	 * 
	 * @param sql
	 */
	protected void appendOrderArgs(StringBuilder sql) {
		sql.append(String.format("order by %s.%s desc ", this.getMainTableAlias(), this.getOrderColumnName()));
	}
	
	/**
	 * 附加分页参数
	 * 
	 * @param sql
	 * @param args
	 * @param startIndex
	 * @param count
	 */
	protected void appendPageArgs(StringBuilder sql, List<Object> args,
			Integer startIndex, Integer count) {
		if (count!= null) {
			sql.append("limit ");
			
			if (startIndex != null) {
				sql.append("?,? ");
				args.add(startIndex);
				args.add(count);
			} else {
				sql.append("0,? ");
				args.add(count);
			}
		}
	}
	
	private String mainTableAlias = null;

	/**
	 * 获取主表别名
	 * 
	 * @return
	 */
	protected String getMainTableAlias() {
		if (mainTableAlias == null) {
			mainTableAlias = this.getTableName().toLowerCase() + "_" + String.valueOf(System.currentTimeMillis());
		}
			
		return mainTableAlias;
	}
	
	/**
	 * 获取排序的数据库列名称
	 * 
	 * @return
	 */
	protected String getOrderColumnName() {
		return "ts_create";
	}
	
	/**
	 * 获取实体对应的数据库表名称
	 * 
	 * @return
	 */
	protected String getTableName() {
		if (!StringUtils.isBlank(this.tableName)) {
			return this.tableName;
		}
		
		return "temp_view";
	}
}
