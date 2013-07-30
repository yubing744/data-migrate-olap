package org.yubing.datmv.olap.query.simple;

import java.util.List;

import org.yubing.datmv.olap.query.Condition;


/**
 *	简单查询条件
 *
 * @author Wu CongWen
 * @email yubing744@163.com
 * @date 2013-7-10
 */
public class SimpleQueryCondition implements Condition {

	public static final String SQC_PRIFIX = "sqc.";
	
	/**
	 * 数据库表栏位名称
	 */
	private String columnName;
	
	/**
	 * 条件类型
	 */
	private String type;
	
	/**
	 * 条件参数
	 */
	private Object[] params;
	
	public SimpleQueryCondition(String key, Object[] value) {
		String[] segs = key.substring(SQC_PRIFIX.length()).split("\\.");
		
		if (segs.length >= 1) {
			this.columnName = segs[0];
			String type = "eq";
			
			if (segs.length >= 2) {
				type = segs[1];
			}

			this.type = type;
		}
		
		this.params = value;
	}
	
	public SimpleQueryCondition(String columnName, String type, Object[] value) {
		this.columnName = columnName;
		this.type = type;
		this.params = value;
	}

	public String getName() {
		return SQC_PRIFIX + this.columnName + "." + this.type;
	}

	public void joinTable(String mainTableAlias, StringBuilder sql,
			List<Object> args) {
	}

	public void appendCondition(String mainTableAlias, StringBuilder sql,
			List<Object> args) {
		if (this.params == null || this.params.length==0) {
			return;
		}
		
		String type = this.type;
		Object val = this.params[0];
		
		if ("eq".equals(type)) {
			sql.append(String.format("and %s.%s=? ", mainTableAlias, this.columnName));
			args.add(val);
		} else if("neq".equals(type)) {
			sql.append(String.format("and %s.%s != ? ", mainTableAlias, this.columnName));
			args.add(val);
		} else if("gt".equals(type)) {
			sql.append(String.format("and %s.%s > ? ", mainTableAlias, this.columnName));
			args.add(val);
		} else if("gte".equals(type)) {
			sql.append(String.format("and %s.%s >= ? ", mainTableAlias, this.columnName));
			args.add(val);
		} else if("lt".equals(type)) {
			sql.append(String.format("and %s.%s < ? ", mainTableAlias, this.columnName));
			args.add(val);
		} else if("lte".equals(type)) {
			sql.append(String.format("and %s.%s <= ? ", mainTableAlias, this.columnName));
			args.add(val);
		} else if("contain".equals(type)) {
			val = "%" + val + "%";
			sql.append(String.format("and %s.%s like '%s' ", mainTableAlias, this.columnName, val));
			//args.add("%" + val + "%");
		} else if("not_contain".equals(type)) {
			val = "%" + val + "%";
			sql.append(String.format("and %s.%s not like '%s' ", mainTableAlias, this.columnName, val));
			//args.add("%" + val + "%");
		} else if("start_with".equals(type)) {
			sql.append(String.format("and %s.%s like ? ", mainTableAlias, this.columnName));
			args.add(val + "%");
		} else if("end_with".equals(type)) {
			sql.append(String.format("and %s.%s like ? ", mainTableAlias, this.columnName));
			args.add(val + "%");
		} else if("is_null".equals(type)) {
			if ("1".equals(val) || "true".equals(val)) {
				sql.append(String.format("and isnull(%s.%s) ", mainTableAlias, this.columnName));
			}
		} else if("not_null".equals(type)) {
			if ("1".equals(val) || "true".equals(val)) {
				sql.append(String.format("and not isnull(%s.%s) ", mainTableAlias, this.columnName));
			}
		} else if("is_empty".equals(type)) {
			if ("1".equals(val) || "true".equals(val)) {
				sql.append(String.format("and %s.%s='' ", mainTableAlias, this.columnName));
			}
		} else if("not_empty".equals(type)) {
			if ("1".equals(val) || "true".equals(val)) {
				sql.append(String.format("and not %s.%s='' ", mainTableAlias, this.columnName));
			}
		} else if("between".equals(type)) {
			if (this.params.length == 2) {
				Object val2 = this.params[1];
				
				sql.append(String.format("and (%s.%s > ? and %s.%s < ?) ", mainTableAlias, this.columnName, mainTableAlias, this.columnName));
				args.add(val);
				args.add(val2);
			}
		} else if("not_between".equals(type)) {
			if (this.params.length == 2) {
				Object val2 = this.params[1];
				
				sql.append(String.format("and (%s.%s <= ? or %s.%s >= ?) ", mainTableAlias, this.columnName, mainTableAlias, this.columnName));
				args.add(val);
				args.add(val2);
			}
		} else if("in".equals(type)) {
			sql.append(String.format("and %s.%s in ( %s ) ", mainTableAlias, this.columnName, val));
			//sql.append(String.format("and %s.%s in ( ? )", mainTableAlias, this.columnName));
			//args.add(this.params);
		} else if("not_in".equals(type)) {
			sql.append(String.format("and %s.%s not in ( %s ) ", mainTableAlias, this.columnName, val));
			//sql.append(String.format("and %s.%s not in ( ? )", mainTableAlias, this.columnName));
			//args.add(this.params);
		}
	}

}
