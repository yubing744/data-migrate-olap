package org.yubing.datmv.olap.query;

import java.util.List;

/**
 *	查询条件
 *
 * @author Wu CongWen
 * @email yubing744@163.com
 * @date 2013-7-10
 */
public interface Condition {

	/**
	 * 获取条件名称
	 * 
	 * @return
	 */
	String getName();

	/**
	 * 关联表
	 * @param mainTablealias 主表别名
	 * @param sql 
	 * @param args
	 */
	void joinTable(String mainTablealias, StringBuilder sql, List<Object> args);

	/**
	 * 附加条件
	 * 
	 * @param mainTablealias 主表别名
	 * @param sql
	 * @param args
	 */
	void appendCondition(String mainTablealias, StringBuilder sql, List<Object> args);

}
