package org.yubing.datmv.olap.query;

import java.util.HashMap;
import java.util.Map;

/**
 *	查询条件工具
 *
 * @author Wu CongWen
 * @email yubing744@163.com
 * @date 2013-7-10
 */
public class QueryConditionUtils {

	/**
	 * 附加查询条件
	 * 
	 * @param conditions
	 * @param condition
	 */
	public static void appendCondition(Map<String, Object> conditions, Condition condition) {
		if (conditions != null && condition != null) {
			conditions.put(condition.getName(), condition);
		}
	}

	/**
	 * 创建查询条件
	 * 
	 * @param condition
	 * @return
	 */
	public static Map<String, Object> createCondition(Condition condition) {
		Map<String, Object> conditions = new HashMap<String, Object>();
		appendCondition(conditions, condition);
		return conditions;
	}
}
