package org.yubing.datmv.olap.query.simple;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.yubing.datmv.olap.query.Condition;

/**
 *	简单查询工具
 *
 * @author Wu CongWen
 * @email yubing744@163.com
 * @date 2013-7-10
 */
public class SimpleQueryUtils {

	/**
	 * 构建简单查询条件
	 * 
	 * @param parameterMap
	 * @return
	 */
	public static Map<String, Object> buildSimpleQueryConditions(Map<String, Object> parameterMap) {
		Map<String, Object> conditions = new LinkedHashMap<String, Object>();
		appendSimpleQueryConditions(conditions, parameterMap);
		return conditions;
	}
	
	/**
	 * 附加简单查询条件
	 * 
	 * @param conditions
	 * @param parameterMap
	 */
	public static void appendSimpleQueryConditions(Map<String, Object> conditions, Map<String, Object> parameterMap) {
		for (Iterator<Entry<String, Object>> iterator = parameterMap.entrySet().iterator(); iterator.hasNext();) {
			Entry<String, Object> entry = iterator.next();
			
			if (entry != null) {
				String key = entry.getKey();
				Object value = entry.getValue();
				
				if (value!= null && value.getClass().isArray()) {
					appendSimpleQueryCondition(conditions, key, (Object[])value);
				} else if (value instanceof Condition) { 
					Condition c = (Condition)value;
					conditions.put(c.getName(), c);
				} else {
					appendSimpleQueryCondition(conditions, key, value);
				}
			}
		}
	}

	/**
	 * 附加单个简单查询条件
	 * 
	 * @param conditions
	 * @param key
	 * @param params
	 */
	public static void appendSimpleQueryCondition(Map<String, Object> conditions, String key, Object[] params) {
		if (key.startsWith(SimpleQueryCondition.SQC_PRIFIX)) {
			Condition c = new SimpleQueryCondition(key, params);
			conditions.put(c.getName(), c);
		}
	}
	
	/**
	 * 附加单个简单查询条件
	 * 
	 * @param conditions
	 * @param key
	 * @param params
	 */
	public static void appendSimpleQueryCondition(Map<String, Object> conditions, String key, Object val) {
		appendSimpleQueryCondition(conditions, key, new Object[]{val});
	}
}
