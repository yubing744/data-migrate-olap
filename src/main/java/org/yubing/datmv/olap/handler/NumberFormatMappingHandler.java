package org.yubing.datmv.olap.handler;

import org.apache.commons.lang.math.NumberUtils;
import org.yubing.datmv.core.ConfigItem;
import org.yubing.datmv.core.DataField;
import org.yubing.datmv.core.DataType;
import org.yubing.datmv.core.MappingHandler;
import org.yubing.datmv.core.RecordContext;
import org.yubing.datmv.core.internal.SimpleDataField;

/**
 *	数字格式化处理器
 *
 * @author Wu CongWen
 * @email yubing744@163.com
 * @date 2013-7-30
 */
public class NumberFormatMappingHandler implements MappingHandler {

	public DataField mapFrom(DataField targetField, ConfigItem configItem,
			RecordContext context) {
		Object data = targetField.getData();
		
		SimpleDataField result = new SimpleDataField(targetField.getName(), DataType.STRING);
		double val = NumberUtils.toDouble(String.valueOf(data));
		result.setData(String.format("%.2f", val));
		
		return result;
	}

}
