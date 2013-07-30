package org.yubing.datmv.olap.handler;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.yubing.datmv.core.ConfigItem;
import org.yubing.datmv.core.DataField;
import org.yubing.datmv.core.Record;
import org.yubing.datmv.core.RecordContext;
import org.yubing.datmv.core.internal.SimpleDataField;
import org.yubing.datmv.core.internal.SimpleRecord;
import org.yubing.datmv.core.internal.SimpleRecordContext;
import org.yubing.datmv.mapping.handler.MigrateConfigMappingHandler;

/**
 * 
 * 度量条件配置
 * 
 * @author yubing
 *
 */
public class MeasureConfigMappingHandler extends MigrateConfigMappingHandler {

	protected Map<String, String> attrMappings = new HashMap<String, String>();
	public void setAttrMappings(Map<String, String> attrMappings) {
		this.attrMappings = attrMappings;
	}

	public MeasureConfigMappingHandler(String migrateConfig, Boolean transpose) {
		super(migrateConfig, transpose);
	}

	public MeasureConfigMappingHandler(String migrateConfig, Boolean transpose, Map<String, String> attrMappings) {
		super(migrateConfig, transpose);
		this.attrMappings = attrMappings;
	}
	
	@Override
	public DataField mapFrom(DataField targetField, ConfigItem configItem, RecordContext context) {
		Record source = context.getSource();
		
		SimpleRecord newSource = new SimpleRecord(source);
		
		for (Iterator<Entry<String, String>> it = attrMappings.entrySet().iterator(); it.hasNext();) {
			Entry<String, String> entry = it.next();
			DataField value = source.getDataField(entry.getKey());
			if (value != null) {
				SimpleDataField newField = new SimpleDataField(entry.getValue(), value.getType());
				newField.setData(value.getData());
				newSource.addDataField(newField);
			}
		}
		
		SimpleRecordContext ctx = new SimpleRecordContext(context.getPageContext(), newSource, context.getTarget());
		
		for (Iterator<Entry<String, String>> it = attrMappings.entrySet().iterator(); it.hasNext();) {
			Entry<String, String> entry = it.next();
			Object value = context.getAttribute(entry.getKey());
			if (value != null) {
				ctx.setAttribute(entry.getValue(), value);
			}
		}
		
		return super.mapFrom(targetField, configItem, ctx);
	}
}
