package org.yubing.datmv.olap.config;

import java.util.Map;

import org.w3c.dom.Element;
import org.yubing.datmv.config.xml.XmlMigrateConfig;
import org.yubing.datmv.config.xml.parser.argment.MigrateConfigMappingHandlerArgmentsParser;
import org.yubing.datmv.config.xml.parser.util.MapParser;
import org.yubing.datmv.util.DocumentUtils;

public class MeasureConfigMappingHandlerArgmentsParser extends MigrateConfigMappingHandlerArgmentsParser {
	public static final String ATTR_TRANSPOSE = "attr-mapping";
	
	public Object[] parserArgs(XmlMigrateConfig xmlMigrateConfig,
			Element element) {
		Object[] args = super.parserArgs(xmlMigrateConfig, element);
		MapParser parger = new MapParser();
		
		Element mapEle = DocumentUtils.findOneElementByTagName(element, ATTR_TRANSPOSE);
		if (mapEle != null) {
			Map<String, String> map = parger.parse(mapEle);
			return new Object[]{args[0], args[1], map};
		}
		
		return args;
	}
}
