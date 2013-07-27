package org.yubing.datmv.olap.type;

import java.io.OutputStream;

import jxl.write.Label;
import jxl.write.WriteException;
import jxl.write.biff.RowsExceededException;

import org.yubing.datmv.core.DataField;
import org.yubing.datmv.core.RecordPage;
import org.yubing.datmv.type.excel.ExcelWriter;

public class OLAPExcelWriter extends ExcelWriter {

	public OLAPExcelWriter(OutputStream outputStream) {
		super(outputStream);
	}

	protected Size writeDataField(DataField dataField, int colNum, int writeLine)
			throws RowsExceededException, WriteException {
		Size max = new Size(0, 0);
		
		Object data = dataField.getData();
		
		if (data instanceof RecordPage) {
			RecordPage page = (RecordPage)data;
			super.writePage(page, new Size(colNum, writeLine));
		}
		
		Label labelCF = new Label(colNum, writeLine, String.valueOf(data));
		ws.addCell(labelCF);
		
		return max;
	}
	
}
