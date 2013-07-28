package org.yubing.datmv.olap.type;

import java.io.OutputStream;

import jxl.write.WriteException;
import jxl.write.biff.RowsExceededException;

import org.yubing.datmv.core.DataField;
import org.yubing.datmv.core.RecordPage;
import org.yubing.datmv.type.excel.ExcelWriter;

public class OLAPExcelWriter extends ExcelWriter {

	public OLAPExcelWriter(OutputStream outputStream) {
		super(outputStream);
	}

	protected Size writeDataField(DataField dataField, int colNum, int rowNum, boolean transpose)
			throws RowsExceededException, WriteException {
		Size size = new Size(0, 0);
		
		Object data = dataField.getData();
		
		if (data instanceof RecordPage) {
			RecordPage page = (RecordPage)data;
			size = super.writePage(page, colNum, rowNum, transpose);
		} else {
			size = super.writeDataField(dataField, colNum, rowNum, transpose);
		}

		return size;
	}
	
}
