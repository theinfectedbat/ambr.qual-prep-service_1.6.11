package com.ambr.gtm.fta.qps.bom;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowCallbackHandler;

import com.ambr.gtm.fta.qps.exception.MaxRowsReachedException;
import com.ambr.platform.rdbms.util.DataRecordUtility;

/**
 *************************************************************************************
 * <P>
 * </P>
 *************************************************************************************
 */
class BOMRowCallbackHandler
	implements RowCallbackHandler
{
	private final BOMUniversePartition 	bomPartition;
	private DataRecordUtility<BOM>		dataRecUtil;

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	thePartition
	 *************************************************************************************
	 */
	BOMRowCallbackHandler(BOMUniversePartition thePartition)
		throws Exception
	{
		this.bomPartition = thePartition;
		this.dataRecUtil = new DataRecordUtility<>(BOM.class);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theResultSet
	 *************************************************************************************
	 */
	@Override
	public void processRow(ResultSet theResultSet) 
		throws SQLException 
	{

		try {
			BOM	aBOM;
			aBOM = new BOM(this.bomPartition);

			this.dataRecUtil.extractDataFromResultSet(aBOM, theResultSet);
			
			this.bomPartition.addBOM(aBOM);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}

		this.bomPartition.rowCount++;
		if (this.bomPartition.maxCursorDepth > 0) {
			if (this.bomPartition.rowCount >= this.bomPartition.maxCursorDepth) {
				throw new MaxRowsReachedException();
			}
		}
	}
}