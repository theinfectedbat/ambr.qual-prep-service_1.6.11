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
class BOMComponentsRowCallbackHandler
	implements RowCallbackHandler
{
	private final BOMUniversePartition bomPartition;

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	thePartition
	 *************************************************************************************
	 */
	BOMComponentsRowCallbackHandler(BOMUniversePartition thePartition) 
	{
		this.bomPartition = thePartition;
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
			BOMComponent aComponent = new BOMComponent();
			DataRecordUtility<BOMComponent> aUtil = new DataRecordUtility<>(BOMComponent.class);
			aUtil.extractDataFromResultSet(aComponent, theResultSet);
			
			this.bomPartition.addComponent(aComponent);
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