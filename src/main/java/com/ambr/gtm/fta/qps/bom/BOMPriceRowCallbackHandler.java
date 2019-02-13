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
class BOMPriceRowCallbackHandler
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
	BOMPriceRowCallbackHandler(BOMUniversePartition thePartition) 
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
			BOMPrice aPrice = new BOMPrice();
			DataRecordUtility<BOMPrice> aUtil = new DataRecordUtility<>(BOMPrice.class);
			aUtil.extractDataFromResultSet(aPrice, theResultSet);
			
			this.bomPartition.addPrice(aPrice);
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