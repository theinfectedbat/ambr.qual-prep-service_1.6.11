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
class BOMDataExtensionRowCallbackHandler
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
	BOMDataExtensionRowCallbackHandler(BOMUniversePartition thePartition) 
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
			String								aGroupName;
			BOMDataExtension 					aBomDE;
			DataRecordUtility<BOMDataExtension> aUtil;
			
			aGroupName = theResultSet.getString("group_name");
			aBomDE = new BOMDataExtension(aGroupName, this.bomPartition.dataExtensionRepos);
			
			aUtil = new DataRecordUtility<>(aBomDE);
			aUtil.extractDataFromResultSet(aBomDE, theResultSet);
			
			this.bomPartition.addDataExtension(aBomDE);
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