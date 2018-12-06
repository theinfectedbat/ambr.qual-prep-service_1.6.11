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
class BOMComponentDataExtensionRowCallbackHandler
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
	BOMComponentDataExtensionRowCallbackHandler(BOMUniversePartition thePartition) 
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
			String											aGroupName;
			BOMComponentDataExtension 						aBomCompDE;
			DataRecordUtility<BOMComponentDataExtension> 	aUtil;
			
			aGroupName = theResultSet.getString("group_name");
			aBomCompDE = new BOMComponentDataExtension(aGroupName, this.bomPartition.dataExtensionRepos);
			
			aUtil = new DataRecordUtility<>(aBomCompDE);
			aUtil.extractDataFromResultSet(aBomCompDE, theResultSet);
			
			this.bomPartition.addComponentDataExtension(aBomCompDE);
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