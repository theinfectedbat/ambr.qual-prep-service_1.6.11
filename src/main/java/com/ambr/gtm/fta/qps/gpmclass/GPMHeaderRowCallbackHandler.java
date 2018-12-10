package com.ambr.gtm.fta.qps.gpmclass;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowCallbackHandler;

import com.ambr.gtm.fta.qps.exception.MaxRowsReachedException;

/**
 *************************************************************************************
 * <P>
 * </P>
 *************************************************************************************
 */
public class GPMHeaderRowCallbackHandler
	implements RowCallbackHandler
{
	private final GPMClassificationUniversePartition	partition;

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	thePartition
	 *************************************************************************************
	 */
	public GPMHeaderRowCallbackHandler(GPMClassificationUniversePartition thePartition) 
	{
		this.partition = thePartition;
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
		String		aCtryOfOrigin;
		Long		aProdKey;

		try {
			aProdKey = theResultSet.getLong("alt_key_prod");
			aCtryOfOrigin = theResultSet.getString("ctry_of_origin");
			if (aCtryOfOrigin == null) {
				return;
			}
			
			this.partition.addProdCtryOfOrigin(aProdKey, aCtryOfOrigin);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}

		this.partition.rowCount++;
		if (this.partition.maxCursorDepth > 0) {
			if (this.partition.rowCount >= this.partition.maxCursorDepth) {
				throw new MaxRowsReachedException();
			}
		}
	}
}