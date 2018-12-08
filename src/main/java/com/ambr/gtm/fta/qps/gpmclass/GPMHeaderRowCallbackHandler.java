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
		GPMCountry	aGPMCtry;
		Long		aProdKey;

		try {
			aGPMCtry = new GPMCountry();
			aProdKey = theResultSet.getLong("alt_key_prod");
			aGPMCtry.ctryCode = theResultSet.getString("ctry_code");
			aGPMCtry.ctryOfOrigin = theResultSet.getString("ctry_of_origin");
			
			this.partition.addCountry(aProdKey, aGPMCtry);
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