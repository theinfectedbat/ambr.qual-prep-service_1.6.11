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
public class GPMClassificationRowCallbackHandler
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
	public GPMClassificationRowCallbackHandler(GPMClassificationUniversePartition thePartition) 
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
		GPMClassification	aGPMClass;

		try {
			aGPMClass = new GPMClassification();
			aGPMClass.cmplKey = theResultSet.getLong("alt_key_cmpl");
			aGPMClass.ctryCode = theResultSet.getString("ctry_code");
			aGPMClass.ctryKey = theResultSet.getLong("alt_key_ctry");
			aGPMClass.effectiveFrom = theResultSet.getTimestamp("effective_from");
			aGPMClass.effectiveTo = theResultSet.getTimestamp("effective_to");
			aGPMClass.imHS1 = theResultSet.getString("im_hs1");
			aGPMClass.prodKey = theResultSet.getLong("alt_key_prod");
			aGPMClass.isActive = theResultSet.getString("is_active");
			
			this.partition.addClassification(aGPMClass);
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