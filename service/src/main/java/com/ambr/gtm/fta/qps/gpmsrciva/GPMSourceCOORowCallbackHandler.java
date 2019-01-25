package com.ambr.gtm.fta.qps.gpmsrciva;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.RowCallbackHandler;

import com.ambr.gtm.fta.qps.exception.MaxRowsReachedException;
import com.ambr.platform.utils.log.MessageFormatter;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GPMSourceCOORowCallbackHandler 
	implements RowCallbackHandler
{
	static Logger		logger = LogManager.getLogger(GPMSourceCOORowCallbackHandler.class);

	private GPMSourceIVAUniversePartition		partition;
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	thePartition
	 *************************************************************************************
	 */
	public GPMSourceCOORowCallbackHandler(GPMSourceIVAUniversePartition thePartition)
		throws Exception
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
		GPMSourceIVA		aGPMSrcIVA;
		String				aValue;
		Long				aProdKey;
		Long				aProdSrcKey;
		String				aCOO;
		
		try {
			
			aValue = theResultSet.getString("is_active");
			aGPMSrcIVA = new GPMSourceIVA(this.partition);
			aProdKey = theResultSet.getLong("alt_key_prod");
			aProdSrcKey = theResultSet.getLong("alt_key_src");
			aCOO = theResultSet.getString("ctry_of_origin");

			if (!"Y".equalsIgnoreCase(aValue)) {
				this.partition.inactiveSrcTable.put(aProdSrcKey, true);
				return;
			}
			
			this.partition.addGPMSourceCOO(aProdKey, aProdSrcKey, aCOO);
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
