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
public class GPMSourceIVARowCallbackHandler 
	implements RowCallbackHandler
{
	static Logger		logger = LogManager.getLogger(GPMSourceIVARowCallbackHandler.class);

	private GPMSourceIVAUniversePartition		partition;
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	thePartition
	 *************************************************************************************
	 */
	public GPMSourceIVARowCallbackHandler(GPMSourceIVAUniversePartition thePartition)
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
		
		try {

			aValue = theResultSet.getString("is_active");
			
			aGPMSrcIVA = new GPMSourceIVA(this.partition);
			aGPMSrcIVA.srcKey = theResultSet.getLong("alt_key_src");
			if (!"Y".equalsIgnoreCase(aValue)) {
				this.partition.inactiveSrcTable.put(aGPMSrcIVA.srcKey, true);
				return;
			}

			aGPMSrcIVA.prodKey = theResultSet.getLong("alt_key_prod");
			aGPMSrcIVA.ivaKey = theResultSet.getLong("alt_key_iva");
			aGPMSrcIVA.ftaEnabledFlag = "Y".equalsIgnoreCase(theResultSet.getString("fta_enabled_flag"));
			aGPMSrcIVA.ftaCode = theResultSet.getString("fta_code");
			aGPMSrcIVA.ctryOfOrigin = theResultSet.getString("ctry_of_origin");
			
			aValue = theResultSet.getString("system_decision");
			if (aValue != null) {
				aGPMSrcIVA.systemDecision = STPDecisionEnum.valueOf(aValue);
			}

			aValue = theResultSet.getString("final_decision");
			if (aValue != null) {
				aGPMSrcIVA.finalDecision = STPDecisionEnum.valueOf(aValue);
			}
			
			aGPMSrcIVA.ctryOfImport = theResultSet.getString("ctry_of_import");
			aGPMSrcIVA.effectiveFrom = theResultSet.getTimestamp("effective_from");
			aGPMSrcIVA.effectiveTo = theResultSet.getTimestamp("effective_to");
			aGPMSrcIVA.ivaCode = theResultSet.getString("iva_code");
			
			if (aGPMSrcIVA.ftaEnabledFlag) {
				this.partition.addGPMSourceIVA(aGPMSrcIVA);
			}
			else {
				MessageFormatter.trace(logger, "processRow", "IVA [{0}]: FTA is not enabled", aGPMSrcIVA.ivaKey);
			}
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
