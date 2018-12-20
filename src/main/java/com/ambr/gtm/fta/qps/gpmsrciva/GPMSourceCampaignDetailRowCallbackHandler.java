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
public class GPMSourceCampaignDetailRowCallbackHandler 
	implements RowCallbackHandler
{
	static Logger		logger = LogManager.getLogger(GPMSourceCampaignDetailRowCallbackHandler.class);

	private GPMSourceIVAUniversePartition		partition;
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	thePartition
	 *************************************************************************************
	 */
	public GPMSourceCampaignDetailRowCallbackHandler(GPMSourceIVAUniversePartition thePartition)
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
		GPMSourceCampaignDetail	aGPMSrcCampDetail;
		Long					aProdKey;
		Long					aProdSrcKey;
		String					aGroupName;
		
		try {
			aGroupName = theResultSet.getString("group_name");
			if ("CAMPAIGN_DETAILS:DEFAULT".equalsIgnoreCase(aGroupName)) {
				// We are only interested in Campaign details data extension

				aProdKey = theResultSet.getLong("alt_key_prod");
				aProdSrcKey = theResultSet.getLong("alt_key_src");
				aGPMSrcCampDetail = new GPMSourceCampaignDetail(theResultSet);
				this.partition.addGPMSourceCampaignDetails(aProdKey, aProdSrcKey, aGPMSrcCampDetail);
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
