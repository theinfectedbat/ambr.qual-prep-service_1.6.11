package com.ambr.gtm.fta.qps.gpmclaimdetail;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.springframework.jdbc.core.RowCallbackHandler;

import com.ambr.gtm.fta.qps.exception.MaxRowsReachedException;
import com.ambr.gtm.fta.qts.util.Utility;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GPMClaimDetailsRowCallbackHandler 
	implements RowCallbackHandler
{
	private GPMClaimDetailsUniversePartition		partition;
	
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	thePartition
	 *************************************************************************************
	 */
	public GPMClaimDetailsRowCallbackHandler(GPMClaimDetailsUniversePartition thePartition)
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
		GPMClaimDetails		aClaimDetail;
		
		try {
			aClaimDetail = new GPMClaimDetails(this.partition);
			aClaimDetail.alt_key_ivainst = theResultSet.getLong("alt_key_ivainst");
			aClaimDetail.prodSrcIVAKey = theResultSet.getLong("record_key");
			aClaimDetail.fta_code_group = theResultSet.getString("fta_code_group");
			aClaimDetail.loadData(theResultSet);
			
			this.partition.addClaimDetail(aClaimDetail);
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
