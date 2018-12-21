package com.ambr.gtm.fta.qps.ptnr;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowCallbackHandler;

import com.ambr.gtm.fta.qps.exception.MaxRowsReachedException;
import com.ambr.gtm.fta.qps.qualtx.universe.QualTXDetail;
import com.ambr.platform.rdbms.util.DataRecordUtility;

/**
 *************************************************************************************
 * <P>
 * </P>
 *************************************************************************************
 */
class PartnerDetailRowCallbackHandler
	implements RowCallbackHandler
{
	private final PartnerDetailUniversePartition 	partition;
	private DataRecordUtility<PartnerDetail>		dataRecUtil;

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	thePartition
	 *************************************************************************************
	 */
	PartnerDetailRowCallbackHandler(PartnerDetailUniversePartition thePartition)
		throws Exception
	{
		this.partition = thePartition;
		this.dataRecUtil = new DataRecordUtility<>(PartnerDetail.class);
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
			PartnerDetail	aPtnrDetail;
			aPtnrDetail = new PartnerDetail();

			this.dataRecUtil.extractDataFromResultSet(aPtnrDetail, theResultSet);
			
			this.partition.addPartnerDetail(aPtnrDetail);
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