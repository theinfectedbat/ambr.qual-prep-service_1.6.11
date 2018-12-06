package com.ambr.gtm.fta.qps.qualtx.universe;

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
class QualTXDetailRowCallbackHandler
	implements RowCallbackHandler
{
	private final QualTXDetailUniversePartition 	partition;
	private DataRecordUtility<QualTXDetail>			dataRecUtil;

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	thePartition
	 *************************************************************************************
	 */
	QualTXDetailRowCallbackHandler(QualTXDetailUniversePartition thePartition)
		throws Exception
	{
		this.partition = thePartition;
		this.dataRecUtil = new DataRecordUtility<>(QualTXDetail.class);
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
			QualTXDetail	aQualTXDetail;
			aQualTXDetail = new QualTXDetail();

			this.dataRecUtil.extractDataFromResultSet(aQualTXDetail, theResultSet);
			
			this.partition.addQualTXDetail(aQualTXDetail);
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