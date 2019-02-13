package com.ambr.gtm.fta.qps.bom.qualstatus;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import com.ambr.gtm.fta.qps.qualtx.engine.PreparationEngineQueueUniverse;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTX;
import com.ambr.gtm.fta.qps.qualtx.engine.result.BOMStatusManager;
import com.ambr.platform.rdbms.util.DataRecordUtility;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class BOMQualificationStatusGenerator 
{
	static Logger		logger = LogManager.getLogger(BOMQualificationStatusGenerator.class);

	private BOMQualificationStatus				statusObj;
	private PreparationEngineQueueUniverse		queueUniverse;
	private JdbcTemplate						jdbcTemplate;
	private long								bomKey;
	private int									measurmentPeriodInSecs;
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	class QualTXDetailLoader
		implements RowCallbackHandler
	{
		private DataRecordUtility<QualTX> 		dataRecUtil;
		
		/**
		 *************************************************************************************
		 * <P>
		 * </P>
		 *************************************************************************************
		 */
		public QualTXDetailLoader()
			throws Exception
		{
			this.dataRecUtil = new DataRecordUtility<>(QualTX.class);
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
			QualTX		aQualTX;
			
			try {
				aQualTX = new QualTX();
				this.dataRecUtil.extractDataFromResultSet(aQualTX, theResultSet);
				BOMQualificationStatusUtility.addQualTXDetails(BOMQualificationStatusGenerator.this.statusObj, aQualTX);
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}		
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	class TradeLaneLoader
		implements RowCallbackHandler
	{

		/**
		 *************************************************************************************
		 * <P>
		 * </P>
		 * 
		 * @param	the
		 *************************************************************************************
		 */
		@Override
		public void processRow(ResultSet theResultSet) 
			throws SQLException 
		{
			String				aValue;
			String				aFtaEnabledFlag;
			TradeLaneDetail		aTradeLaneDetail;
			
			try {
				aFtaEnabledFlag = theResultSet.getString("fta_enabled_flag");
				if (!"Y".equalsIgnoreCase(aFtaEnabledFlag)) {
					return;
				}
				
				aTradeLaneDetail = new TradeLaneDetail();
				aTradeLaneDetail.ftaCode = theResultSet.getString("fta_code");
				aTradeLaneDetail.coiSpec = theResultSet.getString("ctry_of_import");
				aTradeLaneDetail.qualPeriodStartDate = theResultSet.getTimestamp("effective_from");
				aTradeLaneDetail.qualPeriodEndDate = theResultSet.getTimestamp("effective_to");
				aTradeLaneDetail.orgCode = theResultSet.getString("org_code");
				aTradeLaneDetail.ivaCode = theResultSet.getString("iva_code");
				aTradeLaneDetail.systemDecision = theResultSet.getString("system_decision");
				aTradeLaneDetail.finalDecision = theResultSet.getString("final_decision");

				BOMQualificationStatusGenerator.this.statusObj.addTradeLane(aTradeLaneDetail);
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 *	@param	theQueueUniverse
	 *************************************************************************************
	 */
	public BOMQualificationStatusGenerator(
		PreparationEngineQueueUniverse	theQueueUniverse)
		throws Exception
	{
		this.queueUniverse = theQueueUniverse;
		this.jdbcTemplate = new JdbcTemplate(this.queueUniverse.dataSrc);
		this.measurmentPeriodInSecs = 5;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theBOMKey
	 *************************************************************************************
	 */
	public BOMQualificationStatus generate(long theBOMKey)
		throws Exception
	{
		this.bomKey=theBOMKey;
		this.statusObj = new BOMQualificationStatus(theBOMKey);
		this.loadTradeLaneUniverse();
		this.updateActiveTradeLanes();
		this.loadQualTXDetails();
		this.loadPreparationStatusDetails();
		
		return this.statusObj;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	private void loadPreparationStatusDetails()
		throws Exception
	{
		BOMStatusManager						aStatusMgr;
		QualificationPreparationStatusDetail 	aStatusDetail;
		
		aStatusDetail = new QualificationPreparationStatusDetail();
		try {
			aStatusMgr = this.queueUniverse.qtxPrepProgressMgr.getStatusManager();
			aStatusDetail.statusText = QualificationPreparationStatusEnum.IN_PROGRESS.name();
			aStatusDetail.startTime = aStatusMgr.getStartTime();
			aStatusDetail.endTime = aStatusMgr.getEndTime();
			QualificationPreparationStatusDetailUtility.setDuration(
				aStatusDetail, 
				this.queueUniverse.getThroughputUtility(), 
				this.bomKey
			);
		}
		catch (IllegalStateException e) {
			if (this.queueUniverse.qtxDetailUniverse.getQualTXDetailContainer(this.bomKey) == null) {
				aStatusDetail.statusText = QualificationPreparationStatusEnum.PENDING.name();
			}
			else {
				aStatusDetail.statusText = QualificationPreparationStatusEnum.COMPLETED.name();
			}
		}
		this.statusObj.setPrepStatusDetail(aStatusDetail);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	private void loadQualTXDetails()
		throws Exception
	{
		String	aSqlText;
		
		aSqlText = "select alt_key_qualtx, fta_code, ctry_of_import, effective_from, effective_to, qualified_flg, last_modified_date ";
		aSqlText += "from mdi_qualtx ";
		aSqlText += "where src_key = ?";
		
		this.jdbcTemplate.query(aSqlText, new Object[]{this.statusObj.bomKey}, new QualTXDetailLoader());
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 *	@param	theBOMKey
	 *************************************************************************************
	 */
	private void loadTradeLaneUniverse()
		throws Exception
	{
		String	aSqlText;
		
		aSqlText = "SELECT alt_key_iva, fta_enabled_flag, fta_code, system_decision, final_decision, ctry_of_import, effective_from, effective_to, iva_code, org_code ";
		aSqlText += "from mdi_prod_src_iva ";
		aSqlText += "where alt_key_src = (select prod_src_key from mdi_bom where alt_key_bom = ?)";
		
		this.jdbcTemplate.query(aSqlText, new Object[]{this.statusObj.bomKey}, new TradeLaneLoader());
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 *	@param	theBOMKey
	 *************************************************************************************
	 */
	public BOMQualificationStatusGenerator setBOM(long theBOMKey)
		throws Exception
	{
		this.bomKey = theBOMKey;
		return this;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 *	@param	thePeriodIsSecs
	 *************************************************************************************
	 */
	public BOMQualificationStatusGenerator setMeasurementPeriod(int thePeriodIsSecs)
		throws Exception
	{
		this.measurmentPeriodInSecs = thePeriodIsSecs;
		return this;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public void updateActiveTradeLanes() 
		throws Exception	
	{
		 ArrayList<TradeLaneDetail> aTradeLaneDetailList = BOMQualificationStatusGenerator.this.statusObj.tradeLaneDetailList;
		
		 for(TradeLaneDetail tradeLaneDetail : aTradeLaneDetailList) {
			 if(!queueUniverse.qeConfigCache.isTradeLaneEnabled(tradeLaneDetail.orgCode,tradeLaneDetail.ftaCode,tradeLaneDetail.coiSpec)) {
				 tradeLaneDetail.qualEvalStatus=QualificationEvaluationStatusEnum.EVALUATION_DISABLED;
			 }
		 }
	}
}
