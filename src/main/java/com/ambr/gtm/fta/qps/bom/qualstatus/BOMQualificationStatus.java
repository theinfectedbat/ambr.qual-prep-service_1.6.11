package com.ambr.gtm.fta.qps.bom.qualstatus;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;

import com.ambr.gtm.fta.qps.qualtx.engine.QualTX;
import com.ambr.gtm.fta.qts.config.QEConfigCache;
import com.ambr.platform.utils.misc.ParameterizedMessageUtility;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class BOMQualificationStatus
	implements Serializable
{
	public final long								bomKey ;
	public long										currentTime;
	public QualificationPreparationStatusDetail		prepStatusDetail;
	public Duration									requalEstimatedTimeRemaining;
	public Duration									qualEngineEvalEstimatedTimeRemaining;
	public ArrayList<TradeLaneDetail>				tradeLaneDetailList;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBOMKey
     *************************************************************************************
     */
	
	public BOMQualificationStatus(long theBOMKey)
		throws Exception
	{
		this.bomKey = theBOMKey;
		this.currentTime = System.currentTimeMillis();
		this.tradeLaneDetailList = new ArrayList<>();
	}
	
	public BOMQualificationStatus()
			throws Exception
		{
			this.bomKey = 0;
			this.currentTime = System.currentTimeMillis();
			this.tradeLaneDetailList = new ArrayList<>();
		}
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theQualTX
     *************************************************************************************
     */
	public void addQualTXDetails(QualTX theQualTX)
		throws Exception
	{
		for (TradeLaneDetail aTradeLaneDetail : this.tradeLaneDetailList) {
			if (aTradeLaneDetail.isMatch(theQualTX)) {
				aTradeLaneDetail.updateDetails(theQualTX);
			}
		}
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theTradeLaneDetail
     *************************************************************************************
     */
	public void addTradeLane(TradeLaneDetail theTradeLaneDetail)
		throws Exception
	{
		this.tradeLaneDetailList.add(theTradeLaneDetail);
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theStatusDetail
	 *************************************************************************************
	 */
	void setPrepStatusDetail(QualificationPreparationStatusDetail theStatusDetail)
		throws Exception
	{
		this.prepStatusDetail = theStatusDetail;
	}
		

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public String toString()
	{
		int							aMaxFTACodeLength;
		int							aMaxStatusLength;
		ParameterizedMessageUtility aMsgUtil;
		
		try {
			aMaxFTACodeLength = this.tradeLaneDetailList.stream().mapToInt(aRecord->(aRecord.ftaCode == null)? 0 :aRecord.ftaCode.length()).max().getAsInt();
			if (aMaxFTACodeLength < 10) aMaxFTACodeLength = 10;
			aMaxStatusLength = this.tradeLaneDetailList.stream().mapToInt(aRecord->aRecord.getStatusDescription().length()).max().getAsInt();

			aMsgUtil = new ParameterizedMessageUtility();
			
			aMsgUtil.format("BOM Qualification Status Details", false,  true);
			aMsgUtil.format("", false,  true);
			aMsgUtil.format("Current Time: {0}", false,  true, new Timestamp(this.currentTime));
			
			aMsgUtil.format("Qualification Preparation Status: Status [{0}] Start [{1}] End [{2}]", false,  true,
				this.prepStatusDetail.statusText,
				this.prepStatusDetail.startTime,
				this.prepStatusDetail.endTime
			);
			
			aMsgUtil.format("", false,  true);
			aMsgUtil.format("-------------------------------------{0}--------------------{1}-----", false,  true,
				aMsgUtil.createPaddingString("FTA", '-', aMaxFTACodeLength),
				aMsgUtil.createPaddingString("Current Status", '-', aMaxStatusLength)
			);
			aMsgUtil.format("Qualification Period              Trade Lane     Status", false,  true);
			aMsgUtil.format("-------------------------------------{0}--------------------{1}-----", false,  true,
				aMsgUtil.createPaddingString("FTA", '-', aMaxFTACodeLength),
				aMsgUtil.createPaddingString("Current Status", '-', aMaxStatusLength)
			);
			aMsgUtil.format("From            To                FTA{0} COI Current Status{1} Time", false,  true,
				aMsgUtil.createPaddingString("FTA", ' ', aMaxFTACodeLength),
				aMsgUtil.createPaddingString("Current Status", ' ', aMaxStatusLength)
			);
			aMsgUtil.format("-------------------------------------{0}--------------------{1}-----", false,  true,
				aMsgUtil.createPaddingString("FTA", '-', aMaxFTACodeLength),
				aMsgUtil.createPaddingString("Current Status", '-', aMaxStatusLength)
			);

			for (TradeLaneDetail aDetail : this.tradeLaneDetailList) {
				aMsgUtil.format("{0} {1} {2} {3} {4} {5}", false,  true, 
					aDetail.qualPeriodStartDate,
					aDetail.qualPeriodEndDate,
					aMsgUtil.createPaddedValue(aDetail.ftaCode, ' ', aMaxFTACodeLength),
					aMsgUtil.createPaddedValue(aDetail.coiSpec, ' ', 3),
					aMsgUtil.createPaddedValue(aDetail.getStatusDescription(), ' ', aMaxFTACodeLength),
					aDetail.timestamp
				);
			}
			
			return aMsgUtil.getMessage();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
