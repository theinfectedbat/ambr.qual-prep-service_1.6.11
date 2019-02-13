package com.ambr.gtm.fta.qps.bom.qualstatus;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;

import com.ambr.gtm.fta.qps.qualtx.engine.QualTX;
import com.ambr.platform.utils.misc.ParameterizedMessageUtility;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class BOMQualificationStatusUtility
	implements Serializable
{
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theStatusObj
     * @param	theQualTX
     *************************************************************************************
     */
	public static void addQualTXDetails(BOMQualificationStatus theStatusObj, QualTX theQualTX)
		throws Exception
	{
		for (TradeLaneDetail aTradeLaneDetail : theStatusObj.tradeLaneDetailList) {
			if (TradeLaneDetailUtility.isMatch(aTradeLaneDetail, theQualTX)) {
				TradeLaneDetailUtility.updateDetails(aTradeLaneDetail, theQualTX);
			}
		}
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theStatusObj
	 *************************************************************************************
	 */
	public String toString(BOMQualificationStatus theStatusObj)
	{
		int							aMaxFTACodeLength;
		int							aMaxStatusLength;
		ParameterizedMessageUtility aMsgUtil;
		
		try {
			aMaxFTACodeLength = theStatusObj.tradeLaneDetailList.stream().mapToInt(aRecord->(aRecord.ftaCode == null)? 0 :aRecord.ftaCode.length()).max().getAsInt();
			if (aMaxFTACodeLength < 10) aMaxFTACodeLength = 10;
			aMaxStatusLength = theStatusObj.tradeLaneDetailList.stream().mapToInt(aRecord->aRecord.getStatusDescription().length()).max().getAsInt();

			aMsgUtil = new ParameterizedMessageUtility();
			
			aMsgUtil.format("BOM Qualification Status Details", false,  true);
			aMsgUtil.format("", false,  true);
			aMsgUtil.format("Current Time: {0}", false,  true, new Timestamp(theStatusObj.currentTime));
			
			aMsgUtil.format("Qualification Preparation Status: Status [{0}] Start [{1}] End [{2}]", false,  true,
				theStatusObj.prepStatusDetail.statusText,
				theStatusObj.prepStatusDetail.startTime,
				theStatusObj.prepStatusDetail.endTime
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

			for (TradeLaneDetail aDetail : theStatusObj.tradeLaneDetailList) {
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
