package com.ambr.gtm.fta.qps.bom.qualstatus;

import com.ambr.gtm.fta.qps.gpmsrciva.STPDecisionEnum;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTX;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class TradeLaneDetailUtility 
{
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theDetail
     * @param	theQualTX
     *************************************************************************************
     */
	public static boolean isMatch(TradeLaneDetail theDetail, QualTX theQualTX)
		throws Exception
	{
		if (!theDetail.ftaCode.equalsIgnoreCase(theQualTX.fta_code)) {
			return false;
		}
		
		if (!theDetail.coiSpec.equalsIgnoreCase(theQualTX.ctry_of_import)) {
			return false;
		}

		if (!theDetail.qualPeriodStartDate.equals(theQualTX.effective_from)) {
			return false;
		}

		if (!theDetail.qualPeriodEndDate.equals(theQualTX.effective_to)) {
			return false;
		}

		return true;
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theQualTX
     *************************************************************************************
     */
	public static void updateDetails(TradeLaneDetail theDetail, QualTX theQualTX)
		throws Exception
	{
		theDetail.timestamp = theQualTX.last_modified_date;
		if (theDetail.systemDecision == STPDecisionEnum.I.name()) {
			
		}
		if (theQualTX.qualified_flg == null) {
			theDetail.qualEvalStatus = QualificationEvaluationStatusEnum.UNKNOWN;
		}
		
		for (QualificationEvaluationStatusEnum qualStatusEnum : QualificationEvaluationStatusEnum.values()  )
		{
			if(qualStatusEnum.toString().equalsIgnoreCase(theQualTX.qualified_flg))
			{
				theDetail.qualEvalStatus = qualStatusEnum;
			}
		}
	}
	
}
