package com.ambr.gtm.fta.qps.bom.qualstatus;

import java.util.Date;

import com.ambr.gtm.fta.qps.gpmsrciva.STPDecisionEnum;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTX;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class TradeLaneDetail 
{
	public Date										qualPeriodStartDate;
	public Date										qualPeriodEndDate;
	public String									ftaCode;
	public String									coiSpec;
	public QualificationEvaluationStatusEnum		qualEvalStatus;
	public QualificationEvaluationStatusEnum		prevQualEvalStatus;
	public Date										timestamp;
	public STPDecisionEnum							systemDecision;
	public STPDecisionEnum							finalDecision;
	public String									orgCode;
	public String									ivaCode;
	
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public String getStatusDescription()
	{
		String aStatus = "Unknwon (initial qualification pending)";
		
		if (this.qualEvalStatus == null) {
			return aStatus;
		}
		
		switch (this.qualEvalStatus)
		{
			case UNKNOWN: return aStatus;
			case EVALUATION_DISABLED : return "Not Configured For Evaluation";
			default: return this.qualEvalStatus.name();
		}
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theQualTX
     *************************************************************************************
     */
	public boolean isMatch(QualTX theQualTX)
		throws Exception
	{
		if (!this.ftaCode.equalsIgnoreCase(theQualTX.fta_code)) {
			return false;
		}
		
		if (!this.coiSpec.equalsIgnoreCase(theQualTX.ctry_of_import)) {
			return false;
		}

		if (!this.qualPeriodStartDate.equals(theQualTX.effective_from)) {
			return false;
		}

		if (!this.qualPeriodEndDate.equals(theQualTX.effective_to)) {
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
	public void updateDetails(QualTX theQualTX)
		throws Exception
	{
		this.timestamp = theQualTX.last_modified_date;
		if (this.systemDecision == STPDecisionEnum.I) {
			
		}
		
		if (theQualTX.qualified_flg == null) {
			this.qualEvalStatus = QualificationEvaluationStatusEnum.UNKNOWN;
		}
		
		for (QualificationEvaluationStatusEnum qualStatusEnum : QualificationEvaluationStatusEnum.values()  )
		{
			if(qualStatusEnum.toString().equalsIgnoreCase(theQualTX.qualified_flg))
			{
				this.qualEvalStatus = qualStatusEnum;
			}
		}
	}
	
}
