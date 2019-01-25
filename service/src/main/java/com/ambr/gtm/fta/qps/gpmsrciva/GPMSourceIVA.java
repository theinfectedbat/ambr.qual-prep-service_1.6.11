package com.ambr.gtm.fta.qps.gpmsrciva;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;

import com.ambr.platform.utils.log.MessageFormatter;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GPMSourceIVA 
	implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	public long 										srcKey;
	public long 										ivaKey;
	public long											prodKey;
	public boolean										ftaEnabledFlag;
	public STPDecisionEnum								systemDecision;
	public STPDecisionEnum								finalDecision;
	public String										ctryOfImport;
	public Date											effectiveFrom;
	public Date											effectiveTo;
	public String										ftaCode;
	public String 										ivaCode;
	public String 										ctryOfOrigin;
	private transient GPMSourceIVAUniversePartition		partition;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GPMSourceIVA()
		throws Exception
	{
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	thePartition
     *************************************************************************************
     */
	public GPMSourceIVA(GPMSourceIVAUniversePartition thePartition)
		throws Exception
	{
		this.partition = thePartition;
	}
	
	public boolean isIVAEligibleForProcessing()
	{
		boolean isIVAEligible = true;

		if (!this.ftaEnabledFlag || !STPDecisionEnum.M.equals(this.systemDecision))
		{
			isIVAEligible = false;
		}

		if (isIVAForPartialYear(this.effectiveFrom, this.effectiveTo))
		{
			isIVAEligible = false;
		}

		return isIVAEligible;
	}
	
	public boolean isIVAForPartialYear(Date theEffectiveFromDate, Date theEffectiveToDate)
	{
		if (theEffectiveFromDate == null || theEffectiveToDate == null) return true;

		Calendar aIvafrom = Calendar.getInstance();
		aIvafrom.setTime(theEffectiveFromDate);

		int ivaFromDay = aIvafrom.get(Calendar.DAY_OF_MONTH);
		int ivaFromMonth = aIvafrom.get(Calendar.MONTH);

		Calendar aIvaTo = Calendar.getInstance();
		aIvaTo.setTime(theEffectiveToDate);

		int ivaToDay = aIvaTo.get(Calendar.DAY_OF_MONTH);
		int ivaToMonth = aIvaTo.get(Calendar.MONTH);
		if (ivaFromDay != 1 || ivaFromMonth != 0 || ivaToDay != 31 || ivaToMonth != 11) return true;

		return false;

	}
	
}
