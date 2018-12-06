package com.ambr.gtm.fta.qps.gpmsrciva;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GPMSourceIVAProductSourceContainer
	implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	public long							prodSrcKey;
	public ArrayList<GPMSourceIVA>		ivaList;
	
	@JsonIgnore
	private HashMap<Long, GPMSourceIVA> ivaKeyIndex;
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GPMSourceIVAProductSourceContainer()
		throws Exception
	{
		this.ivaList = new ArrayList<>();
	}
	
	public void indexByIVAKey()
	{
		this.ivaKeyIndex = new HashMap<Long, GPMSourceIVA>();
		
		for (GPMSourceIVA gpmSourceIVA : this.ivaList)
		{
			this.ivaKeyIndex.put(gpmSourceIVA.ivaKey, gpmSourceIVA);
		}
	}
	
	public GPMSourceIVA getGPMSourceIVA(long theIVAKey)
	{
		return this.ivaKeyIndex.get(theIVAKey);
	}
	
	
	/**
	 * @param theFTACode
	 * @param theIVACode
	 * @param theCOI
	 * @param theEffectiveFrom
	 * @param theEffectiveTo
	 * @return
	 * @throws Exception
	 */
	public GPMSourceIVA getIVA(String theFTACode, String theIVACode, String theCOI, Date theEffectiveFrom, Date theEffectiveTo)
			throws Exception
	{
		
		if(theFTACode == null || theIVACode == null || theCOI == null || theEffectiveFrom == null || theEffectiveTo == null)
			return null;
		
		for(GPMSourceIVA aSrcIVA : this.ivaList)
		{
			if(aSrcIVA.isIVAEligibleForProcessing())
				continue;
			
			if(!theFTACode.equals(aSrcIVA.ftaCode) 
					|| !theCOI.equals(aSrcIVA.ctryOfImport)
					|| !theIVACode.equals(aSrcIVA.ivaCode))
				if(!theFTACode.equals(aSrcIVA.ftaCode) 
						|| !theCOI.equals(aSrcIVA.ctryOfImport))
						continue;
			
			if(theEffectiveFrom.compareTo(aSrcIVA.effectiveFrom) != 0
					&& theEffectiveTo.compareTo(aSrcIVA.effectiveTo)!=0)
				continue;
			
			return aSrcIVA;
		}
		
		return null;
	}
	
	/**
	 * @param theFTACode
	 * @param theCOI
	 * @param theEffectiveFrom
	 * @param theEffectiveTo
	 * @return
	 * @throws Exception
	 */
	public GPMSourceIVA getIVA(String theFTACode, String theCOI, Date theEffectiveFrom, Date theEffectiveTo)
			throws Exception
	{
		
		if(theFTACode == null || theCOI == null || theEffectiveFrom == null || theEffectiveTo == null)
			return null;
		
		for(GPMSourceIVA aSrcIVA : this.ivaList)
		{
			if(!theFTACode.equals(aSrcIVA.ftaCode) 
					|| !theCOI.equals(aSrcIVA.ctryOfImport)
					)
				continue;
			
			if(theEffectiveFrom.compareTo(aSrcIVA.effectiveFrom) < 0
					&& theEffectiveTo.compareTo(aSrcIVA.effectiveTo) > 0)
				continue;
			
			if(STPDecisionEnum.valueOf("M") != aSrcIVA.systemDecision)
				continue;
			
			return aSrcIVA;
		}
		
		return null;
	}
}
