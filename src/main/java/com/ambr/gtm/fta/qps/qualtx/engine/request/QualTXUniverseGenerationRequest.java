package com.ambr.gtm.fta.qps.qualtx.engine.request;

import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.util.StringUtil;

import com.ambr.gtm.fta.qts.TrackerCodes;
import com.ambr.platform.utils.misc.ParameterizedMessageUtility;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class QualTXUniverseGenerationRequest 
{
	static Logger				logger = LogManager.getLogger(QualTXUniverseGenerationRequest.class);
	
	public boolean						returnStatusFlag;
	public boolean						refreshCachesFlag;
	public ArrayList<TradeLaneSet>		tradeLaneSetList;
	public ArrayList<Long>				bomKeyList;
	public String						currentAnalysisMethodName;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public QualTXUniverseGenerationRequest()
		throws Exception
	{
		this.returnStatusFlag = false;
		this.refreshCachesFlag = true;
		this.tradeLaneSetList = new ArrayList<>();
		this.bomKeyList = new ArrayList<>();
		this.currentAnalysisMethodName = TrackerCodes.AnalysisMethod.TOP_DOWN_ANALYSIS.name();
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public void addBOM(long theBOMKey)
		throws Exception
	{
		this.bomKeyList.add(theBOMKey);
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public TradeLaneSet addFTA(String theFTACode, boolean theRawMaterialFlag, boolean theTopDownFlag)
		throws Exception
	{
		if (theFTACode == null) {
			throw new IllegalArgumentException("FTA code must be specified");
		}
		
		theFTACode = theFTACode.toUpperCase();
		for (TradeLaneSet aTradeLaneSet : this.tradeLaneSetList) {
			if (aTradeLaneSet.ftaCode.equalsIgnoreCase(theFTACode)) {
				return aTradeLaneSet;
			}
		}
		
		TradeLaneSet aTradeLaneSet = new TradeLaneSet(theFTACode);
		aTradeLaneSet.topDownFlag = theTopDownFlag;
		aTradeLaneSet.rawMaterialFlag = theRawMaterialFlag;
		
		this.tradeLaneSetList.add(aTradeLaneSet);

		return aTradeLaneSet;
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public boolean isBOMEnabled(long theBomKey)
		throws Exception
	{
		if (this.bomKeyList.size() == 0) {
			return true;
		}
		
		return this.bomKeyList.contains(theBomKey);
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public boolean isTradeLaneProcessingRequested(String orgCode, String theFTACode, String theCOI)
		throws Exception
	{
		if (this.tradeLaneSetList != null && !this.tradeLaneSetList.isEmpty())
		{
			return this.tradeLaneSetList.stream().anyMatch(p-> p.ftaCode.equalsIgnoreCase(theFTACode) && p.coiList.stream().anyMatch(p1 -> p1.equalsIgnoreCase(theCOI)));
		} 
		return true;
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public String toString()
	{
		try {
			ParameterizedMessageUtility		aUtil = new ParameterizedMessageUtility();
			
			aUtil.format("Generation Request", false, true);
			aUtil.format("  Trade Lanes", false, true);
			for (TradeLaneSet aTradeLane : this.tradeLaneSetList) {
				
				aUtil.format("    FTA  [{0}]", false, true, aTradeLane.ftaCode);
				aUtil.format("    COIs [{0}]", false, true, StringUtil.join(aTradeLane.coiList.toArray(), ","));
			}
			
			aUtil.format("  BOM [{0}]", false, true, StringUtil.join(this.bomKeyList.toArray(), ","));
	
			return aUtil.getMessage();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
