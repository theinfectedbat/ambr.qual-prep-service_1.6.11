package com.ambr.gtm.fta.qps.qualtx.engine.request;

import java.util.ArrayList;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class TradeLaneSet 
{
	public String					ftaCode;
	public ArrayList<String>		coiList;
	
	public boolean					topDownFlag;
	public boolean					rawMaterialFlag;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public TradeLaneSet()
		throws Exception
	{
	
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theFTACode
     *************************************************************************************
     */
	public TradeLaneSet(String theFTACode)
		throws Exception
	{
		this.ftaCode = theFTACode;
		this.coiList = new ArrayList<>();
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theCOI
     *************************************************************************************
     */
	public TradeLaneSet addCOI(String theCOI)
		throws Exception
	{
		if (theCOI == null) {
			return this;
		}
		
		theCOI = theCOI.toLowerCase();
		
		if (!this.coiList.contains(theCOI)) {
			this.coiList.add(theCOI);
		}
		
		return this;
	}
}
