package com.ambr.gtm.fta.qps.gpmclass;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GPMClassificationProductContainer 
	implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	public long								prodKey;
	public String 							ctryOfOrigin;
	public ArrayList<GPMClassification>		classificationList;
	public ArrayList<GPMCountry>			ctryList;
	
	@JsonIgnore
	public HashMap<Long, GPMClassification> ctryCmplKeyIndex;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GPMClassificationProductContainer()
		throws Exception
	{
		this.classificationList = new ArrayList<>();
		this.ctryList = new ArrayList<>();
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	void add(GPMClassification theGPMClass)
		throws Exception
	{
		this.classificationList.add(theGPMClass);
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	void add(GPMCountry theGPMCtry)	
		throws Exception
	{
		this.ctryList.add(theGPMCtry);
	}

	public GPMClassification getGPMClassificationByCtryCmplKey(long theCtryCmplKey)
	{
		return this.ctryCmplKeyIndex.get(theCtryCmplKey);
	}

	public void indexByCtryCmplKey()
	{
		this.ctryCmplKeyIndex = new HashMap<Long, GPMClassification>();
		
		if (this.classificationList == null) return;
		
		for (GPMClassification gpmClassification : this.classificationList)
		{
			this.ctryCmplKeyIndex.put(gpmClassification.cmplKey, gpmClassification);
		}
	}
}
