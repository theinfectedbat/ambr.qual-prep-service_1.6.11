package com.ambr.gtm.fta.qps.qualtx.universe;

import java.util.ArrayList;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class QualTXDetailBOMContainer 
{
	public long							bomKey;
	public ArrayList<QualTXDetail>		qualTXDetailList;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public QualTXDetailBOMContainer()
		throws Exception
	{
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public QualTXDetailBOMContainer(long theBOMKey)
		throws Exception
	{
		this.bomKey = theBOMKey;
		this.qualTXDetailList = new ArrayList<>();
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theDetail
     *************************************************************************************
     */
	public void add(QualTXDetail theDetail)
		throws Exception
	{
		this.qualTXDetailList.add(theDetail);
	}
}
