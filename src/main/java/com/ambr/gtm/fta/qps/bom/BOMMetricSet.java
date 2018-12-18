package com.ambr.gtm.fta.qps.bom;

import java.io.Serializable;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class BOMMetricSet 
	implements Serializable
{
	private static final long serialVersionUID = 1L;

	public long		bomKey;
	public int		depth;
	public int  	refCnt;
	public int  	cmpCnt;
	public Integer	processingPriority;
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public BOMMetricSet()
		throws Exception
	{
	
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBOM
     *************************************************************************************
     */
	BOMMetricSet(BOM theBOM)
		throws Exception
	{
		this.bomKey = theBOM.alt_key_bom;
		this.processingPriority = theBOM.priority;
		this.cmpCnt = theBOM.getComponentCount();
		this.depth = theBOM.getDepth();
		this.refCnt = theBOM.getReferenceCount();
	}
}
