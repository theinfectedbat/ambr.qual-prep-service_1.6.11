package com.ambr.gtm.fta.qps.qualtx.engine;

import java.util.ArrayList;

import com.ambr.gtm.fta.qps.qualtx.exception.ComponentMaxBatchSizeReachedException;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
class QualTXComponentBatch 
{
	private int							startIndex;
	private int							maxBatchSize;
	final QualTX						qualTX;
	public ArrayList<QualTXComponent>	compList;
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param 	theQualTX 
	 * @param	theBatchSize
	 * @param 	theBatchSize 
	 *************************************************************************************
	 */
	public QualTXComponentBatch(QualTX theQualTX, int theStartIndex, int theBatchSize)
		throws Exception
	{
		this.qualTX = theQualTX;
		this.startIndex = theStartIndex;
		this.maxBatchSize = theBatchSize;
		this.compList = new ArrayList<>();
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public int getEndIndex()
		throws Exception
	{
		return this.startIndex + this.compList.size() - 1;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public int getStartIndex()
		throws Exception
	{
		return this.startIndex;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @return		The index of the next position in the component list beyond the "extracted"
	 * 				batch.  Or -1, if there are no more components to extract.
	 *************************************************************************************
	 */
	public int initialize()
		throws Exception
	{
		int aIndex;
		int	aBatchSize;
	
		for (	
				aIndex = this.startIndex, aBatchSize = 0; 
				(aIndex < this.qualTX.compList.size()) && (aBatchSize < this.maxBatchSize); 
				aIndex++, aBatchSize++
			) 
		{
			this.compList.add(this.qualTX.compList.get(aIndex));
		}
		
		if (aIndex >= this.qualTX.compList.size()) {
			return -1;
		}
		
		return aIndex;
	}
}
