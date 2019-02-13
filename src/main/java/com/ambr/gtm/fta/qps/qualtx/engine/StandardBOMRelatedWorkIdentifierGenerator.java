package com.ambr.gtm.fta.qps.qualtx.engine;

import java.text.MessageFormat;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class StandardBOMRelatedWorkIdentifierGenerator 
{
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theBOMKey
	 *************************************************************************************
	 */
	public static String execute(long theBOMKey)
		throws Exception
	{
		return "BOM." + theBOMKey;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theBOMKey
	 * @param	theRelatedRecordKey
	 * @param	theRelatedRecordLabel
	 *************************************************************************************
	 */
	public static String execute(long theBOMKey, long theRelatedRecordKey, String theRelatedRecordLabel)
		throws Exception
	{
		if (theRelatedRecordLabel == null) {
			theRelatedRecordLabel = "RR";
		}
		
		return "BOM." + theBOMKey + "|" + theRelatedRecordLabel + "." + theRelatedRecordKey;
	}
}
