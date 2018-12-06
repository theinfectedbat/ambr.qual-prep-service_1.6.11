package com.ambr.gtm.fta.qps.qualtx.engine;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class StandardWorkIdentifierFilterLogic 
{
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theWorkIdentifier
     * @param	theFilter
     *************************************************************************************
     */
	public static boolean execute(String theWorkIdentifier, String theFilter)
		throws Exception
	{
		if (theWorkIdentifier == null) {
			return false;
		}
		
		if (theFilter == null) {
			return false;
		}
		
		return theWorkIdentifier.toUpperCase().contains(theFilter.toUpperCase());
	}
}
