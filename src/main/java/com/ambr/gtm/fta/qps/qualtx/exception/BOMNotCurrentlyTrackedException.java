package com.ambr.gtm.fta.qps.qualtx.exception;

import java.text.MessageFormat;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class BOMNotCurrentlyTrackedException
	extends Exception
{
	private static final long serialVersionUID = 1L;

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBOMKey
     *************************************************************************************
     */
	public BOMNotCurrentlyTrackedException(long theBOMKey)
		throws Exception
	{
		super(MessageFormat.format("BOM [{0}]: does not exist in status tracking manager.", theBOMKey));
	}
}
