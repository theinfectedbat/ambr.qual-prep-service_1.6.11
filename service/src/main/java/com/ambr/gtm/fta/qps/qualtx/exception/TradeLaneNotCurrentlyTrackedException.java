package com.ambr.gtm.fta.qps.qualtx.exception;

import java.text.MessageFormat;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class TradeLaneNotCurrentlyTrackedException
	extends Exception
{
	private static final long serialVersionUID = 1L;

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theQualTXKey
     *************************************************************************************
     */
	public TradeLaneNotCurrentlyTrackedException(long theQualTXKey)
		throws Exception
	{
		super(MessageFormat.format("Qual TX [{0}]: does not exist in status tracking manager.", theQualTXKey));
	}
}
