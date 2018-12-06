package com.ambr.gtm.fta.qps.qualtx.exception;

import java.text.MessageFormat;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class QualTXPersistenceRetryRequiredException
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
	public QualTXPersistenceRetryRequiredException(long theQualTXKey)
		throws Exception
	{
		super(MessageFormat.format("Qual TX [{0}]: failed to persist due to duplicate key assigned.", theQualTXKey));
	}
}
