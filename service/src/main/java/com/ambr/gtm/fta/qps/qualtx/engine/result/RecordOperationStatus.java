package com.ambr.gtm.fta.qps.qualtx.engine.result;

import java.io.Serializable;
import java.text.MessageFormat;

import com.ambr.platform.utils.exception.ExceptionSerializer;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class RecordOperationStatus
	implements Serializable
{
	private static final long serialVersionUID = 1L;
	public Exception		failureException;
	public String			failureExceptionText;
	public long				recordID;
	public OperationEnum	operation;
	public RecordTypeEnum	recType;
	public String			id;
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public RecordOperationStatus()
		throws Exception
	{
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theRecordType
	 * @param	theRecordID
	 * @param	theOperation
	 * @param	theFailure
	 *************************************************************************************
	 */
	public RecordOperationStatus(
		RecordTypeEnum 	theRecordType, 
		long 			theRecordID, 
		OperationEnum 	theOperation, 
		Exception 		theFailure)
		throws Exception
	{
		this.recType = theRecordType;
		this.recordID = theRecordID;
		this.operation = theOperation;
		this.failureException = theFailure;
		
		this.id = this.recType.name() + "." + this.recordID + "." + this.operation.name();
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public boolean isSuccess()
		throws Exception
	{
		return (this.failureException == null) && (this.failureExceptionText == null);
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public void minimize()
		throws Exception
	{
		if (this.failureException != null) {
			this.failureExceptionText = new ExceptionSerializer(this.failureException).serialize(false);
			this.failureException = null;
		}
	}
}
