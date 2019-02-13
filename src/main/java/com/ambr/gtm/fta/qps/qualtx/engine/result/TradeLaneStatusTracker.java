package com.ambr.gtm.fta.qps.qualtx.engine.result;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.dao.DataIntegrityViolationException;

import com.ambr.gtm.fta.qps.qualtx.engine.QualTX;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponent;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponentDataExtension;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponentPrice;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXDataExtension;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXPrice;
import com.ambr.platform.utils.log.MessageFormatter;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class TradeLaneStatusTracker 
	implements Serializable
{
	private static Logger						logger = LogManager.getLogger(TradeLaneStatusTracker.class);
	
	private static final long serialVersionUID = 1L;

	public TradeLaneStatusEnum							status;
	public long											alt_key_bom;
	public long											alt_key_qualtx;
	public String										fta_code;
	public String										ctry_of_import;
	public String										org_code;
	public ArrayList<RecordOperationStatus>				recOperationList;
	private Date										startTime;
	private Date										endTime;
	private RecordOperationStatus						qualTXSaveFailed;
	private boolean										duplicateQualTXFlag;
	private boolean										duplicateQualTXComponentFlag;									
	private boolean										duplicateQualTXComponentDEFlag;									
	private boolean										duplicateQualTXComponentPriceFlag;									
	private boolean										duplicateQualTXDEFlag;									
	private boolean										duplicateQualTXPriceFlag;									
	private transient BOMStatusManager					mgr;
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public TradeLaneStatusTracker()
		throws Exception
	{
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theQualTX
     *************************************************************************************
     */
	public TradeLaneStatusTracker(QualTX theQualTX)
		throws Exception
	{
		this.alt_key_qualtx = theQualTX.alt_key_qualtx;
		this.alt_key_bom = theQualTX.alt_key_bom;
		this.fta_code = theQualTX.fta_code;
		this.ctry_of_import = theQualTX.ctry_of_import;
		this.org_code = theQualTX.org_code;
		this.status = TradeLaneStatusEnum.INIT;
		this.recOperationList = new ArrayList<>();
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theStatus
	 *************************************************************************************
	 */
	private synchronized void addRecordOperation(RecordOperationStatus theStatus)
		throws Exception
	{
		if(this.recOperationList != null)
		this.recOperationList.add(theStatus);
		
		if (theStatus.failureException != null) {
			MessageFormatter.trace(logger, "addRecordOperation", theStatus.failureException, 
				"Record Type [{0}] ID [{1,number,#}] Operation [{2}]", 
				theStatus.recType.name(),
				theStatus.recordID,
				theStatus.operation.name()
			);
		}
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public void classificationPullSuccess()
		throws Exception
	{
    	this.addRecordOperation(
    		new RecordOperationStatus(RecordTypeEnum.MDI_QUALTX, this.alt_key_qualtx, OperationEnum.CLASSIFICATION_PULL, null)
    	);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQualTXComp
	 *************************************************************************************
	 */
	public void classificationPullSuccess(QualTXComponent theQualTXComp)
		throws Exception
	{
    	this.addRecordOperation(
    		new RecordOperationStatus(
    			RecordTypeEnum.MDI_QUALTX_COMP, 
    			theQualTXComp.alt_key_comp, 
    			OperationEnum.CLASSIFICATION_PULL, 
    			null
    		)
    	);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theException
	 *************************************************************************************
	 */
	public void classificationPullFailure(Exception theException)
		throws Exception
	{
    	this.addRecordOperation(
    		new RecordOperationStatus(
    			RecordTypeEnum.MDI_QUALTX, 
    			this.alt_key_qualtx, 
    			OperationEnum.CLASSIFICATION_PULL, 
    			theException
    		)
    	);
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theQualTXComp
     * @param	theException
     *************************************************************************************
     */
	public void classificationPullFailure(QualTXComponent theQualTXComp, Exception theException)
		throws Exception
	{
		if(theQualTXComp != null)
    	this.addRecordOperation(
    		new RecordOperationStatus(
    			RecordTypeEnum.MDI_QUALTX_COMP, 
    			theQualTXComp.alt_key_comp, 
    			OperationEnum.CLASSIFICATION_PULL, 
    			theException
    		)
    	);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQualTXComp
	 * @param	theException
	 *************************************************************************************
	 */
	public void constructComponentFailure(QualTXComponent theQualTXComp, Exception theException)
		throws Exception
	{
    	this.addRecordOperation(
    		new RecordOperationStatus(
    			RecordTypeEnum.MDI_QUALTX_COMP, 
    			theQualTXComp.alt_key_comp, 
    			OperationEnum.CONSTRUCT_RECORD, 
    			theException
    		)
    	);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQualTXComp
	 *************************************************************************************
	 */
	public void constructComponentSuccess(QualTXComponent theQualTXComp)
		throws Exception
	{
    	this.addRecordOperation(
    		new RecordOperationStatus(
    			RecordTypeEnum.MDI_QUALTX_COMP, 
    			theQualTXComp.alt_key_comp, 
    			OperationEnum.CONSTRUCT_RECORD, 
    			null
    		)
    	);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theException
	 *************************************************************************************
	 */
	public void constructQualTXFailed(Exception theException)
		throws Exception
	{
    	this.addRecordOperation(
    		new RecordOperationStatus(
    			RecordTypeEnum.MDI_QUALTX, 
    			this.alt_key_qualtx, 
    			OperationEnum.CONSTRUCT_RECORD, 
    			theException
    		)
    	);
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public void constructQualTXSucceeded()
		throws Exception
	{
    	this.addRecordOperation(
    		new RecordOperationStatus(
    			RecordTypeEnum.MDI_QUALTX, 
    			this.alt_key_qualtx, 
    			OperationEnum.CONSTRUCT_RECORD, 
    			null
    		)
    	);
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public ArrayList<RecordOperationStatus> getAllRecordOperationStatuses()
		throws Exception
	{
		return this.recOperationList;
	}

    /**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public String getCtryOfImport()
	{
		return this.ctry_of_import;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public long getDuration()
		throws Exception
	{
		if (this.startTime == null) {
			throw new IllegalStateException(MessageFormat.format("Qual TX [{0}]: processing has not started", this.alt_key_qualtx));
		}
		
		if (this.endTime == null) {
			return System.currentTimeMillis() - this.startTime.getTime();
		}
		else {
			return this.endTime.getTime() - this.startTime.getTime();
		}
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public String getDurationText()
		throws Exception
	{
		return DurationFormatUtils.formatDurationHMS(this.getDuration());
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public Date getEndTime()
		throws Exception
	{
		return this.endTime;
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	@JsonIgnore
	public int getErrorCount()	
		throws Exception
	{
		// TODO
		throw new UnsupportedOperationException();
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public String getFTACode()
	{
		return this.fta_code;
	}
	
    /**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public Exception getQualTXSaveException()
		throws Exception
	{
		return this.qualTXSaveFailed.failureException;
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public Date getStartTime()
		throws Exception
	{
		return this.startTime;
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public boolean isPersistenceRetryRequired()
		throws Exception
	{
		return
			this.duplicateQualTXFlag ||
			this.duplicateQualTXDEFlag ||
			this.duplicateQualTXPriceFlag ||
			this.duplicateQualTXComponentFlag ||
			this.duplicateQualTXComponentDEFlag ||
			this.duplicateQualTXComponentPriceFlag
		;
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public boolean isQualTXPersistedSuccessfully()
		throws Exception
	{
		return this.qualTXSaveFailed == null;
	}

    /**
	 *************************************************************************************
	 * <P>
	 * Converts the exception objects into text to compact the serialized value.  Also removes
	 * all objects with a status of "success".
	 * </P>
	 *************************************************************************************
	 */
	private void minimize()
		throws Exception
	{
		RecordOperationStatus	aStatus;
		for (int aIndex = this.recOperationList.size()-1; aIndex >= 0; aIndex--) {
			aStatus = this.recOperationList.get(aIndex);
			if (aStatus.isSuccess()) {
				this.recOperationList.remove(aIndex);
			}
			else {
				aStatus.minimize();
			}
		}
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public void persistCompleted()
		throws Exception
	{
    	this.addRecordOperation(
    		new RecordOperationStatus(
    			RecordTypeEnum.MDI_QUALTX, 
    			this.alt_key_qualtx, 
    			OperationEnum.PERSIST_RECORD, 
    			null
    		)
    	);
	}

    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theException
     *************************************************************************************
     */
	public void persistFailed(Exception theException)
		throws Exception
	{
    	this.addRecordOperation(
    		new RecordOperationStatus(
    			RecordTypeEnum.MDI_QUALTX, 
    			this.alt_key_qualtx, 
    			OperationEnum.PERSIST_RECORD, 
    			theException
    		)
    	);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theException
	 *************************************************************************************
	 */
	public void qualificationFailed(Exception theException)
		throws Exception
	{
		this.addRecordOperation(
			new RecordOperationStatus(RecordTypeEnum.MDI_QUALTX, this.alt_key_qualtx, OperationEnum.QUALIFICATION, theException)
		);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public void qualificationSucceeded()
		throws Exception
	{
		this.addRecordOperation(
			new RecordOperationStatus(RecordTypeEnum.MDI_QUALTX, this.alt_key_qualtx, OperationEnum.QUALIFICATION, null)
		);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQualTXCompDataExt
	 *************************************************************************************
	 */
	public void qualTXComponentDataExtensionSaveSuccess(QualTXComponentDataExtension theQualTXCompDataExt)
		throws Exception
	{
    	this.addRecordOperation(
			new RecordOperationStatus(
				RecordTypeEnum.MDI_QUALTX_COMP_DE, 
				theQualTXCompDataExt.seq_num, 
				OperationEnum.PERSIST_RECORD, 
				null
			)
		);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQualTXCompDataExt
	 * @param	theSaveException
	 *************************************************************************************
	 */
	public void qualTXComponentDataExtensionSaveFailed(
		QualTXComponentDataExtension 	theQualTXCompDataExt,
		Exception 						theSaveException)
		throws Exception
	{
		if (!this.duplicateQualTXComponentDEFlag) {
			this.duplicateQualTXComponentDEFlag = theSaveException instanceof DataIntegrityViolationException;
		}

		this.addRecordOperation(
			new RecordOperationStatus(
				RecordTypeEnum.MDI_QUALTX_COMP_DE, 
				theQualTXCompDataExt.seq_num, 
				OperationEnum.PERSIST_RECORD, 
				theSaveException
			)
		);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQualTXCompPrice
	 *************************************************************************************
	 */
	public void qualTXComponentPriceSaveSuccess(QualTXComponentPrice theQualTXCompPrice)
		throws Exception
	{
    	this.addRecordOperation(
			new RecordOperationStatus(
				RecordTypeEnum.MDI_QUALTX_COMP_PRICE, 
				theQualTXCompPrice.alt_key_price, 
				OperationEnum.PERSIST_RECORD, 
				null
			)
		);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQualTXCompPrice
	 * @param	theSaveException
	 *************************************************************************************
	 */
	public void qualTXComponentPriceSaveFailed(QualTXComponentPrice theQualTXCompPrice, Exception theSaveException)
		throws Exception
	{
		if (!this.duplicateQualTXComponentPriceFlag) {
			this.duplicateQualTXComponentPriceFlag = theSaveException instanceof DataIntegrityViolationException;
		}

		this.addRecordOperation(
			new RecordOperationStatus(
				RecordTypeEnum.MDI_QUALTX_COMP_PRICE, 
				theQualTXCompPrice.alt_key_price, 
				OperationEnum.PERSIST_RECORD, 
				theSaveException
			)
		);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQualTXComp
	 * @param	theSaveException
	 *************************************************************************************
	 */
	public void qualTXComponentSaveFailed(
		QualTXComponent 	theQualTXComp, 
		Exception 			theSaveException)
		throws Exception
	{
		if (!this.duplicateQualTXComponentFlag) {
			this.duplicateQualTXComponentFlag = theSaveException instanceof DataIntegrityViolationException;
		}

		this.addRecordOperation(
    		new RecordOperationStatus(
    			RecordTypeEnum.MDI_QUALTX_COMP, 
    			theQualTXComp.alt_key_comp, 
    			OperationEnum.PERSIST_RECORD, 
    			theSaveException
    		)
    	);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * @param 	theQualTXComp 
	 *************************************************************************************
	 */
	public void qualTXComponentSaveSuccess(QualTXComponent theQualTXComp)
		throws Exception
	{
    	this.addRecordOperation(
    		new RecordOperationStatus(
    			RecordTypeEnum.MDI_QUALTX_COMP, 
    			theQualTXComp.alt_key_comp, 
    			OperationEnum.PERSIST_RECORD, 
    			null
    		)
    	);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQualTXDataExt
	 * @param	theSaveException
	 *************************************************************************************
	 */
	public void qualTXDataExtensionSaveFailed(
		QualTXDataExtension theQualTXDataExt, 
		Exception 			theSaveException)
		throws Exception
	{
		if (!this.duplicateQualTXDEFlag) {
			this.duplicateQualTXDEFlag = theSaveException instanceof DataIntegrityViolationException;
		}
		
    	this.addRecordOperation(
    		new RecordOperationStatus(
    			RecordTypeEnum.MDI_QUALTX_DE, 
    			theQualTXDataExt.seq_num, 
    			OperationEnum.PERSIST_RECORD, 
    			theSaveException
    		)
    	);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * @param theQualTXDataExt 
	 *************************************************************************************
	 */
	public void qualTXDataExtensionSaveSuccess(QualTXDataExtension theQualTXDataExt)
		throws Exception
	{
    	this.addRecordOperation(
    		new RecordOperationStatus(
    			RecordTypeEnum.MDI_QUALTX_DE, 
    			theQualTXDataExt.seq_num, 
    			OperationEnum.PERSIST_RECORD, 
    			null
    		)
    	);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * @param 	theQualTXPrice 
	 * @param	theSaveException
	 *************************************************************************************
	 */
    public void qualTXPriceSaveFailed(QualTXPrice theQualTXPrice, Exception theSaveException)
    	throws Exception
    {
		if (!this.duplicateQualTXPriceFlag) {
			this.duplicateQualTXPriceFlag = theSaveException instanceof DataIntegrityViolationException;
		}
    	
    	this.addRecordOperation(
    		new RecordOperationStatus(
    			RecordTypeEnum.MDI_QUALTX_PRICE, 
    			theQualTXPrice.alt_key_price, 
    			OperationEnum.PERSIST_RECORD, 
    			theSaveException
    		)
    	);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * @param theQualTXPrice 
	 *************************************************************************************
	 */
	public void qualTXPriceSaveSuccess(QualTXPrice theQualTXPrice)
		throws Exception
	{
    	this.addRecordOperation(
    		new RecordOperationStatus(
    			RecordTypeEnum.MDI_QUALTX_PRICE, 
    			theQualTXPrice.alt_key_price, 
    			OperationEnum.PERSIST_RECORD, 
    			null
    		)
    	);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theSaveException
	 *************************************************************************************
	 */
	public void qualTXSaveFailed(Exception theSaveException)
		throws Exception
	{
		this.qualTXSaveFailed = 
    		new RecordOperationStatus(
    			RecordTypeEnum.MDI_QUALTX, 
    			this.alt_key_qualtx, 
    			OperationEnum.PERSIST_RECORD, 
    			theSaveException
    		);
		
		if (!this.duplicateQualTXFlag) {
			this.duplicateQualTXFlag = theSaveException instanceof DataIntegrityViolationException;
		}
	
    	this.addRecordOperation(this.qualTXSaveFailed);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public void qualTXSaveSuccess()
		throws Exception
	{
    	this.addRecordOperation(
    		new RecordOperationStatus(
    			RecordTypeEnum.MDI_QUALTX, 
    			this.alt_key_qualtx, 
    			OperationEnum.PERSIST_RECORD, 
    			null
    		)
    	);
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * Sets the country of import field.  This method is provided since the ctry of import
	 * is not known at the time the Qual TX is initially created
	 * </P>
	 * 
	 * @param	theCtryOfImport
	 *************************************************************************************
	 */
	public void setCtryOfImport(String theCtryOfImport)
		throws Exception
	{
		this.ctry_of_import = theCtryOfImport;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public void setEndTime()
		throws Exception
	{
		this.endTime = new Timestamp(System.currentTimeMillis());
		if (this.mgr != null) {
			this.minimize();
			this.mgr.tradeLaneCompleted(this);
		}
	}

	/**
	 *************************************************************************************
	 * <P>
	 * Sets the FTA code field.  This method is provided since the FTA code is not known at
	 * the time the Qual TX is initially created
	 * </P>
	 * 
	 * @param	theFTACode
	 *************************************************************************************
	 */
	public void setFTACode(String theFTACode)
		throws Exception
	{
		this.fta_code = theFTACode;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theMgr
	 *************************************************************************************
	 */
	void setManager(BOMStatusManager theMgr)
		throws Exception
	{
		this.mgr = theMgr;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public void setStartTime()
		throws Exception
	{
		this.startTime = new Timestamp(System.currentTimeMillis());
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theOrgCode
	 *************************************************************************************
	 */
	public void setOrgCode(String theOrgCode)
		throws Exception
	{
		this.org_code = theOrgCode;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQualTXComp
	 * @param	theException
	 *************************************************************************************
	 */
	public void sourceIVAPullFailure(QualTXComponent theQualTXComp, Exception theException)
		throws Exception
	{
    	this.addRecordOperation(
    		new RecordOperationStatus(
    			RecordTypeEnum.MDI_QUALTX_COMP, 
    			theQualTXComp.alt_key_comp, 
    			OperationEnum.SOURCE_IVA_PULL, 
    			theException
    		)
    	);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQualTXComp
	 *************************************************************************************
	 */
	public void sourceIVAPullSuccess(QualTXComponent theQualTXComp)
		throws Exception
	{
    	this.addRecordOperation(
    		new RecordOperationStatus(
    			RecordTypeEnum.MDI_QUALTX_COMP, 
    			theQualTXComp.alt_key_comp, 
    			OperationEnum.SOURCE_IVA_PULL, 
    			null
    		)
    	);
	}
}
