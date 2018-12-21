package com.ambr.gtm.fta.qps.qualtx.engine;

import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.gtm.fta.qps.bom.BOMComponent;
import com.ambr.gtm.fta.qps.bom.BOMDataExtension;
import com.ambr.gtm.fta.qps.bom.BOMPrice;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetails;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsSourceIVAContainer;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVA;
import com.ambr.gtm.fta.qps.qualtx.engine.result.TradeLaneStatusTracker;
import com.ambr.gtm.fta.qps.qualtx.exception.ComponentMaxBatchSizeReachedException;
import com.ambr.gtm.fta.qps.qualtx.exception.QualTXPersistenceRetryRequiredException;
import com.ambr.gtm.fta.qps.util.QualTXUtility;
import com.ambr.gtm.fta.qts.TrackerCodes;
import com.ambr.gtm.fta.qts.config.QEConfigCache;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfiguration;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfigurationRepository;
import com.ambr.gtm.utils.legacy.rdbms.de.GroupNameSpecification;
import com.ambr.gtm.utils.legacy.sps.SimplePropertySheetManager;
import com.ambr.platform.rdbms.util.bdrp.BatchDataRecordTask;
import com.ambr.platform.rdbms.util.bdrp.DataRecordInterface;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.platform.utils.queue.AsynchronousTaskHandlerInterface;
import com.ambr.platform.utils.queue.AsynchronousTaskManager;
import com.ambr.platform.utils.queue.TaskInterface;
import com.ambr.platform.utils.queue.TaskProgressInterface;
import com.ambr.platform.utils.queue.TaskQueue;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class TradeLaneProcessorTask 
	implements TaskInterface, TaskProgressInterface
{
	private static Logger						logger = LogManager.getLogger(TradeLaneProcessorTask.class);
	
	private BOM 								bom;
	private GPMSourceIVA						srcIVA;
	private QualTX								qualTX;
	private TradeLaneProcessorQueue				tradeLaneQueue;
	private ClassificationProcessorQueue		gpmClassQueue;
	private ComponentProcessorQueue				componentQueue;
	private String								description;
	private TypedPersistenceQueue<QualTXPrice> 	qualTXPriceQueue;
	private long								newQualTXKey;
	private QEConfigCache                       qeConfigCache;
	private SimplePropertySheetManager			propertySheetManager;
	private TradeLaneStatusTracker				statusTracker;
	private PersistenceTaskHandler				taskHandler;
	private DataExtensionConfigurationRepository dataExtCfgRepos;
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	class PersistenceTaskHandler implements AsynchronousTaskHandlerInterface
	{
		@Override
		public void handleCompletion(Object theTaskReturnValue) 
			throws Exception 
		{
			BatchDataRecordTask  	aTask = (BatchDataRecordTask)theTaskReturnValue;
			DataRecordInterface		aDataRec;
			TradeLaneStatusTracker	aStatusTracker = TradeLaneProcessorTask.this.statusTracker;
			Exception				aSaveException;
			
			aTask.waitForCompletion();
			aDataRec = aTask.getDataRecord();
			aSaveException = aTask.getSaveFailureException();
			
			if (aDataRec instanceof QualTX) {
				if (aTask.getRowsUpdated() == 1 || aSaveException == null) {
					aStatusTracker.qualTXSaveSuccess();
				}
				else {
					aStatusTracker.qualTXSaveFailed(aSaveException);
				}
			}
			else if (aDataRec instanceof QualTXPrice) {
				if (aTask.getRowsUpdated() == 1 || aSaveException == null) {
					aStatusTracker.qualTXPriceSaveSuccess((QualTXPrice)aDataRec);
				}
				else {
					aStatusTracker.qualTXPriceSaveFailed((QualTXPrice)aDataRec, aSaveException);
				}
			}
			else if (aDataRec instanceof QualTXDataExtension) {
				if (aTask.getRowsUpdated() == 1  || aSaveException == null) {
					aStatusTracker.qualTXDataExtensionSaveSuccess((QualTXDataExtension)aDataRec);
				}
				else {
					aStatusTracker.qualTXDataExtensionSaveFailed((QualTXDataExtension)aDataRec, aSaveException);
				}
			}
			else if (aDataRec instanceof QualTXComponent) {
				if (aTask.getRowsUpdated() == 1  || aSaveException == null) {
					aStatusTracker.qualTXComponentSaveSuccess((QualTXComponent)aDataRec);
				}
				else {
					aStatusTracker.qualTXComponentSaveFailed((QualTXComponent)aDataRec, aSaveException);
				}
			}
			else if (aDataRec instanceof QualTXComponentPrice) {
				if (aTask.getRowsUpdated() == 1  || aSaveException == null) {
					aStatusTracker.qualTXComponentPriceSaveSuccess((QualTXComponentPrice)aDataRec);
				}
				else {
					aStatusTracker.qualTXComponentPriceSaveFailed((QualTXComponentPrice)aDataRec, aSaveException);
				}
			}
			else if (aDataRec instanceof QualTXComponentDataExtension) {
				if (aTask.getRowsUpdated() == 1  || aSaveException == null) {
					aStatusTracker.qualTXComponentDataExtensionSaveSuccess((QualTXComponentDataExtension)aDataRec);
				}
				else {
					aStatusTracker.qualTXComponentDataExtensionSaveFailed((QualTXComponentDataExtension)aDataRec, aSaveException);
				}
			}
		}
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQueue
	 * @param	theBOM
	 * @param	theSrcIVA
	 * @param	theNewQualTXKey
	 *************************************************************************************
	 */
	public TradeLaneProcessorTask(
		TradeLaneProcessorQueue theQueue, 
		BOM 					theBOM, 
		GPMSourceIVA 			theSrcIVA,
		long					theNewQualTXKey)
		throws Exception
	{
		this.bom = theBOM;
		this.srcIVA = theSrcIVA;
		this.tradeLaneQueue = theQueue;
		this.gpmClassQueue = this.tradeLaneQueue.gpmClassQueue;
		this.componentQueue = this.tradeLaneQueue.compQueue;
		this.qualTXPriceQueue = this.tradeLaneQueue.qualTXPricePersistenceQueue;
		this.dataExtCfgRepos = this.tradeLaneQueue.queueUniverse.dataExtCfgRepos;
		this.newQualTXKey = theNewQualTXKey;
		this.qeConfigCache = this.tradeLaneQueue.queueUniverse.qeConfigCache;
		this.propertySheetManager = this.tradeLaneQueue.queueUniverse.qtxBusinessLogicProcessor.propertySheetManager;
		this.description = "BOM." + this.bom.alt_key_bom + "QTX." + this.newQualTXKey;
		this.taskHandler = new PersistenceTaskHandler();
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	private void constructQualTX()
		throws Exception
	{
		int							aStartIndex;
		ComponentBatch				aBatch;
		Future<?>					aFuture;
		AsynchronousTaskManager		aTaskMgr = new AsynchronousTaskManager(this.description);
		
		this.qualTX = new QualTX(this.tradeLaneQueue.idGenerator, this.newQualTXKey);
		this.qualTX.alt_key_bom = this.bom.alt_key_bom;
		this.statusTracker = this.tradeLaneQueue.queueUniverse.qtxPrepProgressMgr
			.getStatusManager()
			.getBOMTracker(this.bom.alt_key_bom)
			.trackTradeLane(this.qualTX)
		;
		
		try {
			this.statusTracker.setStartTime();
			this.mapQualTXFields();
			
			MessageFormatter.debug(logger, "constructQualTX", "Queue [{0}] Task [{1}]: creating trade lane. QTX [{2,number,#}] FTA [{3}] COI [{4}] From [{5}] To [{6}].", 
				this.tradeLaneQueue.getName(), 
				this.description,
				this.qualTX.alt_key_qualtx,
				this.qualTX.fta_code,
				this.qualTX.ctry_of_import,
				this.qualTX.effective_from,
				this.qualTX.effective_to
			);
			
			aFuture = this.gpmClassQueue.put(this.bom, this.qualTX, this.statusTracker);
			aTaskMgr.addTask(aFuture);
			
			aStartIndex = 0;
			aBatch = new ComponentBatch(this.qualTX, this.statusTracker, aStartIndex, this.tradeLaneQueue.maxCompBatchSize, this.componentQueue.queueUniverse.dataExtCfgRepos);
			for (BOMComponent aComponent : this.bom.compList) {
				
				try {
					aBatch.addBOMComponent(aComponent);
				}
				catch (ComponentMaxBatchSizeReachedException e) {
					aFuture = this.componentQueue.put(aBatch);
					aTaskMgr.addTask(aFuture);
					aStartIndex += aBatch.getSize();
					aBatch = new ComponentBatch(this.qualTX, this.statusTracker, aStartIndex, this.tradeLaneQueue.maxCompBatchSize, this.tradeLaneQueue.queueUniverse.dataExtCfgRepos);
				}
			}
			
			if (aBatch.getSize() > 0) {
				aFuture = this.componentQueue.put(aBatch);
				aTaskMgr.addTask(aFuture);
			}
			
			for (BOMPrice aBOMPrice : this.bom.priceList) {
				QualTXPrice aQualTXPrice = this.qualTX.createPrice();
				this.mapPriceFields(aQualTXPrice, aBOMPrice);
			}
			
			for (BOMDataExtension aBOMDE : this.bom.deList) {
				if (!this.tradeLaneQueue.isDECopyEnabled(aBOMDE.group_name)) {
					continue;
				}
				
				QualTXDataExtension aQualTXDE = this.qualTX.createDataExtension(aBOMDE.group_name, this.tradeLaneQueue.dataExtRepos);
				this.mapDataExtensionFields(aBOMDE, aQualTXDE);
			}
			
			aTaskMgr.waitForCompletion();
			this.statusTracker.constructQualTXSucceeded();
		}
		catch (Exception e) {
			this.statusTracker.constructQualTXFailed(e);
		}
		finally {
		}
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theTaskQueue
	 *************************************************************************************
	 */
	@Override
	public void execute(TaskQueue<? extends TaskInterface> theTaskQueue) 
		throws Exception 
	{
		Future<PersistenceRetryTask>	aFuture;
		
		this.constructQualTX();
		
		try {
			try
			{
				this.persisteQualTX();
				if (TrackerCodes.AnalysisMethodFromConfig.TOP_DOWN.name().equalsIgnoreCase(qeConfigCache.getQEConfig(this.qualTX.org_code).getAnalysisMethod())) 
					new QualTXUtility(this.qualTX, this.tradeLaneQueue.queueUniverse.trackerClientAPI, this.statusTracker).readyForQualification(TrackerCodes.AnalysisMethod.TOP_DOWN_ANALYSIS.ordinal(), this.bom.priority);
				return;
			}
			catch (QualTXPersistenceRetryRequiredException e) {
				MessageFormatter.debug(logger, "execute", e, "Qual TX [{0}]: persistence failed, submitting to retry queue", this.qualTX.alt_key_qualtx);
			}
			
			aFuture = this.tradeLaneQueue.queueUniverse.persistenceRetryQueue.put(this.qualTX);
			aFuture.get();
			
			if (this.qualTX.getPersistFailedFlag()) {
				
			}
		}
		finally {
			this.statusTracker.setEndTime();
			this.statusTracker = null;
		}
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theWorkIdentifier
	 * @param	theFilter
	 *************************************************************************************
	 */
	@Override
	public boolean filterWorkIdentifier(String theWorkIdentifier, String theFilter) 
		throws Exception 
	{
		return StandardWorkIdentifierFilterLogic.execute(theWorkIdentifier, theFilter);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	@Override
	public String getDescription() 
	{
		return this.description;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	@Override
	public ArrayList<String> getWorkIdentifiersForTask() 
		throws Exception 
	{
		ArrayList<String>	aList;
		
		aList = new ArrayList<>();
		aList.add(StandardBOMRelatedWorkIdentifierGenerator.execute(this.bom.alt_key_bom, this.newQualTXKey, "QTX"));
		return aList;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theBOMDE
	 * @param	theQualTXDE
	 *************************************************************************************
	 */
	private void mapDataExtensionFields(BOMDataExtension theBOMDE, QualTXDataExtension theQualTXDE)
		throws Exception
	{
		GroupNameSpecification	aGroupNameSpec;
		
		aGroupNameSpec = new GroupNameSpecification(theBOMDE.group_name);
		if (aGroupNameSpec.deFlexName.equalsIgnoreCase("IMPL_BOM_PROD_FAMILY") && 
			aGroupNameSpec.deConfigName.equalsIgnoreCase("TEXTILES"))
		{
			DataExtensionConfiguration deCfg = dataExtCfgRepos.getDataExtensionConfiguration(theBOMDE.group_name);
			if (deCfg != null)
			{
				String physicalColumn = deCfg.getFlexColumnMapping().get("KNIT_TO_SHAPE");
				this.qualTX.knit_to_shape = (String) theBOMDE.getValue(physicalColumn);
			}
		}
		
		for (String aColumnName : theBOMDE.getColumnNames()) {
			theQualTXDE.setValue(aColumnName, theBOMDE.getValue(aColumnName));
		}
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theTaskQueue
	 *************************************************************************************
	 */
	private void mapPriceFields(QualTXPrice aQualTXPrice, BOMPrice aBOMPrice)
		throws Exception
	{
		aQualTXPrice.price = aBOMPrice.price;
		aQualTXPrice.price_type = aBOMPrice.price_type;
		aQualTXPrice.currency_code = aBOMPrice.currency_code;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	private void mapQualTXFields()
		throws Exception
	{
		this.qualTX.analysis_method 			= TrackerCodes.AnalysisMethod.TOP_DOWN_ANALYSIS.name();
		this.qualTX.area 						= this.bom.area;
		this.qualTX.area_uom 					= this.bom.area_uom;
		this.qualTX.assembly_type 				= this.bom.assembly_type;
		this.qualTX.bom_type 					= this.bom.bom_type;
		this.qualTX.cost 						= this.bom.getBOMPrice(BOMPrice.NET_COST);
		this.qualTX.created_by 					= this.bom.created_by;
		this.qualTX.created_date 				= new Timestamp(System.currentTimeMillis());
		this.qualTX.ctry_of_import 				= this.srcIVA.ctryOfImport;
		this.qualTX.ctry_of_manufacture 		= this.bom.ctry_of_manufacture;
		this.qualTX.ctry_of_origin 				= this.bom.ctry_of_origin;
		this.qualTX.currency_code 				= this.bom.currency_code;
		this.qualTX.direct_processing_cost 		= this.bom.direct_processing_cost;
		this.qualTX.effective_from 				= this.srcIVA.effectiveFrom;
		this.qualTX.effective_to 				= this.srcIVA.effectiveTo;
		this.qualTX.fta_code 					= this.srcIVA.ftaCode;
		this.qualTX.gross_weight 				= this.bom.gross_weight;
		this.qualTX.include_for_trace_value 	= this.bom.include_for_trace_value;
		this.qualTX.iva_code					= this.srcIVA.ivaCode;
		this.qualTX.last_modified_by 			= this.qualTX.created_by;
		this.qualTX.last_modified_date 			= this.qualTX.created_date;
		this.qualTX.manufacturer_key 			= this.bom.manufacturer_key;
		this.qualTX.org_code 					= this.bom.org_code;
		this.qualTX.prod_family 				= this.bom.prod_family;
		this.qualTX.prod_key 					= this.bom.prod_key;
		this.qualTX.prod_src_iva_key 			= this.srcIVA.ivaKey;
		this.qualTX.prod_src_key 				= this.bom.prod_src_key;
		this.qualTX.prod_sub_family 			= this.bom.prod_sub_family;
		this.qualTX.seller_key 					= this.bom.seller_key;
		this.qualTX.source_of_data 				= "BP:QUALTX_PREP_SERVICE";
		this.qualTX.supplier_key 				= this.bom.supplier_key;
		this.qualTX.src_id 						= this.bom.bom_id;
		this.qualTX.src_key 					= this.bom.alt_key_bom;
		this.qualTX.uom 						= this.bom.weight_uom;
		this.qualTX.value 						= this.bom.getBOMPrice(BOMPrice.TRANSACTION_VALUE);
//		this.qualTX.rvc_limit_safety_factor		= this.qeConfigCache.getQEConfig(this.qualTX.org_code).getAnalysisConfig().getRvcLimitSafFactor();
//		this.qualTX.rvc_threshold_safety_factor	= this.qeConfigCache.getQEConfig(this.qualTX.org_code).getAnalysisConfig().getRvcThreshHoldSaffactor();
		this.qualTX.rvc_restricted				= TrackerCodes.AssemblyType.INTERMEDIATE.name().equals(this.qualTX.assembly_type) ? "Y" : "";
		
		this.mapClaimDetails();
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	private void mapClaimDetails()
		throws Exception
	{
		GPMClaimDetailsSourceIVAContainer		aClaimDetails;
		
		aClaimDetails = this.componentQueue.queueUniverse.gpmClaimDetailsCache.getClaimDetails(this.srcIVA.ivaKey);
		if (aClaimDetails == null) {
			this.qualTX.fta_code_group = QualTXUtility.determineFTAGroupCode(this.qualTX.org_code, this.qualTX.fta_code, propertySheetManager);
			return;
		}
		
		this.qualTX.fta_code_group = aClaimDetails.getFTACodeGroup();
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	private void persisteQualTX()
		throws Exception
	{
		Future<BatchDataRecordTask> 	aFuture;
		BatchDataRecordTask 			aTask;
		AsynchronousTaskManager			aTaskMgr = new AsynchronousTaskManager(this.description);
		
		try {
			//
			// Persist the Qual TX Header
			//
			
			// This below flag will determine the successful construction of QUALTX via Top-Down approach.
			this.qualTX.td_construction_status = TrackerCodes.QualTXContructionStatus.COMPLETED.ordinal();
			
			aFuture = this.tradeLaneQueue.qualTXPersistenceQueue.put(this.qualTX);
			aTaskMgr.addTask(aFuture);
			this.waitAndValidatePersistence(aTaskMgr);
			
			if (!this.statusTracker.isQualTXPersistedSuccessfully()) {
				throw new IllegalStateException(
					MessageFormat.format("Qual TX [{0}] failed to persist.", this.qualTX.alt_key_qualtx), 
					this.statusTracker.getQualTXSaveException()
				);
			}
	
			//
			// We can now persist the Qual TX Price records
			//
		
			for (QualTXPrice aQualTXPrice : this.qualTX.priceList) {
				aFuture = this.qualTXPriceQueue.put(aQualTXPrice);
				aTaskMgr.addTask(aFuture);
			}
	
			//
			// We can now persist the Qual TX DE records
			//
		
			TypedPersistenceQueue<QualTXDataExtension>	aQTXDEQueue;
			for (QualTXDataExtension aQualTXDataExt : this.qualTX.deList) {
	
				aQTXDEQueue = this.tradeLaneQueue.qualTXDataExtQueue.getQueue(aQualTXDataExt.group_name, aQualTXDataExt, true);
				aFuture = aQTXDEQueue.put(aQualTXDataExt);
				aTaskMgr.addTask(aFuture);
			}
			
			//
			// We can now persist the Qual TX Components
			//
			int aCompIndex = 0;
			for (QualTXComponent aQualTXComponent : this.qualTX.compList) {
				try {
					aCompIndex++;
					if (aQualTXComponent == null) {
						MessageFormatter.info(logger, "persisteQualTX", "Qualtx [{0,number,#}] components [{1}].  Null component at index [{2}]", 
							this.qualTX.alt_key_qualtx, 
							this.qualTX.compList.size(),
							aCompIndex
						);
						continue;
					}
					
					aFuture = this.tradeLaneQueue.qualTXComponentPersistenceQueue.put(aQualTXComponent);
					aTaskMgr.addTask(aFuture);
				}
				catch (Exception e) {
					MessageFormatter.error(logger, "persisteQualTX", e, "Qualtx [{0,number,#}] components [{1}]", this.qualTX.alt_key_qualtx, this.qualTX.compList.size());
				}
			}

			// Wait for all "level 2" tables to be persisted.  Then we can start on "level 3" tables
			this.waitAndValidatePersistence(aTaskMgr);
	
			TypedPersistenceQueue<QualTXComponentDataExtension>	aQTXCompQueue;
			for (QualTXComponent aQualTXComponent : this.qualTX.compList) {
				for (QualTXComponentDataExtension aQualTXCompDE : aQualTXComponent.deList) {
					aQTXCompQueue = this.tradeLaneQueue.queueUniverse.qualTXComponentdataExtQueue.getQueue(aQualTXCompDE.group_name, aQualTXCompDE, true);
					aFuture = aQTXCompQueue.put(aQualTXCompDE);
					aTaskMgr.addTask(aFuture);
				}
				for (QualTXComponentPrice aQualTXCompPrice : aQualTXComponent.priceList) {
					aFuture = this.tradeLaneQueue.queueUniverse.qualTXComponentPriceQueue.put(aQualTXCompPrice);
					aTaskMgr.addTask(aFuture);
				}
			}
			
			this.waitAndValidatePersistence(aTaskMgr);
			this.statusTracker.persistCompleted();
		}
		catch (QualTXPersistenceRetryRequiredException pe) {
			throw pe;
		}
		catch (Exception e) {
			this.statusTracker.persistFailed(e);
		}
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theTaskMgr
	 *************************************************************************************
	 */
	private void waitAndValidatePersistence(AsynchronousTaskManager theTaskMgr)
		throws Exception
	{
		theTaskMgr.waitForCompletion(this.taskHandler);
		
		if (this.statusTracker.isPersistenceRetryRequired()) {
			throw new QualTXPersistenceRetryRequiredException(this.qualTX.alt_key_qualtx);
		}
	}
}
