package com.ambr.gtm.fta.qps.qualtx.engine;

import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.gtm.fta.qps.bom.BOMUniverse;
import com.ambr.gtm.fta.qps.qualtx.engine.result.BOMStatusTracker;
import com.ambr.gtm.fta.qps.qualtx.engine.result.TradeLaneStatusTracker;
import com.ambr.gtm.fta.qps.qualtx.universe.QualTXDetail;
import com.ambr.gtm.fta.qps.util.QualTXComponentExpansionUtility;
import com.ambr.gtm.fta.qps.util.QualTXUtility;
import com.ambr.gtm.fta.qts.TrackerCodes;
import com.ambr.gtm.fta.qts.config.QEConfigCache;
import com.ambr.gtm.fta.qts.util.Env;
import com.ambr.gtm.fta.qts.util.TradeLane;
import com.ambr.gtm.fta.qts.util.TradeLaneContainer;
import com.ambr.gtm.fta.qts.util.TradeLaneData;
import com.ambr.gtm.fta.trade.client.TradeQualtxClient;
import com.ambr.gtm.fta.trade.model.BOMQualAuditEntity;
import com.ambr.platform.rdbms.orm.EntityManager;
import com.ambr.platform.rdbms.orm.exception.EntityDoesNotExistException;
import com.ambr.platform.utils.log.MessageFormatter;
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
public class QualTXComponentExpansionTask
	implements TaskInterface, TaskProgressInterface
{
	static Logger	logger = LogManager.getLogger(QualTXComponentExpansionTask.class);

	private QualTXComponentExpansionProcessorQueue			queue;
	private QualTXDetail									qualTXDetail;
	private String											description;
	PreparationEngineQueueUniverse							queueUniverse;
	BOM														bom;
	private TradeLaneStatusTracker							qualTXTracker;
	private BOMStatusTracker								bomTracker;
	private QEConfigCache qeConfigCache;
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQueue
	 * @param	theBOM
	 *************************************************************************************
	 */
	public QualTXComponentExpansionTask(QualTXComponentExpansionProcessorQueue theQueue, BOM theBOM, QualTXDetail theQualTXDetail, BOMStatusTracker theBOMTracker)
		throws Exception
	{
		this.bom = theBOM;
		this.qualTXDetail = theQualTXDetail;
		this.queue = theQueue;
		this.queueUniverse = theQueue.queueUniverse;
		this.description = "QualTXDetail." + this.qualTXDetail.alt_key_qualtx;
		this.bomTracker = theBOMTracker;
		this.qeConfigCache = this.queueUniverse.qeConfigCache;
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
		AsynchronousTaskManager				aTaskMgr = new AsynchronousTaskManager(this.description);
		String 								analysisMethod;
		boolean								isFallBackRM = false;
		boolean								isFallBackIntermediate = false;
		BOMUniverse							aBOMUniverse = this.queueUniverse.getBOMUniverse();
		
		try {
			//Check if TD construction is completed.
			if(qualTXDetail.td_construction_status == null || TrackerCodes.QualTXContructionStatus.COMPLETED.ordinal() != qualTXDetail.td_construction_status)
			{
				MessageFormatter.debug(logger, "execute", "Queue [{0}] Task [{1}]: QualTX Top-Down Contruction is not complete.", 
						this.queue.getName(), 
						this.description
					);
				return;
			}
			
			analysisMethod = qeConfigCache.getQEConfig(qualTXDetail.org_code).getAnalysisMethod();
			isFallBackRM = false;
			
			TradeLaneContainer aTradeLaneContainer = qeConfigCache.getQEConfig(qualTXDetail.org_code).getTradeLaneContainer();
			if(aTradeLaneContainer != null)
			{
				TradeLane aTradeLane = new TradeLane(qualTXDetail.fta_code, qualTXDetail.ctry_of_import);
				TradeLaneData aTradeLaneData = aTradeLaneContainer.getTradeLaneData(aTradeLane);
				if(aTradeLaneData != null)
				{
					isFallBackRM = aTradeLaneData.isRawMaterialAnalysisMethod();
					isFallBackIntermediate = aTradeLaneData.isIntermediateAnalysisMethod();
				}
			}

			
			
			//Check if RM construction is completed considering the fall-back/default analysis method. If yes, continue with the next one.
			if((TrackerCodes.AnalysisMethod.RAW_MATERIAL_ANALYSIS.ordinal() == TrackerCodes.AnalysisMethodFromConfig.valueOf(analysisMethod).ordinal()
					|| (TrackerCodes.AnalysisMethod.TOP_DOWN_ANALYSIS.ordinal() == TrackerCodes.AnalysisMethodFromConfig.valueOf(analysisMethod).ordinal() && isFallBackRM))
					&& qualTXDetail.rm_construction_status != null && TrackerCodes.QualTXContructionStatus.COMPLETED.ordinal() == qualTXDetail.rm_construction_status)
			{
				MessageFormatter.debug(logger, "execute", "Queue [{0}] Task [{1}]: QualTX Raw-Material Contruction is already complete.", 
						this.queue.getName(), 
						this.description
					);
				return;
			}
			
			//Check if Intermediate construction is completed considering the fall-back/default analysis method. If yes, continue with the next one.
			if((TrackerCodes.AnalysisMethod.INTERMEDIATE_ANALYSIS.ordinal() == TrackerCodes.AnalysisMethodFromConfig.valueOf(analysisMethod).ordinal()
					|| (TrackerCodes.AnalysisMethod.TOP_DOWN_ANALYSIS.ordinal() == TrackerCodes.AnalysisMethodFromConfig.valueOf(analysisMethod).ordinal() && isFallBackIntermediate))
					&& qualTXDetail.in_construction_status != null && TrackerCodes.QualTXContructionStatus.COMPLETED.ordinal() == qualTXDetail.in_construction_status)
			{
				MessageFormatter.debug(logger, "execute", "Queue [{0}] Task [{1}]: QualTX Intermediate Contruction is already complete.", 
						this.queue.getName(), 
						this.description
					);
				return;
			}
			
			QualTX						aQualTX;
			EntityManager<QualTX>		aQualTXMgr;
			TradeQualtxClient tradeQualtxClient = Env.getSingleton().getTradeQualtxClient();
			aQualTXMgr = new EntityManager<>(
					QualTX.class,
					this.queueUniverse.txMgr, 
					this.queueUniverse.schemaDesc, 
					new JdbcTemplate(this.queueUniverse.dataSrc)
				);
				
				aQualTXMgr.getLoader().setTableFilter(new String[]{"mdi_qualtx_comp"});
				
				try {
					aQualTX = aQualTXMgr.loadExistingEntity(new QualTX(qualTXDetail.alt_key_qualtx));
					aQualTX.idGenerator = this.queueUniverse.idGenerator;
					this.qualTXTracker = this.bomTracker.trackTradeLane(aQualTX);
					this.qualTXTracker.setStartTime();
				}
				catch (EntityDoesNotExistException e) {
					MessageFormatter.error(logger, "execute", e, "QualTX [{0,number,#}]: not found.", qualTXDetail.alt_key_qualtx);
					return;
				}
				
				QualTXUtility aQualTXUtility = new QualTXUtility(aQualTX, this.queueUniverse.trackerClientAPI, this.qualTXTracker);
				QualTXComponentExpansionUtility aQualTXComponentExpansionUtility = new QualTXComponentExpansionUtility(aBOMUniverse, aQualTX, this.queueUniverse.dataExtCfgRepos, this.queueUniverse.gpmClaimDetailsCache, this.queueUniverse.ivaCache, this.queueUniverse.gpmClassCache,this.queueUniverse.qtxBusinessLogicProcessor, false, this.qualTXTracker);

				
			if(
					//If RM is default analysis method.
					((qualTXDetail.td_construction_status != null && TrackerCodes.QualTXContructionStatus.COMPLETED.ordinal() == qualTXDetail.td_construction_status)  && TrackerCodes.AnalysisMethod.RAW_MATERIAL_ANALYSIS.ordinal() == TrackerCodes.AnalysisMethodFromConfig.valueOf(analysisMethod).ordinal())
	
					//If TD is default method and it is NOT_QUALIFIED, and fall back is RM
					|| (TrackerCodes.AnalysisMethod.TOP_DOWN_ANALYSIS.ordinal() == TrackerCodes.AnalysisMethodFromConfig.valueOf(analysisMethod).ordinal() && "NOT_QUALIFIED".equalsIgnoreCase(qualTXDetail.qualified_flg) && isFallBackRM)
			  )	
			{
				
					MessageFormatter.debug(logger, "execute", "Processing QualTX [{0,number,#}]: component expansion for Raw Material approach.", qualTXDetail.alt_key_qualtx);
				
					//If there are no MAKE components and the default config is not RM the expansion need not be executed.
					if(!aQualTX.hasMakeComponents() && TrackerCodes.AnalysisMethod.RAW_MATERIAL_ANALYSIS.ordinal() != TrackerCodes.AnalysisMethodFromConfig.valueOf(analysisMethod).ordinal())
					{
						MessageFormatter.debug(logger, "execute", "QualTX [{0,number,#}]: does not have make components.", qualTXDetail.alt_key_qualtx);
						return;
					}
					
					aQualTXComponentExpansionUtility.determineRawMaterialComponentsList();
					aQualTX.rm_construction_status = TrackerCodes.QualTXContructionStatus.COMPLETED.ordinal();
					
					try {
						//Perform audit when NOT Initial Load.
						if(aQualTX.raw_material_decision != null)
						{
							aQualTXMgr.getTracker().stopTracking();
							BOMQualAuditEntity audit = QualTXUtility.buildAudit(aQualTX.alt_key_qualtx, aQualTX.org_code, aQualTX.last_modified_by, aQualTXMgr,"EXPANSION");
							tradeQualtxClient.doRecordLevelAudit(audit);
							aQualTXMgr.getTracker().startTracking();
						}
						aQualTXMgr.save();
					}
					catch (Exception e)
					{
						MessageFormatter.error(logger, "execute", e, "Exception while persisting Raw-Material Analysis result on  QualTX [{0,number,#}]", qualTXDetail.alt_key_qualtx);
						return;
					}
					aQualTXUtility.readyForQualification(TrackerCodes.AnalysisMethod.RAW_MATERIAL_ANALYSIS.ordinal(), this.bom.priority);
					MessageFormatter.debug(logger, "execute", "Done expanding components for QualTX [{0,number,#}], number of components identified for raw-material : [{1}]", qualTXDetail.alt_key_qualtx, aQualTX.getRawMaterialComponentList().size());
			}
			
			if(
					//If Intermediate is default analysis method.
					((qualTXDetail.td_construction_status != null && TrackerCodes.QualTXContructionStatus.COMPLETED.ordinal() == qualTXDetail.td_construction_status)  && TrackerCodes.AnalysisMethod.INTERMEDIATE_ANALYSIS.ordinal() == TrackerCodes.AnalysisMethodFromConfig.valueOf(analysisMethod).ordinal())
	
					//If TD is default method and it is NOT_QUALIFIED, and fall back is Intermediate
					|| (TrackerCodes.AnalysisMethod.TOP_DOWN_ANALYSIS.ordinal() == TrackerCodes.AnalysisMethodFromConfig.valueOf(analysisMethod).ordinal() && "NOT_QUALIFIED".equalsIgnoreCase(qualTXDetail.qualified_flg) && isFallBackIntermediate)
			  )	
			{
				
				MessageFormatter.debug(logger, "execute", "Processing QualTX [{0,number,#}]: component expansion for Intermediate approach.", qualTXDetail.alt_key_qualtx);
				
				//If there are no MAKE components and the default config is not Intermediate the expansion need not be executed.
				if(!aQualTX.hasMakeComponents() && TrackerCodes.AnalysisMethod.INTERMEDIATE_ANALYSIS.ordinal() != TrackerCodes.AnalysisMethodFromConfig.valueOf(analysisMethod).ordinal())
				{
					MessageFormatter.debug(logger, "execute", "QualTX [{0,number,#}]: does not have make components.", qualTXDetail.alt_key_qualtx);
					return;
				}
				
				aQualTXComponentExpansionUtility.determineIntermmediateComponentsList();
				aQualTX.in_construction_status = TrackerCodes.QualTXContructionStatus.COMPLETED.ordinal();
				
				try {
					//Perform audit when NOT Initial Load.
					if(aQualTX.intermediate_decision != null)
					{
						BOMQualAuditEntity audit = QualTXUtility.buildAudit(aQualTX.alt_key_qualtx, aQualTX.org_code, aQualTX.last_modified_by, aQualTXMgr,"EXPANSION");
						tradeQualtxClient.doRecordLevelAudit(audit);
					}
					aQualTXMgr.save();
				}
				catch (Exception e)
				{
					MessageFormatter.error(logger, "execute", e, "Exception while persisting Intermediate Analysis results on QualTX [{0,number,#}]", qualTXDetail.alt_key_qualtx);
					return;
				}
				aQualTXUtility.readyForQualification(TrackerCodes.AnalysisMethod.INTERMEDIATE_ANALYSIS.ordinal(), this.bom.priority);
				MessageFormatter.debug(logger, "execute", "Done expanding components for QualTX [{0,number,#}], number of components identified for intermediate : [{1}]", qualTXDetail.alt_key_qualtx, aQualTX.getIntermediateComponentList().size());
			}
		}
		finally {
			if(this.qualTXTracker != null)
				this.qualTXTracker.setEndTime();
		}
		
		aTaskMgr.waitForCompletion();
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
	 * 
	 *************************************************************************************
	 */
	@Override
	public ArrayList<String> getWorkIdentifiersForTask() 
		throws Exception 
	{
		ArrayList<String>	aList;
		
		aList = new ArrayList<>();
		aList.add(StandardBOMRelatedWorkIdentifierGenerator.execute(this.qualTXDetail.alt_key_qualtx));
		return aList;
	}
}
