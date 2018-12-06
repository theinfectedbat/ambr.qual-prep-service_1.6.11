package com.ambr.gtm.fta.qps.qualtx.engine;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.Future;

import org.apache.commons.lang.time.DateUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVA;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAContainerCache;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAProductSourceContainer;
import com.ambr.gtm.fta.qps.qualtx.engine.result.BOMStatusEnum;
import com.ambr.gtm.fta.qps.qualtx.engine.result.BOMStatusTracker;
import com.ambr.gtm.fta.qps.qualtx.universe.QualTXDetail;
import com.ambr.gtm.fta.qps.qualtx.universe.QualTXDetailBOMContainer;
import com.ambr.gtm.fta.qps.util.BillOfMaterialEnum;
import com.ambr.gtm.fta.qts.config.QEConfigCache;
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
public class BOMProcessorTask
	implements TaskInterface, TaskProgressInterface
{
	static Logger	logger = LogManager.getLogger(BOMProcessorTask.class);

	private BOMProcessorQueue					queue;
	private BOM									bom;
	private GPMSourceIVAProductSourceContainer	srcIVAContainer;
	private TradeLaneProcessorQueue				tradeLaneQueue;
	private String								description;
	private GPMSourceIVAContainerCache			ivaCache;
	private BOMStatusTracker					bomTracker;
	private QualTXBusinessLogicProcessor		businessLogicProcessor;
	private QEConfigCache						qeConfigCache;
	 
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQueue
	 * @param	theBOM
	 *************************************************************************************
	 */
	public BOMProcessorTask(BOMProcessorQueue theQueue, BOM theBOM)
		throws Exception
	{
		this.bom = theBOM;
		this.queue = theQueue;
		this.ivaCache = this.queue.ivaCache;
		this.tradeLaneQueue = this.queue.tradeLaneQueue;
		this.businessLogicProcessor = this.tradeLaneQueue.queueUniverse.qtxBusinessLogicProcessor;
		this.qeConfigCache = this.businessLogicProcessor.qeConfigCache;
		this.description = "BOM." + this.bom.alt_key_bom;
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
		BOMStatusTracker			aTracker;
		AsynchronousTaskManager		aTaskMgr = new AsynchronousTaskManager(this.description);
	
		try {
			this.bomTracker = this.queue.queueUniverse.qtxPrepProgressMgr.getStatusManager().bomStarted(this.bom);
			this.bomTracker.setStartTime();
			if (!this.queue.request.isBOMEnabled(this.bom.alt_key_bom)) {
				MessageFormatter.debug(logger, "execute", "Queue [{0}] Task [{1}]: BOM is not enabled for processing.", 
					this.queue.getName(), 
					this.description
				);
				this.bomTracker.status = BOMStatusEnum.EXCLUDED_FROM_PROCESSING;
				this.bomTracker.setEndTime();
				return;
			}
			
			if(!"Y".equalsIgnoreCase(this.bom.is_active))
			{
				MessageFormatter.debug(logger, "execute", "Queue [{0}] Task [{1}]: BOM is not active.", 
						this.queue.getName(), 
						this.description
					);
					this.bomTracker.status = BOMStatusEnum.EXCLUDED_FROM_PROCESSING;
					this.bomTracker.setEndTime();
					return;
			}

			if (BillOfMaterialEnum.PROD_FAMILY.TEXTILES.name().equals(this.bom.prod_family))
			{
				if (!this.businessLogicProcessor.isKnitToShapeChecked(this.bom) && !this.businessLogicProcessor.isThereAnyEssentialComponent(this.bom))
				{
					MessageFormatter.debug(logger, "execute", "Queue [{0}] Task [{1}]: There is no Essential Component present and Knit To Shape Criteria is not checked.",
							this.queue.getName(), 
							this.description
					);
					this.bomTracker.status = BOMStatusEnum.EXCLUDED_FROM_PROCESSING;
					this.bomTracker.setEndTime();
					return;
				}
			}
			
			this.srcIVAContainer = this.ivaCache.getSourceIVABySource(this.bom.prod_src_key);
	
			MessageFormatter.debug(logger, "execute", "Queue [{0}] Task [{1}]: processing [{2}] trade lanes, [{3}|{4}]] components.", 
				this.queue.getName(), 
				this.description,
				this.srcIVAContainer != null ? this.srcIVAContainer.ivaList.size() : 0,
				this.bom.compList.size(),
				this.bom.componentCount
			);
	
			this.bomTracker.status = BOMStatusEnum.IN_PROGRESS;
			if( null != this.srcIVAContainer)
			for (GPMSourceIVA aSrcIVA : this.srcIVAContainer.ivaList) {
				Future<TradeLaneProcessorTask>	aFuture;
				
				if (!aSrcIVA.isIVAEligibleForProcessing())
				{
					MessageFormatter.debug(logger, "execute", "BOM [{0}]: Trade Lane [{1}/{2}] with FTA Enabled flag [{3}] and effective dates[{4}/{5}] - NOT eligible for processing.",
							this.bom.bom_id,
							aSrcIVA.ftaCode, 
							aSrcIVA.ctryOfImport,
							aSrcIVA.ftaEnabledFlag,
							aSrcIVA.effectiveFrom,
							aSrcIVA.effectiveTo);
				   continue;
				}
				
				if (!this.queue.request.isTradeLaneProcessingRequested(this.bom.org_code,aSrcIVA.ftaCode, aSrcIVA.ctryOfImport)) {
					MessageFormatter.debug(logger, "execute", "Queue [{0}] Task [{1}]: Trade Lane [{2}/{3}] - processing has not be requested.", 
						this.queue.getName(), 
						this.description,
						aSrcIVA.ftaCode,
						aSrcIVA.ctryOfImport
					);
					continue;
				}
				
				if (!this.queue.ignoreTradeLandConfigurationFlag) {
					if (!qeConfigCache.isTradeLaneEnabled(this.bom.org_code, aSrcIVA.ftaCode, aSrcIVA.ctryOfImport)) { 
						MessageFormatter.debug(logger, "execute", "Queue [{0}] Task [{1}]: Trade Lane [{2}/{3}] - evaluation is not enabled.", 
							this.queue.getName(), 
							this.description,
							aSrcIVA.ftaCode,
							aSrcIVA.ctryOfImport
						);
						continue;
					}
				}
				
				//Check for Date, is in Current Year and future year
				if (!this.businessLogicProcessor.isBOMEligibleForCurrentYearQualification(aSrcIVA, this.bom)
						&& !this.businessLogicProcessor.isBOMEligibleforFutureYearQualification(aSrcIVA, this.bom))
				{
					 continue;
				}
				
				
				if (this.isTradeLaneUpToDate(this.bom, aSrcIVA))
				{
					continue;
				}
				
				aFuture = this.tradeLaneQueue.put(this.bom, aSrcIVA);
				aTaskMgr.addTask(aFuture);
			}
	
			aTaskMgr.waitForCompletion();
			this.bomTracker.setEndTime();
		}
		finally {
			this.bomTracker = null;
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
	 * 
	 *************************************************************************************
	 */
	@Override
	public ArrayList<String> getWorkIdentifiersForTask() 
		throws Exception 
	{
		ArrayList<String>	aList;
		
		aList = new ArrayList<>();
		aList.add(StandardBOMRelatedWorkIdentifierGenerator.execute(this.bom.alt_key_bom));
		return aList;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theBOM
	 * @param	theSrcIVA
	 *************************************************************************************
	 */
	private boolean isTradeLaneUpToDate(BOM theBOM, GPMSourceIVA theSrcIVA)
		throws Exception
	{
		QualTXDetailBOMContainer aContainer;
		boolean aResult = false;

		try
		{
			aContainer = this.queue.qtxDetailUniverse.getQualTXDetailContainer(theBOM.alt_key_bom);
			if (aContainer != null)
			{
				for (QualTXDetail aDetail : aContainer.qualTXDetailList)
				{

					if (!aDetail.fta_code.equalsIgnoreCase(theSrcIVA.ftaCode))
					{
						continue;
					}

					if (!aDetail.ctry_of_import.equalsIgnoreCase(theSrcIVA.ctryOfImport))
					{
						continue;
					}
					
					if (aDetail.iva_code == null || !aDetail.iva_code.equalsIgnoreCase(theSrcIVA.ivaCode))
					{
						continue;
					}

					if (!DateUtils.isSameDay(aDetail.effective_from, theSrcIVA.effectiveFrom) 
							&& !DateUtils.isSameDay(aDetail.effective_to, theSrcIVA.effectiveTo)) continue;

					//
					// The trade lane already exists
					//

					aResult = true;
					break;
				}
			}
		}
		finally
		{
			MessageFormatter.debug(logger, "isTradeLaneUpToDate", "BOM [{0}]: result [{1}]", theBOM.alt_key_bom, aResult);
		}

		return aResult;
	}
}
