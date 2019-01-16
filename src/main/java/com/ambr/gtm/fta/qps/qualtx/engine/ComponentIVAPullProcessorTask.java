package com.ambr.gtm.fta.qps.qualtx.engine;

import java.util.ArrayList;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import com.ambr.gtm.fta.qps.bom.BOMComponent;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsCache;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationProductContainer;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationProductContainerCache;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAContainerCache;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAProductSourceContainer;
import com.ambr.gtm.fta.qps.util.CumulationComputationRule;
import com.ambr.gtm.fta.qps.util.DetermineComponentCOO;
import com.ambr.gtm.fta.qps.util.PreviousYearQualificationRule;
import com.ambr.gtm.fta.qps.util.QualTXComponentUtility;
import com.ambr.platform.utils.queue.TaskInterface;
import com.ambr.platform.utils.queue.TaskProgressInterface;
import com.ambr.platform.utils.queue.TaskQueue;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class ComponentIVAPullProcessorTask 
	implements TaskInterface, TaskProgressInterface
{
	static Logger	logger = LogManager.getLogger(ComponentIVAPullProcessorTask.class);

	private ComponentIVAPullProcessorQueue			queue;
	private ComponentBatch							componentBatch;
	private String									description;
	private GPMSourceIVAContainerCache				ivaCache;
	private GPMClaimDetailsCache					claimDetailsCache;
	private GPMClassificationProductContainerCache	gpmClassCache;
	private CumulationComputationRule				cumulationComputationRule;
	private DetermineComponentCOO					determineComponentCOO;
	private QualTXBusinessLogicProcessor			businessProcessor;
	private PreviousYearQualificationRule			previousYearQualificationRule;
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theQueue
     * @param	theComponentBatch
     *************************************************************************************
     */
	public ComponentIVAPullProcessorTask(
		ComponentIVAPullProcessorQueue	theQueue,
		ComponentBatch					theComponentBatch)
		throws Exception
	{
		this.queue = theQueue;
		this.componentBatch = theComponentBatch;
		this.ivaCache = this.queue.ivaCache;
		this.claimDetailsCache = this.queue.claimDetailsCache;
		this.businessProcessor = this.queue.queueUniverse.qtxBusinessLogicProcessor;
		this.determineComponentCOO = this.businessProcessor.determineComponentCOO;
		this.cumulationComputationRule = this.businessProcessor.cumulationComputationRule;
		this.previousYearQualificationRule= this.businessProcessor.previousYearQualificationRule;
		this.gpmClassCache =  this.queue.queueUniverse.gpmClassCache;
		this.description =
			"BOM." +
			this.componentBatch.getBOMKey() +
			"QTX." +
			this.componentBatch.qualTX.alt_key_qualtx +
			"COMPONENT." +
			this.componentBatch.getStartIndex() +
			"-" +
			this.componentBatch.getEndIndex()
		;
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
		GPMSourceIVAProductSourceContainer		aContainer;
		GPMClassificationProductContainer       aGPMClassContainer;
		BOMComponent 							aBOMComp;
		QualTXComponent 						aQualTXComp;
		try {
			for (int aCompIndex = 0; aCompIndex < this.componentBatch.getSize(); aCompIndex++) {
				aBOMComp = this.componentBatch.getBOMComponent(aCompIndex);
				aQualTXComp = this.componentBatch.getQualTXComponent(aCompIndex);
				
				if(aQualTXComp == null) continue;

				try {
		
					QualTXComponentUtility aQualTXComponentUtility = new QualTXComponentUtility(aQualTXComp, aBOMComp, this.claimDetailsCache, this.ivaCache, this.queue.queueUniverse.dataExtCfgRepos, this.componentBatch.statusTracker);
					aQualTXComponentUtility.setQualTXBusinessLogicProcessor(this.businessProcessor);
					aQualTXComponentUtility.setGPMClassificationCache(this.gpmClassCache);

					aContainer = aQualTXComponentUtility.pullIVAData();
					this.componentBatch.statusTracker.sourceIVAPullSuccess(aQualTXComp);
				}
				catch (Exception e) {
					this.componentBatch.statusTracker.sourceIVAPullFailure(aQualTXComp, e);
				}
			}
		}
		finally {
			this.queue.incrementIVAPullCount(this.componentBatch.getSize());
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
		return this.componentBatch.getWorkIdentifiersForTask();
	}
}
