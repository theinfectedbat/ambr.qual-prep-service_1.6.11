package com.ambr.gtm.fta.qps.qualtx.engine;

import java.text.MessageFormat;
import java.util.ArrayList;

import com.ambr.gtm.fta.qps.bom.BOMComponent;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationProductContainer;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationProductContainerCache;
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
public class QualTXComponentClassificationProcessorTask 
	implements TaskInterface, TaskProgressInterface
{
	private ComponentBatch							componentBatch;
	private GPMClassificationProductContainer		gpmClassContainer;
	private ClassificationProcessorQueue			queue;
	private GPMClassificationProductContainerCache	gpmClassCache;
	private String 									description;
	private QualTXBusinessLogicProcessor qtxBusinessLogicProcessor;
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQueue
	 * @param	theComponentBatch
	 *************************************************************************************
	 */
	public QualTXComponentClassificationProcessorTask(
		ClassificationProcessorQueue 	theQueue, 
		ComponentBatch					theComponentBatch)
		throws Exception
	{
		this.queue = theQueue;
		this.gpmClassCache = this.queue.gpmClassCache;
		this.componentBatch = theComponentBatch;
		this.qtxBusinessLogicProcessor = this.queue.queueUniverse.qtxBusinessLogicProcessor;
		
		this.description = 
			"BOM." + this.componentBatch.getBOMKey() +
			"QTX." + this.componentBatch.qualTX.alt_key_qualtx +
			"COMPONENT." + this.componentBatch.getStartIndex() +
			"-" + this.componentBatch.getEndIndex()
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
		try {
			for (int aCompIndex = 0; aCompIndex < this.componentBatch.getSize(); aCompIndex++) {
				BOMComponent 		aBOMComp = this.componentBatch.getBOMComponent(aCompIndex);
				QualTXComponent 	aQualTXComp = this.componentBatch.getQualTXComponent(aCompIndex);

				QualTXComponentUtility aQualTXComponentUtility = new QualTXComponentUtility(aQualTXComp, aBOMComp, this.gpmClassCache, this.componentBatch.statusTracker);
				aQualTXComponentUtility.setQualTXBusinessLogicProcessor(qtxBusinessLogicProcessor);
				this.gpmClassContainer = aQualTXComponentUtility.pullCtryCmplData();
			}
		}
		finally {
			this.queue.incrementClassificationCount(this.componentBatch.getSize());
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
