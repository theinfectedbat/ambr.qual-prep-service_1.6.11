package com.ambr.gtm.fta.qps.qualtx.engine;

import java.text.MessageFormat;
import java.util.ArrayList;

import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationProductContainer;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationProductContainerCache;
import com.ambr.gtm.fta.qps.qualtx.engine.result.TradeLaneStatusTracker;
import com.ambr.platform.utils.queue.TaskInterface;
import com.ambr.platform.utils.queue.TaskProgressInterface;
import com.ambr.platform.utils.queue.TaskQueue;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class QualTXClassificationProcessorTask 
	implements TaskInterface, TaskProgressInterface
{
	private BOM 									bom;
	private QualTX									qualTX;
	private GPMClassificationProductContainer		gpmClassContainer;
	private ClassificationProcessorQueue			queue;
	private String 									description;
	private GPMClassificationProductContainerCache	gpmClassCache;
	private QualTXBusinessLogicProcessor 			qualTXBusinessLogicProcessor;
	private TradeLaneStatusTracker					statusTracker;
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQueue
	 * @param	theBOM
	 * @param 	theQualTX 
	 * @param	theStatusTracker
	 *************************************************************************************
	 */
	public QualTXClassificationProcessorTask(
		ClassificationProcessorQueue 	theQueue, 
		BOM 							theBOM, 
		QualTX							theQualTX,
		TradeLaneStatusTracker 			theStatusTracker)
		throws Exception
	{
		this.queue = theQueue;
		this.gpmClassCache = this.queue.gpmClassCache;
		this.bom = theBOM;
		this.qualTX = theQualTX;
		this.statusTracker = theStatusTracker;
		this.qualTXBusinessLogicProcessor = this.queue.queueUniverse.qtxBusinessLogicProcessor;
		this.description =
			"BOM." + this.bom.alt_key_bom +
			"QTX." + this.qualTX.alt_key_qualtx
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
		//String ctryOfImport = this.qualTX.ctry_of_import;
		try {
			
			this.gpmClassContainer = this.gpmClassCache.getGPMClassificationsByProduct(this.bom.prod_key);
			if (this.gpmClassContainer == null) {
				return;
			}

			if (this.gpmClassContainer.classificationList.size() == 0) {
				return;
			}
			
//			this.qualTX.hs_num = this.gpmClassContainer.classificationList.get(0).imHS1;
//			this.qualTX.prod_ctry_cmpl_key = this.gpmClassContainer.classificationList.get(0).cmplKey;

			
			this.qualTXBusinessLogicProcessor.setQualTXHeaderHSNumber(this.qualTX, this.gpmClassContainer.classificationList);
		
			this.statusTracker.classificationPullSuccess();
		}
		catch (Exception e) {
			this.statusTracker.classificationPullFailure(e);
		}
		finally {
			this.queue.incrementClassificationCount(1);
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
		aList.add(StandardBOMRelatedWorkIdentifierGenerator.execute(this.bom.alt_key_bom, this.qualTX.alt_key_qualtx, "QTX"));
		return aList;
	}
}
