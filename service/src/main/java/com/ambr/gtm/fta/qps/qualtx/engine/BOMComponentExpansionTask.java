package com.ambr.gtm.fta.qps.qualtx.engine;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.gtm.fta.qps.qualtx.engine.result.BOMStatusEnum;
import com.ambr.gtm.fta.qps.qualtx.engine.result.BOMStatusTracker;
import com.ambr.gtm.fta.qps.qualtx.universe.QualTXDetail;
import com.ambr.gtm.fta.qps.qualtx.universe.QualTXDetailBOMContainer;
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
public class BOMComponentExpansionTask
	implements TaskInterface, TaskProgressInterface
{
	static Logger	logger = LogManager.getLogger(BOMComponentExpansionTask.class);

	private BOMComponentExpansionProcessorQueue					queue;
	private BOM													bom;
	private String												description;
	private BOMStatusTracker									bomTracker;
	PreparationEngineQueueUniverse								queueUniverse;
	private QualTXComponentExpansionProcessorQueue				qualTXComponentExpansionQueue;
	 
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQueue
	 * @param	theBOM
	 *************************************************************************************
	 */
	public BOMComponentExpansionTask(BOMComponentExpansionProcessorQueue theQueue, BOM theBOM)
		throws Exception
	{
		this.bom = theBOM;
		this.queue = theQueue;
		this.qualTXComponentExpansionQueue = this.queue.qualTXComponentExpansionProcessorQueue;
		this.queueUniverse = theQueue.queueUniverse;
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
		AsynchronousTaskManager		aTaskMgr = new AsynchronousTaskManager(this.description);
	
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
		
		MessageFormatter.debug(logger, "execute", "Queue [{0}] Task [{1}]: processing, [{3}|{4}]] components.", 
				this.queue.getName(), 
				this.description,
				this.bom.compList.size(),
				this.bom.componentCount
			);
		
		QualTXDetailBOMContainer qtxDetailBOMContainer = this.queueUniverse.qtxDetailUniverse.getQualTXDetailContainer(this.bom.alt_key_bom);
		if(qtxDetailBOMContainer == null)
		{
			MessageFormatter.debug(logger, "execute", "Queue [{0}] Task [{1}] : QualTX Detail for BOM [{2}] is not available for processing.", 
					this.queue.getName(), 
					this.description,
					this.bom.alt_key_bom
				);
				this.bomTracker.status = BOMStatusEnum.EXCLUDED_FROM_PROCESSING;
				this.bomTracker.setEndTime();
				return;
		}
		for(QualTXDetail qualTXDetail : qtxDetailBOMContainer.qualTXDetailList)
		{
			Future<QualTXComponentExpansionTask>	aFuture;
			aFuture = this.qualTXComponentExpansionQueue.put(this.bom, qualTXDetail, this.bomTracker);
			aTaskMgr.addTask(aFuture);
		}

		aTaskMgr.waitForCompletion();
		this.bomTracker.setEndTime();
		this.bomTracker = null;
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
}
