package com.ambr.gtm.fta.qps.qualtx.engine;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.qps.bom.BOMComponent;
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
public class ComponentProcessorTask 
	implements TaskInterface, TaskProgressInterface
{
	static Logger	logger = LogManager.getLogger(ComponentProcessorTask.class);

	private ComponentProcessorQueue					queue;
	private ComponentBatch							componentBatch;
	private String 									description;
	private ClassificationProcessorQueue			gpmClassQueue;
	private ComponentIVAPullProcessorQueue			compIVAPullQueue;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theQueue
     * @param	theComponentBatch
     *************************************************************************************
     */
	public ComponentProcessorTask(
		ComponentProcessorQueue		theQueue,
		ComponentBatch				theComponentBatch)
		throws Exception
	{
		this.queue = theQueue;
		this.componentBatch = theComponentBatch;
		this.gpmClassQueue = this.queue.gpmClassQueue;
		this.compIVAPullQueue = this.queue.compIVAPullQueue;
		
		this.description = 
			"BOM" +
			this.componentBatch.getBOMKey() +
			"QTX" +
			this.componentBatch.qualTX.alt_key_qualtx +
			"BOMCOMP" +
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
		Future<? extends TaskInterface>		aFuture;
		AsynchronousTaskManager				aTaskMgr = new AsynchronousTaskManager(this.description);

		try {
			this.componentBatch.createQualTXComponents();
			
			aFuture = this.gpmClassQueue.put(this.componentBatch);
			aTaskMgr.addTask(aFuture);
			
			aFuture = this.compIVAPullQueue.put(this.componentBatch);
			aTaskMgr.addTask(aFuture);
			
			aTaskMgr.waitForCompletion();
		}
		finally {
			this.queue.incrementComponentProcessedCount(this.componentBatch.getSize());
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
