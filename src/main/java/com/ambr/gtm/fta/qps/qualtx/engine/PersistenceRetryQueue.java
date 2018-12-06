package com.ambr.gtm.fta.qps.qualtx.engine;

import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.platform.uoid.UniversalObjectIDGenerator;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.platform.utils.queue.TaskQueue;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class PersistenceRetryQueue
	extends TaskQueue<PersistenceRetryTask>
{
	static Logger						logger = LogManager.getLogger(PersistenceRetryQueue.class);

	UniversalObjectIDGenerator			idGenerator;
	PreparationEngineQueueUniverse		queueUniverse;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theQueueUniverse
     *************************************************************************************
     */
	public PersistenceRetryQueue(PreparationEngineQueueUniverse theQueueUniverse)
		throws Exception
	{
		super("Persistence Retry Queue", true);
		
		this.queueUniverse = theQueueUniverse;
		this.idGenerator = this.queueUniverse.idGenerator;
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theQualTX
     *************************************************************************************
     */
	public Future<PersistenceRetryTask> put(QualTX theQualTX)
		throws Exception
	{
		Future<PersistenceRetryTask>	aFuture;
		
		MessageFormatter.debug(logger, "put", "Qual TX [{0}]: retrying", theQualTX.alt_key_qualtx);
		aFuture = this.execute(new PersistenceRetryTask(this, theQualTX));
		return aFuture;
	}
}
