package com.ambr.gtm.fta.qps.qualtx.engine;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.transaction.PlatformTransactionManager;

import com.ambr.platform.rdbms.util.bdrp.BatchDataRecordPersistenceQueue;
import com.ambr.platform.rdbms.util.bdrp.DataRecordInterface;
import com.ambr.platform.rdbms.util.bdrp.DataRecordValueMapInterface;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.platform.utils.queue.TaskQueueParameters;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class DataExtensionPersistenceQueue<T extends DataRecordValueMapInterface> 
{
	static Logger	logger = LogManager.getLogger(DataExtensionPersistenceQueue.class);

	private HashMap<String, TypedPersistenceQueue<T>>		persistenceQueueTable;
	private DataSource										dataSrc;
	private PlatformTransactionManager						txMgr;
	private TaskQueueParameters								queueParams;
	private int												batchInsertConcurrentQueueCount;
	private int 											batchSize;
	private int 											batchInsertMaxWaitPeriodInSecs;
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theDataSrc
     * @param	theTxMgr
     *************************************************************************************
     */
	public DataExtensionPersistenceQueue(DataSource theDataSrc, PlatformTransactionManager theTxMgr)
		throws Exception
	{
		this.persistenceQueueTable = new HashMap<>();
		this.dataSrc = theDataSrc;
		this.txMgr = theTxMgr;
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public ArrayList<TypedPersistenceQueue<?>> getInternalQueues()
		throws Exception
	{
		return new ArrayList<>(this.persistenceQueueTable.values());
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theGroupName
     * @param	theDataRecordClass
     * @param	theCreateFlag
     *************************************************************************************
     */
	public synchronized TypedPersistenceQueue<T> getQueue(
		String 		theGroupName,
		Class<T> 	theDataRecordClass,
		boolean 	theCreateFlag)
		throws Exception
	{
		return this.getQueue(theGroupName, theDataRecordClass.newInstance(), theCreateFlag);
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theGroupName
     * @param	theDataRecordClass
     * @param	theCreateFlag
     *************************************************************************************
     */
	public synchronized TypedPersistenceQueue<T> getQueue(
		String 		theGroupName,
		Object	 	theDataRecordInstance,
		boolean 	theCreateFlag)
		throws Exception
	{
		String						aQueueName = MessageFormat.format("Data Extension [{0}] Persistence Queue", theGroupName);
		TypedPersistenceQueue<T>	aQueue;
		
		aQueue = this.persistenceQueueTable.get(aQueueName);
		if ((aQueue == null) && theCreateFlag) {
			aQueue = new TypedPersistenceQueue<T>(
				this.dataSrc, 
				this.txMgr, 
				aQueueName, 
				theDataRecordInstance
			);
			
			aQueue.setBatchSize(this.batchSize);
			aQueue.setConcurrentQueueCount(this.batchInsertConcurrentQueueCount);
			aQueue.setMaxWaitPeriod(this.batchInsertMaxWaitPeriodInSecs);
			aQueue.setQueueParams(this.queueParams);
			aQueue.start();
			
			this.persistenceQueueTable.put(aQueue.getName(), aQueue);
		}
		
		return aQueue;
	}

    /**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theBatchInsertSize
	 *************************************************************************************
	 */
	public void setBatchSize(int theBatchInsertSize)
		throws Exception
	{
		this.batchSize = theBatchInsertSize;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theBatchInsertConcurrentQueueCount
	 *************************************************************************************
	 */
	public void setConcurrentQueueCount(int theBatchInsertConcurrentQueueCount)
		throws Exception
	{
		this.batchInsertConcurrentQueueCount = theBatchInsertConcurrentQueueCount;
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theQueueParams
     *************************************************************************************
     */
	public void setQueueParams(TaskQueueParameters theQueueParams)
		throws Exception
	{
		this.queueParams = theQueueParams;
	}

    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBatchInsertMaxWaitPeriodInSecs
     *************************************************************************************
     */
	public void setMaxWaitPeriod(int theBatchInsertMaxWaitPeriodInSecs)
		throws Exception
	{
		this.batchInsertMaxWaitPeriodInSecs = theBatchInsertMaxWaitPeriodInSecs;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theOrderlyFlag
	 *************************************************************************************
	 */
	public void shutdown(boolean theOrderlyFlag)
		throws Exception
	{
		for (TypedPersistenceQueue<?> aQueue : this.persistenceQueueTable.values()) {
			try {
				aQueue.shutdown(theOrderlyFlag);
			}
			catch (Exception e) {
				MessageFormatter.error(logger, "shutdown", e, "Queue [{0}].  unexpected error", aQueue.getName());
			}
		}
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public void waitForCompletion()
		throws Exception
	{
		for (TypedPersistenceQueue<T> aQueue : this.persistenceQueueTable.values()) {
			try {
				aQueue.waitForCompletion();
			}
			catch (Exception e) {
				MessageFormatter.error(logger, "waitForCompletion", e, "Queue [{0}]", aQueue.getName());
			}
		}
	}
}
