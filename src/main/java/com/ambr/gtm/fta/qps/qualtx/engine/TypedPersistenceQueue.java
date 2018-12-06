package com.ambr.gtm.fta.qps.qualtx.engine;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.transaction.PlatformTransactionManager;

import com.ambr.platform.rdbms.util.bdrp.BatchDataRecordInsertSQLStatement;
import com.ambr.platform.rdbms.util.bdrp.BatchDataRecordPersistenceQueue;
import com.ambr.platform.rdbms.util.bdrp.BatchDataRecordTask;
import com.ambr.platform.rdbms.util.bdrp.DataRecordInterface;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.platform.utils.misc.ParameterizedMessageUtility;
import com.ambr.platform.utils.queue.TaskQueue;
import com.ambr.platform.utils.queue.TaskQueueParameters;

/**
 *****************************************************************************************
 * <P>
 * </P>
 * 
 * 
 *****************************************************************************************
 */
public class TypedPersistenceQueue<T extends DataRecordInterface> 
{
	static final Logger											logger = LogManager.getLogger(TypedPersistenceQueue.class);
	
	private int													batchSize;
	private TaskQueueParameters									queueParams;
	private int													concurrentQueueCount;
	private int													maxWaitPeriodInSecs;
	private DataSource											dataSrc;
	private PlatformTransactionManager							txMgr;
	private ArrayList<BatchDataRecordPersistenceQueue>			batchPersistenceQueueList;
	private BatchDataRecordInsertSQLStatement					insertSQLStatement;
	private int													nextQueueIndex;
	private String												queueName;

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * @param theDataSrc 
	 * @param theTxMgr 
	 * @param theName 
	 * @param theDataRecClass 
	 *************************************************************************************
	 */
	public TypedPersistenceQueue(
		DataSource 					theDataSrc, 
		PlatformTransactionManager 	theTxMgr,
		String						theName,
		Class<T>					theDataRecClass)
		throws Exception
	{
		this(theDataSrc, theTxMgr, theName, theDataRecClass.newInstance());
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * @param theDataSrc 
	 * @param theTxMgr 
	 * @param theName 
	 * @param theDataRecord 
	 *************************************************************************************
	 */
	public TypedPersistenceQueue(
		DataSource 					theDataSrc, 
		PlatformTransactionManager 	theTxMgr,
		String						theName,
		Object						theDataRecord)
		throws Exception
	{
		this.queueName = theName;
		
		this.dataSrc = theDataSrc;
		this.txMgr = theTxMgr;
		this.batchPersistenceQueueList = new ArrayList<>();
		this.insertSQLStatement = new BatchDataRecordInsertSQLStatement(theDataRecord);
		this.concurrentQueueCount = 1;
		this.queueParams = new TaskQueueParameters(5,  100);
		this.batchSize = 100;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public ArrayList<TaskQueue<?>> getInternalQueues()
		throws Exception
	{
		return new ArrayList<TaskQueue<?>>(this.batchPersistenceQueueList);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public String getName()
		throws Exception
	{
		return this.queueName;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	private synchronized BatchDataRecordPersistenceQueue getNextQueue()
		throws Exception
	{
		BatchDataRecordPersistenceQueue		aQueue;
		
		aQueue = this.batchPersistenceQueueList.get(this.nextQueueIndex++);
		
		if (this.nextQueueIndex >= this.batchPersistenceQueueList.size()) {
			this.nextQueueIndex = 0;
		}
		
		return aQueue;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	thePaddingLength
	 *************************************************************************************
	 */
	public String getStatus(int thePaddingLength)
		throws Exception
	{
		ParameterizedMessageUtility	aMsgUtil = new ParameterizedMessageUtility(thePaddingLength);
		
		aMsgUtil.format("Queue [{0}]: concurrent queue count [{1}] max wait period (secs) [{2}]", false, true, 
			this.queueName,
			this.concurrentQueueCount,
			this.maxWaitPeriodInSecs
		);
		
		for (BatchDataRecordPersistenceQueue aQueue : this.batchPersistenceQueueList) {
			aMsgUtil.format(aQueue.getStatus(thePaddingLength+3), true, false);
		}
		
		return aMsgUtil.getMessage();
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theMeasurmentPeriodInSecs
	 *************************************************************************************
	 */
	public HashMap<String, Double> measureThroughput(int theMeasurmentPeriodInSecs)
		throws Exception
	{
		HashMap<String, Double>				aThroughputMap = new HashMap<>();
		HashMap<String, Future<Double>>		aFutureMap = new HashMap<>();
		
		ExecutorService aExecutor = Executors.newFixedThreadPool(this.concurrentQueueCount);
		
		for (BatchDataRecordPersistenceQueue aQueue : this.batchPersistenceQueueList) {
			Future<Double>	aThroughputFuture = aExecutor.submit(()->
				{
					return aQueue.measureThroughput(theMeasurmentPeriodInSecs);
				}
			);
			
			aFutureMap.put(aQueue.getName(), aThroughputFuture);
		}
		
		for (String aQueueName : aFutureMap.keySet()) {
			Future<Double>	aThroughputFuture = aFutureMap.get(aQueueName);
			Double 			aThroughput = aThroughputFuture.get();
			
			aThroughputMap.put(aQueueName, aThroughput);
		}
	
		return aThroughputMap;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQualTX
	 *************************************************************************************
	 */
	public Future<BatchDataRecordTask> put(T theDataRecord)
		throws Exception
	{
		BatchDataRecordPersistenceQueue		aQueue;
		Future<BatchDataRecordTask>			aFuture;
		
		aQueue = this.getNextQueue();
		aFuture = aQueue.put(theDataRecord);
		return aFuture;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * @param theConcurrentQueueCount 
	 *************************************************************************************
	 */
	public TypedPersistenceQueue setConcurrentQueueCount(int theConcurrentQueueCount)
		throws Exception
	{
		MessageFormatter.info(logger, "setConcurrentQueueCount", "Queue [{0}]: Current [{1}] Target [{2}]", 
			this.queueName, 
			this.concurrentQueueCount, 
			theConcurrentQueueCount
		);

		this.concurrentQueueCount = theConcurrentQueueCount;
		return this;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * @param theBatchSize 
	 *************************************************************************************
	 */
	public TypedPersistenceQueue setBatchSize(int theBatchSize)
		throws Exception
	{
		MessageFormatter.info(logger, "setBatchSize", "Queue [{0}]: Current [{1}] Target [{2}]", 
			this.queueName, 
			this.batchSize, 
			theBatchSize
		);
		
		this.batchSize = theBatchSize;
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param theMaxWaitPeriodInSecs 
	 *************************************************************************************
	 */
	public TypedPersistenceQueue setMaxWaitPeriod(int theMaxWaitPeriodInSecs)
		throws Exception
	{
		MessageFormatter.info(logger, "setMaxWaitPeriod", "Queue [{0}]: Current [{1}] Target [{2}]", 
			this.queueName, 
			this.maxWaitPeriodInSecs, 
			theMaxWaitPeriodInSecs
		);
		
		this.maxWaitPeriodInSecs = theMaxWaitPeriodInSecs;
		return this;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param theParams 
	 *************************************************************************************
	 */
	public TypedPersistenceQueue setQueueParams(TaskQueueParameters theParams)
		throws Exception
	{
		theParams.log(logger, "setQueueParams", this.queueParams);
		this.queueParams = theParams;
		return this;
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
		for (BatchDataRecordPersistenceQueue aQueue : this.batchPersistenceQueueList) {
			try {
				aQueue.shutdown(theOrderlyFlag);
			}
			catch (Exception e) {
				MessageFormatter.error(logger, "shutdown", e, "Queue [{0}]", aQueue.getName());
			}
		}
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public void start()
		throws Exception
	{
		BatchDataRecordPersistenceQueue		aQueue;
		
		this.nextQueueIndex = 0;
		this.batchPersistenceQueueList = new ArrayList<>();

		for (int aIndex = 1; aIndex <= this.concurrentQueueCount; aIndex++) {
			aQueue = new BatchDataRecordPersistenceQueue(
				MessageFormat.format("{0} [{1}]", this.queueName, aIndex),
				this.batchSize,
				this.maxWaitPeriodInSecs,
				this.insertSQLStatement, 
				this.txMgr, 
				this.dataSrc
			);

			aQueue.setQueueParemeters(this.queueParams);
			aQueue.start();

			this.batchPersistenceQueueList.add(aQueue);
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
		for (BatchDataRecordPersistenceQueue aQueue : this.batchPersistenceQueueList) {
			try {
				aQueue.waitForCompletion();
			}
			catch (Exception e) {
				MessageFormatter.error(logger, "waitForCompletion", e, "Queue [{0}]", aQueue.getName());
			}
		}
	}
}
