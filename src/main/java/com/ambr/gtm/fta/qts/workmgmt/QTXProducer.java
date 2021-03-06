package com.ambr.gtm.fta.qts.workmgmt;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import com.ambr.gtm.fta.qts.QTXWorkRepository;
import com.ambr.gtm.fta.qts.WorkManagementException;
import com.ambr.gtm.fta.qts.trade.MDIQualTxRepository;
import com.ambr.gtm.fta.qts.util.BlockingExecutor;
import com.ambr.gtm.fta.qts.util.LongStatistic;
import com.ambr.gtm.fta.qts.util.RunnableTuple;
import com.ambr.gtm.fta.qts.util.StatisticMonitor;
import com.ambr.platform.rdbms.bootstrap.SchemaDescriptorService;
import com.ambr.platform.uoid.UniversalObjectIDGenerator;

//TODO expose interface to stop/start producer
//TODO expose interface to alter consumer pool size
//TODO intermediate producers cannot easily submit batches of work to consumer, since work is submitted to them in quantities of 1 ... review further
//TODO use rest template and check performance
//TODO setup producers as beans and initialize based on command line args (i.e. should producer initialize)
//TODO other stats to include current queue size, estimated time to complete based on last X minutes of processing?
public abstract class QTXProducer implements Runnable
{
	private static final Logger logger = LogManager.getLogger(QTXProducer.class);
	
	protected SchemaDescriptorService schemaService;
	protected JdbcTemplate template;
	protected PlatformTransactionManager txMgr;
	
	private BlockingExecutor threadExecutor;
	protected int fetchSize;
	protected int threads;
	protected int readAhead;
	protected int batchSize;
	protected int sleepInterval;
	
	private boolean isActive;
	
	private StatisticMonitor processingTimes;
	
	private UniversalObjectIDGenerator idGenerator;
	private QTXWorkRepository workRepository;
	private MDIQualTxRepository qualtxRepository;
	
	protected QTXMonitoredMetrics metrics;
	protected ArrayList<QTXMonitoredMetrics> monitoredMetrics = new ArrayList<QTXMonitoredMetrics>();
	
	public QTXProducer(SchemaDescriptorService schemaService, PlatformTransactionManager txMgr, JdbcTemplate template)
	{
		this.schemaService = schemaService;
		this.template = template;
		this.txMgr = txMgr;
		
		this.initializeStats();
	}
	
	public QTXMonitoredMetrics getMetrics()
	{
		return this.metrics;
	}
	
	public double getThroughput(long perUnitTime)
	{
		return this.processingTimes.getThroughput(perUnitTime);
	}
	
	public QTXMonitoredMetrics addMonitoredMetrics()
	{
		QTXMonitoredMetrics monitoredMetric = new QTXMonitoredMetrics(this.getClass().getSimpleName());
		
		monitoredMetric.start();
		
		synchronized (this.monitoredMetrics)
		{
			this.monitoredMetrics.add(monitoredMetric);
		}
		
		return monitoredMetric;
	}
	
	public QTXMonitoredMetrics removeMonitoredMetrics(QTXMonitoredMetrics monitoredMetric)
	{
		synchronized (this.monitoredMetrics)
		{
			this.monitoredMetrics.remove(monitoredMetric);
		}
		
		monitoredMetric.stop();
		
		return monitoredMetric;
	}
	
	public Iterator<RunnableTuple> pendingQueueEntries()
	{
		return this.threadExecutor.pendingQueueEntries();
	}
	
	public PlatformTransactionManager getTransactionManager()
	{
		return this.txMgr;
	}
	
	public JdbcTemplate getJdbcTemplate()
	{
		return this.template;
	}
	
	public void init(int threads, int readAhead, int fetchSize, int batchSize, int sleepInterval, MDIQualTxRepository qualTxRepository, QTXWorkRepository workRepository, UniversalObjectIDGenerator idGenerator) throws WorkManagementException
	{
		this.processingTimes = new StatisticMonitor("ConsumerProcessingTime", 60, 5000);
		
		this.idGenerator = idGenerator;
		this.workRepository = workRepository;
		this.qualtxRepository = qualTxRepository;
		
		this.threads = threads;
		this.readAhead = readAhead;
		this.fetchSize = fetchSize;
		this.batchSize = batchSize;
		this.sleepInterval = sleepInterval;
		
		this.template.setFetchSize(this.fetchSize);
		
		logger.debug(this.getClass().getSimpleName() + " initializing with " + this.threads + " threads");
		logger.debug(this.getClass().getSimpleName() + " initializing with " + this.readAhead + " readAhead");
		logger.debug(this.getClass().getSimpleName() + " initializing with " + this.fetchSize + " fetchSize");
		logger.debug(this.getClass().getSimpleName() + " initializing with " + this.batchSize + " batchSize");
		logger.debug(this.getClass().getSimpleName() + " initializing with " + this.sleepInterval + " sleepInterval");
		
		if (this.sleepInterval <= 0) throw new WorkManagementException(this.getClass().getName() + " cannot have a sleepInterval <= 0");
		
		this.threadExecutor = new BlockingExecutor(threads, readAhead);
	}
	
	public QTXWorkRepository getWorkRepository()
	{
		return this.workRepository;
	}
	
	public MDIQualTxRepository getQualTxRepository()
	{
		return this.qualtxRepository;
	}
	
	public void completedStats(long itemCount, long duration)
	{
		this.updateCompletedStats(this.metrics, itemCount, duration);
		
		for (QTXMonitoredMetrics monitoredMetric : this.monitoredMetrics)
		{
			this.updateCompletedStats(monitoredMetric, itemCount, duration);
		}
	}
	
	private void updateCompletedStats(QTXMonitoredMetrics monitoredMetric, long itemCount, long duration)
	{
		monitoredMetric.add(itemCount, duration);
		
		LongStatistic longStat = new LongStatistic(duration);
		this.processingTimes.addStatistic(itemCount, longStat);
	}
	
	public void submit(QTXConsumer<?> task)
	{
		this.addedStats();
		
		task.setProducer(this);
		this.threadExecutor.submit(task);
	}
	
	private void addedStats()
	{
		this.metrics.increment();
		
		for (QTXMonitoredMetrics monitoredMetric : this.monitoredMetrics)
		{
			monitoredMetric.increment();
		}
	}
	
	public void setTrackWork(boolean track)
	{
		this.threadExecutor.setTrackWork(track);
	}
	
	public void waitTillQueueEmpty()
	{
		this.threadExecutor.waitTillQueueEmpty();
	}
	
	public void startup() throws WorkManagementException
	{
		logger.debug("Startup called");
		
		if (this.isActive == true) throw new WorkManagementException("Producer is already running");
		
		this.isActive = true;
		
		new Thread(this).start();
		
		logger.debug("Startup complete");
	}
	
	public void shutdown()
	{
		logger.debug("Shutdown called");
		
		this.isActive = false;
		this.threadExecutor.shutdown();
		
		logger.debug("Shutdown complete ... note that consumers may still be running until completion");
	}
	
	public void awaitTermination(long time, TimeUnit unit) throws InterruptedException
	{
		this.isActive = false;
		this.threadExecutor.awaitTermination(time, unit);
	}
	
	public boolean hasWork()
	{
		return this.threadExecutor.hasWork();
	}

	public void doWork() throws Exception
	{
		if (this.hasWork() == true)
		{
			return;
		}
		
		this.threadExecutor.clearTrackedWork();

		this.findWork();
	}
	
	protected abstract void findWork() throws Exception;

	public void run()
	{
		while (this.isActive)
		{
			try
			{
				this.doWork();
			}
			catch (Exception exe)
			{
				logger.error("Exception while proccessing the Requalification works, Retry happens after : " + this.sleepInterval * 1000 + " Seconds", exe);
			}

			try
			{
				Thread.sleep(this.sleepInterval * 1000);
			}
			catch (InterruptedException i)
			{
				logger.error("Interrupt captured.  Shutting down", i);
				this.isActive = false;
			}
		}
	}
	
	public long getNextSequence() throws Exception
	{
		return this.idGenerator.generate().getLSB();
	}
	
	public UniversalObjectIDGenerator getIDGenerator() throws Exception
	{
		return this.idGenerator;
	}
	
	public void initializeStats()
	{
		this.metrics = new QTXMonitoredMetrics(this.getClass().getSimpleName());
		
		this.metrics.start();
	}
}
