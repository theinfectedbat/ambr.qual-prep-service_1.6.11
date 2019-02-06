package com.ambr.gtm.fta.qts.workmgmt;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.ambr.gtm.fta.qps.bom.api.BOMUniverseBOMClientAPI;
import com.ambr.gtm.fta.qps.gpmclass.api.GetGPMClassificationsByProductFromUniverseClientAPI;
import com.ambr.gtm.fta.qps.gpmsrciva.api.GetGPMSourceIVAByProductFromUniverseClientAPI;
import com.ambr.gtm.fta.qps.qualtx.engine.PreparationEngineQueueUniverse;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXBusinessLogicProcessor;
import com.ambr.gtm.fta.qps.qualtx.engine.api.CacheRefreshInformation;
import com.ambr.gtm.fta.qps.qualtx.engine.api.GetCacheRefreshInformationClientAPI;
import com.ambr.gtm.fta.qts.RequalificationBOMStatus;
import com.ambr.gtm.fta.qts.RequalificationTradeLaneStatus;
import com.ambr.gtm.fta.qts.util.RunnableTuple;
import com.ambr.platform.rdbms.bootstrap.SchemaDescriptorService;
import com.ambr.platform.utils.log.PerformanceTracker;

public class QTXWorkProducer extends QTXProducer
{
	private static final Logger logger = LogManager.getLogger(QTXWorkProducer.class);

	private QTXCompWorkProducer compWorkProducer;
	private QTXWorkPersistenceProducer workPersistenceProducer;
	private QTXStageProducer stageProducer;

	private BOMUniverseBOMClientAPI bomUniverseBOMClientAPI;
	private GetGPMClassificationsByProductFromUniverseClientAPI gpmClassificationsByProductAPI;
	private GetGPMSourceIVAByProductFromUniverseClientAPI gpmSourceIVAByProductFromUniverseClientAPI;
	private GetCacheRefreshInformationClientAPI cacheRefreshInformationClientAPI;
	public PreparationEngineQueueUniverse queueUniverse;

	private int status;

	private long maxObjects;

	public static final int REQUAL_SERVICE_AVAILABLE = 0;
	public static final int REQUAL_SERVICE_IN_PROGRESS = 1;

	public QTXWorkProducer(SchemaDescriptorService schemaService, PlatformTransactionManager txMgr,	JdbcTemplate template)
	{
		super(schemaService, txMgr, template);
	}

	private QualTXBusinessLogicProcessor qtxBusinessLogicProcessor;

	public QualTXBusinessLogicProcessor getQtxBusinessLogicProcessor()
	{
		return qtxBusinessLogicProcessor;
	}

	public void setQtxBusinessLogicProcessor(QualTXBusinessLogicProcessor qtxBusinessLogicProcessor)
	{
		this.qtxBusinessLogicProcessor = qtxBusinessLogicProcessor;
	}

	public void setQueueUniverse(PreparationEngineQueueUniverse theQueueUniverse) throws Exception
	{
		this.queueUniverse = theQueueUniverse;
	}

	public void setMaxObjects(long max)
	{
		this.maxObjects = max;
	}

	public ArrayList<QTXMonitoredMetrics> getMonitoredMetrics()
	{
		ArrayList<QTXMonitoredMetrics> metricList = new ArrayList<QTXMonitoredMetrics>();

		metricList.add(this.getMetrics());
		metricList.add(this.stageProducer.getMetrics());
		metricList.add(this.compWorkProducer.getMetrics());
		metricList.add(this.workPersistenceProducer.getMetrics());

		return metricList;
	}

	public void setAPI(BOMUniverseBOMClientAPI bomUniverseBOMClientAPI,
			GetGPMClassificationsByProductFromUniverseClientAPI gpmClassificationsByProductAPI,
			GetGPMSourceIVAByProductFromUniverseClientAPI gpmSourceIVAByProductFromUniverseClientAPI,
			GetCacheRefreshInformationClientAPI cacheRefreshInformationClientAPI)
	{
		this.bomUniverseBOMClientAPI = bomUniverseBOMClientAPI;
		this.gpmClassificationsByProductAPI = gpmClassificationsByProductAPI;
		this.gpmSourceIVAByProductFromUniverseClientAPI = gpmSourceIVAByProductFromUniverseClientAPI;
		this.cacheRefreshInformationClientAPI = cacheRefreshInformationClientAPI;
	}

	public void setQTXWorkPersistenceProducer(QTXWorkPersistenceProducer workPersistenceProducer)
	{
		this.workPersistenceProducer = workPersistenceProducer;
	}

	public void setQTXCompWorkProducer(QTXCompWorkProducer compWorkProducer)
	{
		this.compWorkProducer = compWorkProducer;
	}

	public void setQTXStageProducer(QTXStageProducer stageProducer)
	{
		this.stageProducer = stageProducer;
	}

	public int getStatus()
	{
		return this.status;
	}

	public void setStatus(int status)
	{
		this.status = status;
	}

	protected synchronized void registeredWorkCompleted(WorkPackage workPackage)
	{
		logger.debug("Registering work completed " + workPackage.work.qtx_wid);

		workPackage.headerProcessed = true;

		this.checkWorkForCompleteness(workPackage);
	}

	protected synchronized void registeredCompWorkCompleted(CompWorkPackage compWorkPackage)
	{
		logger.trace("Registering work component completed " + compWorkPackage.compWork.qtx_wid + ":" + compWorkPackage.compWork.qtx_comp_wid);

		WorkPackage workPackage = compWorkPackage.getParentWorkPackage();

		workPackage.compWorkCompleted(compWorkPackage);

		this.checkWorkForCompleteness(workPackage);
	}

	// The next linked work package can only process once the head and comps are
	// complete. Otherwise comp consumers could pickup comps concurrently and
	// process out of order or other concurrency issues if two+ consumers target
	// the same comp.
	// TODO check to see if work package failed due to LockException - if so flag the whole set as RETRY and discard set of workpackages
	private void checkWorkForCompleteness(WorkPackage workPackage)
	{
		WorkPackage linkedPackage = workPackage.getLinkedPackage();
		if (workPackage.isWorkComplete() && linkedPackage != null)
		{
			this.submitWork(linkedPackage);

			return;
		}

		if (workPackage.isChainComplete())
		{
			logger.debug("Work fully complete " + workPackage.work.qtx_wid + " submitting root " + workPackage.getRootPackage().work.qtx_wid);

			this.workPersistenceProducer.submitWork(workPackage.getRootPackage());
		}
	}

	public void submitWork(WorkPackage workPackage)
	{
		logger.debug("Submitting work " + workPackage.work.qtx_wid);

		QTXWorkConsumer consumer = new QTXWorkConsumer(workPackage);
		consumer.setQtxBusinessLogicProcessor(this.qtxBusinessLogicProcessor);
		this.submit(consumer);

		for (CompWorkPackage compWorkPackage : workPackage.compWorks.values())
		{
			this.compWorkProducer.submitWork(compWorkPackage);
		}
	}

	public void executeFindWork() throws Exception
	{
		try
		{
			setStatus(REQUAL_SERVICE_IN_PROGRESS);
			this.findWork();
		}
		finally
		{
			setStatus(REQUAL_SERVICE_AVAILABLE);
		}
	}

	@Override
	protected void findWork() throws Exception
	{
		PerformanceTracker tracker = null;
		int processedCount = 0;
		do
		{
			tracker = new PerformanceTracker(logger, Level.INFO, "QTXProducer starting");
			try
			{
				processedCount = this.processWork();
			}
			finally
			{
				tracker.stop("QTXWork producer completed {0} items", new Object[] {processedCount});
			}
		}
		while (processedCount > 0);
	}

	private int processWork() throws Exception
	{
		HashMap<Long, WorkPackage> stagedWork = null;

		// TODO look to see if the staged work can be returned WorkProducer. load existing pending ar_qtx_work records then add staged work created by Stage Producer
		PerformanceTracker tracker = new PerformanceTracker(logger, Level.INFO, "FindStagingWork");
		tracker.start();
		try
		{
			if (this.stageProducer == null)
				throw new Exception("QTXStageProducer is not assigned");
			
			this.stageProducer.executeFindWork();
		}
		catch (Exception e)
		{
			logger.error("Stage Producer threw and error while converting stage data to work records", e);
		}
		finally
		{
			tracker.stop("Records processed {0}", new Object[] {this.stageProducer.getWorkProcessed()});
		}

		tracker = new PerformanceTracker(logger, Level.INFO, "RefreshCache");
		tracker.start();
		long bestTime;
		try
		{
			CacheRefreshInformation cacheInfo = this.cacheRefreshInformationClientAPI.execute();
			bestTime = cacheInfo.cacheLoadStart;
		}
		finally
		{
			tracker.stop("Refresh complete", null);
		}

		tracker = new PerformanceTracker(logger, Level.INFO, "LoadingUniverse");
		tracker.start();
		TransactionStatus status = this.txMgr.getTransaction(new DefaultTransactionDefinition());
		try
		{
			QTXWorkUniverse workStaging = new QTXWorkUniverse(this.getIDGenerator(), this.txMgr,
				this.schemaService.getPrimarySchemaDescriptor(), this.template, bomUniverseBOMClientAPI,
				gpmClassificationsByProductAPI, gpmSourceIVAByProductFromUniverseClientAPI, this.batchSize,
				this.maxObjects);

			logger.debug("Requalification loading work as of " + new Timestamp(bestTime));
			stagedWork = workStaging.stageWork(bestTime, this.template);

			this.txMgr.commit(status);

			logger.debug("work producer transaction committed");
		}
		catch (Exception e)
		{
			logger.error("Staging work failed, work items failed to load/stage properly - issuing rollback", e);

			try
			{
				this.txMgr.rollback(status);
			}
			catch (Exception rollbackException)
			{
				logger.error("Error encountered during rollback", rollbackException);
			}

			throw e;
		}
		finally
		{
			tracker.stop("Universe load complete", null);
		}

		// TODO pull out list of work and order by priority then submit
		for (Iterator<WorkPackage> iterator = stagedWork.values().iterator(); iterator.hasNext();)
			this.submitWork(iterator.next());

		// !!TODO check to see when work is completed (all producers) and then go back for more ...
		this.waitTillQueueEmpty();
		this.compWorkProducer.waitTillQueueEmpty();
		this.workPersistenceProducer.waitTillQueueEmpty();

		return stagedWork.size();
	}

	public RequalificationBOMStatus getRequalificationBOMStatus(long bomKey)
	{
		RequalificationBOMStatus finalStatus = new RequalificationBOMStatus();

		finalStatus.bomKey = bomKey;
		finalStatus.requestTime = System.currentTimeMillis();

		// TODO this isnt right. duration is header or comp whichever is greater PLUS persistence
		// TODO plus the throughput of the persistence guy factored by lane position in work/comp_work queue
		finalStatus.putTradeLaneStatus(this.getHeaderRequalificationBOMStatus(bomKey).tradeLaneStatusList);
		finalStatus
				.putTradeLaneStatus(this.compWorkProducer.getCompRequalificationBOMStatus(bomKey).tradeLaneStatusList);
		finalStatus.sumTradeLaneStatus(
				this.workPersistenceProducer.getPersistentRequalificationBOMStatus(bomKey).tradeLaneStatusList);

		return finalStatus;
	}

	public RequalificationBOMStatus getHeaderRequalificationBOMStatus(long bomKey)
	{
		RequalificationBOMStatus bomStatus = new RequalificationBOMStatus();

		bomStatus.bomKey = bomKey;

		this.getTradeLaneStatsForBOM(bomKey, bomStatus);

		return bomStatus;
	}

	// TODO can this be a generic method at QTXProducer level - method is
	// currently a "copy" in qtxworkproducer/qtxworkpersistenceproducer/qtxcompworkproducer
	public void getTradeLaneStatsForBOM(long bomKey, RequalificationBOMStatus bomStatus)
	{
		long requestTime = System.currentTimeMillis();
		int counter = 0;
		for (Iterator<RunnableTuple> i = this.pendingQueueEntries(); i.hasNext();)
		{
			RunnableTuple tuple = i.next();

			if (tuple.future.isDone() || tuple.future.isCancelled())
				continue;

			QTXWorkConsumer workConsumer = (QTXWorkConsumer) tuple.runnable;

			if (workConsumer.workList != null)
			{
				// TODO need to check for following work package
				// TODO get number of linked packages and comp count to estimate
				// duration
				for (WorkPackage workPackage : workConsumer.workList)
				{
					if (workPackage.bom != null && workPackage.bom.alt_key_bom == bomKey)
					{
						RequalificationTradeLaneStatus tradeLaneStats = new RequalificationTradeLaneStatus();

						tradeLaneStats.qualtxKey = workPackage.qualtx.alt_key_qualtx;
						tradeLaneStats.requestTime = requestTime;

						tradeLaneStats.ftaCode = workPackage.qualtx.fta_code;
						tradeLaneStats.ivaCode = workPackage.qualtx.iva_code;
						tradeLaneStats.effectiveFrom = (workPackage.qualtx.effective_from != null) ? workPackage.qualtx.effective_from.getTime() : null;
						tradeLaneStats.effectiveTo = (workPackage.qualtx.effective_to != null) ? workPackage.qualtx.effective_to.getTime() : null;
						tradeLaneStats.coi = workPackage.qualtx.ctry_of_import;

						// Calculate estimate based on metrics
						tradeLaneStats.estimate = System.currentTimeMillis();

						double throughput = this.getThroughput(1); // returns throughput per millisecond

						logger.info(bomKey + " found at " + counter + " throughput " + throughput + " at " + tradeLaneStats.estimate);

						tradeLaneStats.position = counter;
						tradeLaneStats.duration = (long) ((double) tradeLaneStats.position / throughput);
						tradeLaneStats.estimate += tradeLaneStats.duration;

						bomStatus.putTradeLaneStatus(tradeLaneStats);
					}
				}
			}

			counter++;
		}
	}
}
