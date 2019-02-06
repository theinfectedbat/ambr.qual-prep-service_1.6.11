package com.ambr.gtm.fta.qts.workmgmt;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.ambr.gtm.fta.qps.bom.BOMUniverse;
import com.ambr.gtm.fta.qps.qualtx.engine.api.CacheRefreshInformation;
import com.ambr.gtm.fta.qps.qualtx.engine.api.GetCacheRefreshInformationClientAPI;
import com.ambr.gtm.fta.qts.QTXStage;
import com.ambr.gtm.fta.qts.QTXWork;
import com.ambr.gtm.fta.qts.QTXWorkRepository;
import com.ambr.gtm.fta.qts.QtxStatusUpdateRequest;
import com.ambr.gtm.fta.qts.TrackerCodes;
import com.ambr.gtm.fta.qts.WorkManagementException;
import com.ambr.gtm.fta.qts.TrackerCodes.QTXStageStatus;
import com.ambr.gtm.fta.qts.api.TrackerClientAPI;
import com.ambr.gtm.fta.qts.config.QEConfigCache;
import com.ambr.gtm.fta.qts.trade.MDIQualTxRepository;
import com.ambr.platform.rdbms.bootstrap.SchemaDescriptorService;
import com.ambr.platform.uoid.UniversalObjectIDGenerator;
import com.ambr.platform.utils.log.PerformanceTracker;

//TODO need to schedule this process to run at an exact time per company.  could be hourly, daily, or at midnight - review leave scheduling in TA for now, it can make rest call to requal to process ar_qtx_stage
//TODO remove the JSON structures and third party dependency  and replace with POJO
//TODO review getimpacted qualtx keys - can these be run in parallel and consolidated when complete.  then create ar_qtx_work items.
//TODO what is final user id for merged set of records.
//TODO what should time_stamp be for created ar_qtx_work records?  last time_stamp on ar_qtx_stage for merged set of records or "now"
public class QTXStageProducer extends QTXProducer
{
	private static final Logger logger = LogManager.getLogger(QTXStageProducer.class);
	
	private ArQtxWorkUtility utility;
	
	private GetCacheRefreshInformationClientAPI cacheRefreshInformationClientAPI;
//	private TrackerClientAPI trackerClientAPI;
	private  QEConfigCache		qeConfigCache;
	
	private long maxStageWork = 1000000L;
	private long maxWork = 1000000L;
	
	private long workProcessed = 0L;
	
	public QTXStageProducer(SchemaDescriptorService schemaService, PlatformTransactionManager txMgr, JdbcTemplate template)
	{
		super(schemaService, txMgr, template);
	}
	
	public void init(int threads, int readAhead, int fetchSize, int batchSize, int sleepInterval, long maxStageWork, long maxWork, MDIQualTxRepository qualTxRepository, QTXWorkRepository workRepository, UniversalObjectIDGenerator idGenerator, BOMUniverse bomUniverse, QEConfigCache qeConfigCache) throws WorkManagementException
	{
		super.init(threads, readAhead, fetchSize, batchSize, sleepInterval, qualTxRepository, workRepository, idGenerator);
		
		this.utility = new ArQtxWorkUtility(this.getWorkRepository(), this.template, bomUniverse, qeConfigCache);
		this.qeConfigCache = qeConfigCache;
		
		this.maxWork = maxWork;
		this.maxStageWork = maxStageWork;
	}
	
	public void setLimits(long maxWork, long maxStage)
	{
		this.maxWork = maxWork;
		this.maxStageWork = maxStage;
	}
	
	public void setAPI(GetCacheRefreshInformationClientAPI cacheRefreshInformationClientAPI, TrackerClientAPI trackerClientAPI)
	{
		this.cacheRefreshInformationClientAPI = cacheRefreshInformationClientAPI;
//		this.trackerClientAPI = trackerClientAPI;
	}
	
	public long getWorkProcessed()
	{
		return this.workProcessed;
	}

	public void executeFindWork() throws Exception
	{
		this.workProcessed = 0L;
		CacheRefreshInformation cacheInfo = this.cacheRefreshInformationClientAPI.execute();
		Timestamp bestTime = new Timestamp(cacheInfo.cacheLoadStart);
		
		logger.info("QTXStageProducer: Requalification loading work as of " + bestTime);
		
		this.template.query("select ar_qtx_stage.qtx_sid, ar_qtx_stage.user_id, ar_qtx_stage_data.data, ar_qtx_stage.time_stamp, ar_qtx_stage.priority from ar_qtx_stage left join ar_qtx_stage_data on ar_qtx_stage.qtx_sid=ar_qtx_stage_data.qtx_sid where ar_qtx_stage.time_stamp<=? and status=? order by ar_qtx_stage.time_stamp", new Object[]{bestTime, QTXStageStatus.INIT.ordinal()}, 
			new ResultSetExtractor<Object>() {
				public Object extractData(java.sql.ResultSet results) throws java.sql.SQLException
				{
					try
					{
						DataLoader<QTXStage> loader = new DataLoader<QTXStage>(QTXStage.class);
						List<QTXStage> processedStageList = new ArrayList<QTXStage>();
						StageWorkConverter stageWorkConverter = new StageWorkConverter(utility, qeConfigCache, template, batchSize);
											
						while (results.next() == true)
						{
							workProcessed++;
							
							QTXStage stageData = loader.getObjectFromResultSet(results);

							processedStageList.add(stageData);
							
							long totalSize = stageWorkConverter.generateWorkFromStagedData(stageData);
							
							//Set data to null so it can be garbage collected
							stageData.data = null;
							
							if (totalSize >= maxStageWork)
							{
								processStageWork(stageWorkConverter, processedStageList, bestTime);
								
								stageWorkConverter = new StageWorkConverter(utility, qeConfigCache, template, batchSize);
								processedStageList.clear();
							}
						}
						
						if (processedStageList.size() > 0)
						{
							processStageWork(stageWorkConverter, processedStageList, bestTime);
						}
					}
					catch (Exception e)
					{
						throw new java.sql.SQLException("Failed to process stage records", e);
					}
					
					return null;
				}
			});
	}
	
	private void processStageWork(StageWorkConverter stageWorkConverter, List<QTXStage> processedStageList, Timestamp bestTime) throws Exception
	{
		TransactionStatus status = txMgr.getTransaction(new DefaultTransactionDefinition());
		try
		{
			while (true)
			{
				Map<Long, QTXWork> consolidatedWork = stageWorkConverter.getConsolidatedWork(maxWork, bestTime);
				
				if (consolidatedWork == null || consolidatedWork.isEmpty()) break;

				getWorkRepository().storeWork(consolidatedWork.values());
				
				//TODO nagesh/sankar - is this call still necessary, part of the implementation is commented out
				updateTrackerStatus(consolidatedWork.values());
			}

			updateStageRecordsStatus(processedStageList, QTXStageStatus.COMPLETED);

			txMgr.commit(status);
		}
		catch (Exception e)
		{
			logger.error("Failed to expand work stage data - issuing rollback", e);
			
			txMgr.rollback(status);
			
			throw new java.sql.SQLException("Failed to expand work stage data - issuing rollback", e);
		}
	}
	
	@Override
	protected void findWork() throws Exception
	{
	}
	
	private int updateStageRecordsStatus(List<QTXStage> stageList, QTXStageStatus status) throws SQLException
	{
		String sql = "update ar_qtx_stage set status=?,time_stamp=? where qtx_sid=?"; //Double check the time-stamp updation
		int rowsAffected = 0;
		
		PerformanceTracker tracker = new PerformanceTracker(logger, Level.INFO, "updateStageRecordsStatus");
		tracker.start();
		try
		{
			int updateMatrix[][] = template.batchUpdate(sql, stageList, this.batchSize, new ParameterizedPreparedStatementSetter<QTXStage>() {
					private Timestamp now = new Timestamp(System.currentTimeMillis());
					
					@Override
					public void setValues(PreparedStatement ps, QTXStage argument) throws SQLException {
						ps.setInt(1, status.ordinal());
						ps.setTimestamp(2, this.now);
						ps.setLong(3, argument.qtx_sid);
					}
				  });
			
			for (int updateList[] : updateMatrix)
			{
				for (int updateCount : updateList)
					rowsAffected = rowsAffected + updateCount;
			}
		}
		finally
		{
			tracker.stop("ar_qtx_stage records affected = {0}" , new Object[]{rowsAffected});
		}
		
		return rowsAffected;
	}

	//TODO API needs to include searching by a specific org_code (or company_code)
	//TODO only allow one company code request to be run at a time, different company codes can run concurrently
//	private List<QTXStage> loadStageWork(long bestTime) throws SQLException
//	{
//		PerformanceTracker tracker = new PerformanceTracker(logger, Level.INFO, "loadStageWork");
//		List<QTXStage> stageList = null;
//		
//		tracker.start();
//		
//		try
//		{
//			SimpleDataLoaderResultSetExtractor<QTXStage> extractor = new SimpleDataLoaderResultSetExtractor<QTXStage>(QTXStage.class);
//
//			extractor.setMaxRows(this.readAhead);
//			
//			stageList = this.template.query("select ar_qtx_stage.qtx_sid, ar_qtx_stage.user_id, ar_qtx_stage_data.data, ar_qtx_stage.time_stamp, ar_qtx_stage.priority from ar_qtx_stage left join ar_qtx_stage_data on ar_qtx_stage.qtx_sid=ar_qtx_stage_data.qtx_sid where ar_qtx_stage.time_stamp<=? and status=? order by ar_qtx_stage.time_stamp", new Object[]{new java.sql.Timestamp(bestTime), QTXStageStatus.INIT.ordinal()}, extractor);
//			
//			logger.debug("QTXStageProducer records found = " + stageList.size());
//		}
//		finally
//		{
//			tracker.stop("QTXStageProducer records = {0}" , new Object[]{(stageList != null) ? stageList.size() : "ERROR"});
//		}
//		
//		return stageList;
//	}


	//TODO setup API call to send in one shot
	//TODO API should consider local vs remote configuration
	public void updateTrackerStatus(Collection<QTXWork> workList)
	{
		try
		{
			for (QTXWork work : workList)
			{
				QtxStatusUpdateRequest qtxStatusUpdateRequest = new QtxStatusUpdateRequest();
				qtxStatusUpdateRequest.setBOMKey(work.bom_key);
				qtxStatusUpdateRequest.setOrgCode(work.company_code);
				qtxStatusUpdateRequest.setQualtxKey(work.details.qualtx_key);
				qtxStatusUpdateRequest.setQualtxWorkId(work.qtx_wid);
				qtxStatusUpdateRequest.setStatus(TrackerCodes.QualtxStatus.INIT.ordinal());
				qtxStatusUpdateRequest.setUserId(work.userId);
				qtxStatusUpdateRequest.setIvaKey(work.iva_key);
		
			//	this.trackerClientAPI.updateQualtxStatus(qtxStatusUpdateRequest);
			}
		}
		catch (Exception e)
		{
			logger.error("Failed to notify tracker of created work", e);
		}
	}
}
