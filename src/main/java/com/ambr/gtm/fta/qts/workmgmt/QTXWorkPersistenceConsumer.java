package com.ambr.gtm.fta.qts.workmgmt;

import java.util.ArrayList;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.ambr.gtm.fta.qps.qualtx.engine.QualTX;
import com.ambr.gtm.fta.qps.util.QualTXUtility;
import com.ambr.gtm.fta.qts.QTXCompWork;
import com.ambr.gtm.fta.qts.QTXWork;
import com.ambr.gtm.fta.qts.QtxStatusUpdateRequest;
import com.ambr.gtm.fta.qts.TrackerCodes;
import com.ambr.gtm.fta.qts.util.Env;
import com.ambr.gtm.fta.trade.BOMQualtxData;
import com.ambr.gtm.fta.trade.client.LockException;
import com.ambr.gtm.fta.trade.client.TradeQualtxClient;
import com.ambr.gtm.fta.trade.model.BOMQualAuditEntity;

public class QTXWorkPersistenceConsumer extends QTXConsumer<WorkPackage>
{
	private static Logger logger = LogManager.getLogger(QTXWorkPersistenceConsumer.class);

	public QTXWorkPersistenceConsumer(ArrayList<WorkPackage> workList)
	{
		super(workList);
	}

	public QTXWorkPersistenceConsumer(WorkPackage workPackage)
	{
		super(workPackage);
	}

	public void doWork(WorkPackage workPackage) throws Exception
	{
		workPackage = workPackage.getRootPackage();

		QTXWork work = workPackage.work;
		QualTX qualtx = workPackage.qualtx;

		logger.debug("Persisting work item " + work.qtx_wid + " alt qualtx key " + ((qualtx != null) ? qualtx.alt_key_qualtx : "NOT_FOUND"));

		JdbcTemplate template = this.producer.getJdbcTemplate();
		PlatformTransactionManager txMgr = this.producer.getTransactionManager();
		TransactionStatus status = txMgr.getTransaction(new DefaultTransactionDefinition());
		TradeQualtxClient tradeQualtxClient = null;
		try
		{
			tradeQualtxClient = Env.getSingleton().getTradeQualtxClient();

			Exception failure = workPackage.getFailure();

			if (failure != null) throw failure;

			workPackage.entityMgr.save(work.userId, false);

			BOMQualAuditEntity audit = QualTXUtility.buildAudit(workPackage.qualtx.alt_key_qualtx, workPackage.work.company_code, workPackage.work.userId, workPackage.entityMgr,"TD");
			tradeQualtxClient.doRecordLevelAudit(audit);

			String aWorkId = null;
			boolean statusUpdated = false;
			if (workPackage.deleteBOMQual)
			{
				ArrayList<Long> qualtxKeyList = new ArrayList<Long>();
				ArrayList<Long> qtxWorkIdList = new ArrayList<Long>();

				qualtxKeyList.add(workPackage.qualtx.alt_key_qualtx);
				qtxWorkIdList.add(workPackage.work.qtx_wid);

				BOMQualtxData bomQualtxData = new BOMQualtxData();
				bomQualtxData.bomkey = workPackage.work.bom_key;
				bomQualtxData.qtxWorkIdList = qtxWorkIdList;
				bomQualtxData.qualtxKeyList = qualtxKeyList;
				bomQualtxData.priority = workPackage.bom.priority;
				bomQualtxData.action = "delete";
				aWorkId = Env.getSingleton().getTradeQualtxClient().createWorkForBOMQualUpdate(bomQualtxData);
				if (aWorkId != null)
				{
					this.updateWorkToSuccess(workPackage, template);
					statusUpdated = true;
				}
			}
			if (!statusUpdated) this.updateWorkToSuccess(workPackage, template);

			// if the current analysis method is Top-Down mark the
			// TD_CONSTRUCTION_STATUS as COMPLETE
			if (work.details.analysis_method == TrackerCodes.AnalysisMethod.TOP_DOWN_ANALYSIS) qualtx.td_construction_status = TrackerCodes.QualTXContructionStatus.COMPLETED.ordinal();

			//txMgr.commit(status); TA-83611

			// TODO review further. would be better to notify tracker when
			// entire work package chain is complete. even better to only post
			// when entire BOM is ready.
			try
			{
				QtxStatusUpdateRequest request = new QtxStatusUpdateRequest();

				request.setQualtxKey(work.details.qualtx_key);
				request.setQualtxWorkId(work.qtx_wid);
				request.setBOMKey(work.bom_key);
				if(workPackage.isReadyForQualification)
					request.setStatus(TrackerCodes.QualtxStatus.READY_FOR_QUALIFICATION.ordinal());
				else
				{
					request.setStatus(TrackerCodes.QualtxStatus.QUALIFICATION_NOT_REQUIRED.ordinal());
					request.setWaitForNextAnalysisMethodFlg(true);
				}
				Env.getSingleton().getTrackerAPI().updateQualtxStatus(request);
			}
			catch (Exception e)
			{
				logger.error("Error notifying tracker of status update for work " + work.qtx_wid, e);

				TransactionStatus loggerStatus = txMgr.getTransaction(new DefaultTransactionDefinition());
				try
				{
					producer.getWorkRepository().logError(work.qtx_wid, e);

					//txMgr.commit(loggerStatus);
				}
				catch (Exception logException)
				{
					logger.error("Exception encountered storing exception during tracker status update " + work.qtx_wid, logException);
					/*try
					{
						txMgr.rollback(loggerStatus);
					}
					catch (Exception rollbackException)
					{
						logger.error("Exception encountered during rollback of exception logging for tracker status update " + work.qtx_wid, rollbackException);
					}*/
				}
			}
			
			txMgr.commit(status); //TA-83611
			
		}
		catch (Exception e)
		{
			logger.error("Exception encountered processing or persisting work " + work.qtx_wid, e);

			txMgr.rollback(status);

			TransactionStatus rollbackStatus = txMgr.getTransaction(new DefaultTransactionDefinition());
			try
			{
				boolean retry = ExceptionUtils.hasCause(e, LockException.class);

				producer.getWorkRepository().logError(work.qtx_wid, e);

				this.updateWorkToFailure(workPackage, retry, template);

				txMgr.commit(rollbackStatus);
				if(!retry)
				{
					try
					{
						QtxStatusUpdateRequest request = new QtxStatusUpdateRequest();

						request.setQualtxKey(work.details.qualtx_key);
						request.setQualtxWorkId(work.qtx_wid);
						request.setBOMKey(work.bom_key);
						request.setStatus(TrackerCodes.QualtxStatus.QUALTX_PREP_FAILED.ordinal());

						Env.getSingleton().getTrackerAPI().updateQualtxStatus(request);
					}
					catch (Exception exception)
					{
						logger.error("Error notifying tracker of status update for work " + work.qtx_wid, exception);

						TransactionStatus loggerStatus = txMgr.getTransaction(new DefaultTransactionDefinition());
						try
						{
							producer.getWorkRepository().logError(work.qtx_wid, exception);

							txMgr.commit(loggerStatus);
						}
						catch (Exception logException)
						{
							logger.error("Exception encountered storing exception during tracker status update " + work.qtx_wid, logException);
							try
							{
								txMgr.rollback(loggerStatus);
							}
							catch (Exception rollbackException)
							{
								logger.error("Exception encountered during rollback of exception logging for tracker status update " + work.qtx_wid, rollbackException);
							}
						}
					}
				}
			}
			catch (Exception inner)
			{
				logger.error("Failed during error handler", inner);

				txMgr.rollback(rollbackStatus);
			}

			throw e;
		}
		finally
		{
			Long lockId = workPackage.getLockId();
			if (lockId != null)
			{
				if(tradeQualtxClient ==null) tradeQualtxClient = Env.getSingleton().getTradeQualtxClient();
				tradeQualtxClient.releaseLock(lockId);
			}
		}
	}

	private void updateWorkToFailure(WorkPackage workPackage, boolean retry, JdbcTemplate template) throws Exception
	{
		WorkPackage next = workPackage;

		while (next != null)
		{
			ArrayList<QTXWork> workList = new ArrayList<QTXWork>();
			workList.add(next.work);

			producer.getWorkRepository().updateWorkStatus(workList, (retry) ? TrackerCodes.QualtxStatus.RETRY : TrackerCodes.QualtxStatus.QUALTX_PREP_FAILED);

			producer.getWorkRepository().updateWorkHSStatus(next.work.workHSList, (retry) ? TrackerCodes.QualtxHSPullStatus.RETRY : TrackerCodes.QualtxHSPullStatus.ERROR);

			producer.getWorkRepository().updateCompWorkStatus(next.work.compWorkList, (retry) ? TrackerCodes.QualtxCompStatus.RETRY : TrackerCodes.QualtxCompStatus.ERROR);

			for (QTXCompWork compWork : next.work.compWorkList)
			{
				producer.getWorkRepository().updateCompWorkHSStatus(compWork.compWorkHSList, (retry) ? TrackerCodes.QualtxCompHSPullStatus.RETRY : TrackerCodes.QualtxCompHSPullStatus.ERROR);
				producer.getWorkRepository().updateCompWorkIVAStatus(compWork.compWorkIVAList, (retry) ? TrackerCodes.QualtxCompIVAPullStatus.RETRY : TrackerCodes.QualtxCompIVAPullStatus.ERROR);
			}

			next = next.getLinkedPackage();
		}
	}

	private void updateWorkToSuccess(WorkPackage workPackage, JdbcTemplate template) throws Exception
	{
		WorkPackage next = workPackage;
		while (next != null)
		{
			ArrayList<QTXWork> workList = new ArrayList<QTXWork>();
			workList.add(next.work);
			
			if(workPackage.isReadyForQualification)
				producer.getWorkRepository().updateWorkStatus(workList, TrackerCodes.QualtxStatus.READY_FOR_QUALIFICATION);
			else
				producer.getWorkRepository().updateWorkStatus(workList, TrackerCodes.QualtxStatus.QUALIFICATION_NOT_REQUIRED);

			producer.getWorkRepository().updateWorkHSStatus(next.work.workHSList, TrackerCodes.QualtxHSPullStatus.COMPLETED);

			producer.getWorkRepository().updateCompWorkStatus(next.work.compWorkList, TrackerCodes.QualtxCompStatus.COMPLETED);

			for (QTXCompWork compWork : next.work.compWorkList)
			{
				producer.getWorkRepository().updateCompWorkHSStatus(compWork.compWorkHSList, TrackerCodes.QualtxCompHSPullStatus.COMPLETED);
				producer.getWorkRepository().updateCompWorkIVAStatus(compWork.compWorkIVAList, TrackerCodes.QualtxCompIVAPullStatus.COMPLETED);
			}

			next = next.getLinkedPackage();
		}
	}

	@Override
	protected void processWork() throws Exception
	{
		for (WorkPackage workPackage : this.workList)
		{
			try
			{
				this.doWork(workPackage);
			}
			catch (Exception e)
			{
				logger.error("Error encountered handling persistence of " + workPackage.work.qtx_wid, e);
			}
		}
	}
}
