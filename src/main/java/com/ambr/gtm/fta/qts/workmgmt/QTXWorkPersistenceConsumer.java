package com.ambr.gtm.fta.qts.workmgmt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.ambr.gtm.fta.qps.qualtx.engine.QualTX;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponent;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponentDataExtension;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponentPrice;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXPrice;
import com.ambr.gtm.fta.qts.QTXCompWork;
import com.ambr.gtm.fta.qts.QTXWork;
import com.ambr.gtm.fta.qts.QtxStatusUpdateRequest;
import com.ambr.gtm.fta.qts.TrackerCodes;
import com.ambr.gtm.fta.qts.trade.MDIBomRepository;
import com.ambr.gtm.fta.qts.trade.MDIQualTx;
import com.ambr.gtm.fta.qts.util.Env;
import com.ambr.gtm.fta.trade.client.LockException;
import com.ambr.gtm.fta.trade.client.TradeQualtxClient;
import com.ambr.gtm.fta.trade.model.BOMQualAuditEntity;
import com.ambr.gtm.fta.trade.model.BOMQualAuditEntity.STATE;
import com.ambr.platform.rdbms.orm.DataRecordColumnModification;
import com.ambr.platform.rdbms.orm.DataRecordModificationTracker;
import com.ambr.platform.rdbms.orm.EntityManager;

public class QTXWorkPersistenceConsumer extends QTXConsumer<WorkPackage>
{
	private static Logger logger = LogManager.getLogger(QTXWorkPersistenceConsumer.class);

	public QTXWorkPersistenceConsumer(ArrayList <WorkPackage> workList)
	{
		super(workList);
	}
	
	public QTXWorkPersistenceConsumer(WorkPackage workPackage)
	{
		super(workPackage);
	}
	
	//TODO time_stamp, how to handle updating status columns.  should all records for all work items under one ar_qtx_work get the same time_stamp (yes)
	//TODO what about time_stamp on ar_qtx_work/details - these are never updated, could be kept in sync with ar_qtx_work_status record but that is 1-2 more update statements to execute.
	public void doWork(WorkPackage workPackage) throws Exception
	{
		workPackage = workPackage.getRootPackage();
		
		QTXWork work = workPackage.work;
		QualTX qualtx = workPackage.qualtx;
		
		//TODO review this further - should not be in a state where a qualtx is not loaded.  this should be caught in staging and set this work type to error
		logger.debug("Persisting work item " + work.qtx_wid + " alt qualtx key " + ((qualtx != null) ? qualtx.alt_key_qualtx : "NOT_FOUND"));
		
		JdbcTemplate template = this.producer.getJdbcTemplate();
		PlatformTransactionManager txMgr = this.producer.getTransactionManager();
		TransactionStatus status = txMgr.getTransaction(new DefaultTransactionDefinition());
		try
		{
			TradeQualtxClient tradeQualtxClient = Env.getSingleton().getTradeQualtxClient();

			Exception failure = workPackage.getFailure();
			
			if (failure != null)
				throw failure;
			
			workPackage.entityMgr.save(work.userId, false);
			
			
			//Iterate through all linked packages and compwork packages to get full setup qualtxcomponent records modified
			WorkPackage currentWorkPackage = workPackage;
			HashMap<Long, EntityManager<QualTXComponent>> qualtxCompEntityMap = new HashMap<Long, EntityManager<QualTXComponent>>();
			while (currentWorkPackage != null)
			{
				for (CompWorkPackage compWorkPackage : workPackage.compWorks.values())
				{
					qualtxCompEntityMap.put(compWorkPackage.qualtxComp.alt_key_comp, compWorkPackage.entityMgr);
				}
				
				currentWorkPackage = currentWorkPackage.getLinkedPackage();
			}
	/*		
			for (EntityManager<QualTXComponent> qualtxCompEntityMgr : qualtxCompEntityMap.values())
			{
				qualtxCompEntityMgr.save(work.userId, false);
			}
   */
			//TODO create a new Timestamp and use for all records in a modified/created state
			//TODO make sure parent records are updated accordingly in the case where only the child is modified
			//TODO set on all modified pojo classes
			//TODO placeholder to make Audit API call (convert workPackage.entityMgr.get... to BOMQualAuditEntity record
			 BOMQualAuditEntity audit = this.buildAudit(workPackage,qualtxCompEntityMap.values());
			 tradeQualtxClient.doRecordLevelAudit(audit);
			
			if (workPackage.deleteBOMQual)
			{
				ArrayList<Long> list = new ArrayList<Long>();
				
				list.add(workPackage.qualtx.alt_key_qualtx);
				
				MDIBomRepository.deleteBomQualRecords(work.bom_key, list, workPackage.qualtx.org_code, (work.userId == null)? workPackage.qualtx.last_modified_by : work.userId, template);
			}
			
			this.updateWorkToSuccess(workPackage, template);
			
			if (workPackage.lockId != null)
				tradeQualtxClient.releaseLock(workPackage.lockId);

			//if the current analysis method is Top-Down mark the TD_CONSTRUCTION_STATUS as COMPLETE
			if(work.details.analysis_method == TrackerCodes.AnalysisMethod.TOP_DOWN_ANALYSIS) 
				qualtx.td_construction_status = TrackerCodes.QualTXContructionStatus.COMPLETED.ordinal();
				
			txMgr.commit(status);
			
			//TODO review further.  would be better to notify tracker when entire work package chain is complete.  even better to only post when entire BOM is ready.
			try
			{
				QtxStatusUpdateRequest request = new QtxStatusUpdateRequest();
				
				request.setQualtxKey(work.details.qualtx_key);
				request.setQualtxWorkId(work.qtx_wid);
				request.setBOMKey(work.bom_key);
				request.setStatus(TrackerCodes.QualtxStatus.READY_FOR_QUALIFICATION.ordinal());
				
			//	Env.getSingleton().getTrackerAPI().updateQualtxStatus(request);
			}
			catch (Exception e)
			{
				logger.error("Error notifying tracker of status update for work " + work.qtx_wid, e);
				
				TransactionStatus loggerStatus = txMgr.getTransaction(new DefaultTransactionDefinition());
				try
				{
					producer.getWorkRepository().logError(work.qtx_wid, e);
					
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
		catch (Exception e)
		{
			logger.error("Exception encountered processing or persisting work " + work.qtx_wid, e);
			
			txMgr.rollback(status);

			TransactionStatus rollbackStatus = txMgr.getTransaction(new DefaultTransactionDefinition());
			try
			{
				boolean retry = ExceptionUtils.hasCause(e, LockException.class);
				
				//TODO go through entire structure and log all errors (work and compwork items)
				producer.getWorkRepository().logError(work.qtx_wid, e);
				
				this.updateWorkToFailure(workPackage, retry, template);

				txMgr.commit(rollbackStatus);
				
				try
				{
					QtxStatusUpdateRequest request = new QtxStatusUpdateRequest();
					
					request.setQualtxKey(work.details.qualtx_key);
					request.setQualtxWorkId(work.qtx_wid);
					request.setBOMKey(work.bom_key);
					request.setStatus(TrackerCodes.QualtxStatus.QUALTX_PREP_FAILED.ordinal());
					
				//	Env.getSingleton().getTrackerAPI().updateQualtxStatus(request);
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
			catch (Exception inner)
			{
				logger.error("Failed during error handler", inner);

				txMgr.rollback(rollbackStatus);
			}
			
			throw e;
		}
	}
	
	private void updateWorkToFailure(WorkPackage workPackage, boolean retry,JdbcTemplate template) throws Exception
	{
		WorkPackage next = workPackage;
		
		while (next != null)
		{
			ArrayList<QTXWork> workList = new ArrayList<QTXWork>();
			workList.add(next.work);

			producer.getWorkRepository().updateWorkStatus(workList, (retry) ? TrackerCodes.QualtxStatus.INIT : TrackerCodes.QualtxStatus.QUALTX_PREP_FAILED);
	
			//TODO wrap all of the common table updates into batch update statements
			producer.getWorkRepository().updateWorkHSStatus(next.work.workHSList, (retry) ? TrackerCodes.QualtxHSPullStatus.INIT : TrackerCodes.QualtxHSPullStatus.ERROR);
			
			producer.getWorkRepository().updateCompWorkStatus(next.work.compWorkList, (retry) ? TrackerCodes.QualtxCompStatus.INIT : TrackerCodes.QualtxCompStatus.ERROR);
				
			for (QTXCompWork compWork : next.work.compWorkList)
			{
				producer.getWorkRepository().updateCompWorkHSStatus(compWork.compWorkHSList, (retry) ? TrackerCodes.QualtxCompHSPullStatus.INIT : TrackerCodes.QualtxCompHSPullStatus.ERROR);
				producer.getWorkRepository().updateCompWorkIVAStatus(compWork.compWorkIVAList, (retry) ? TrackerCodes.QualtxCompIVAPullStatus.INIT : TrackerCodes.QualtxCompIVAPullStatus.ERROR);
			}
			
			next = next.getLinkedPackage();
		}
	}
	
	private void updateWorkToSuccess(WorkPackage workPackage, JdbcTemplate template) throws Exception
	{
		//TODO wrap all of the common table updates into batch update statements
		WorkPackage next = workPackage;
		while (next != null)
		{
			ArrayList<QTXWork> workList = new ArrayList<QTXWork>();
			workList.add(next.work);

			producer.getWorkRepository().updateWorkStatus(workList, TrackerCodes.QualtxStatus.READY_FOR_QUALIFICATION);

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
	
	private BOMQualAuditEntity buildAudit(WorkPackage workPackage, Collection<EntityManager<QualTXComponent>> qualtxCompEntityList) throws Exception
	{
		BOMQualAuditEntity audit = new BOMQualAuditEntity(MDIQualTx.TABLE_NAME, workPackage.qualtx.alt_key_qualtx);
		audit.setSurrogateKeyColumn("ALT_KEY_QUALTX");
		audit.setOrgCode(workPackage.work.company_code);
		audit.setUserID(workPackage.work.userId);
		audit.setState(STATE.MODIFY);;
		HashMap<Long, BOMQualAuditEntity> auditMap = new HashMap<Long, BOMQualAuditEntity>();
		
		for (DataRecordModificationTracker<?> deletedRecord : workPackage.entityMgr.getTracker().getDeletedRecords())
		{
			if (deletedRecord.modifiableRecord instanceof QualTXPrice)
			{
				Long altkey = ((QualTXPrice) deletedRecord.modifiableRecord).alt_key_price;
				BOMQualAuditEntity priceAudit = auditMap.get(altkey);
				if(priceAudit == null) priceAudit = new BOMQualAuditEntity("MDI_QUALTX_PRICE", altkey);
				priceAudit.setSurrogateKeyColumn("ALT_KEY_PRICE");
				priceAudit.setState(STATE.DELETE);
				auditMap.put(altkey, priceAudit);
				audit.addChildTable(priceAudit);
			}
			else if (deletedRecord.modifiableRecord instanceof QualTXComponent)
			{
				Long altkey = ((QualTXComponent) deletedRecord.modifiableRecord).alt_key_comp;
				BOMQualAuditEntity compAudit = auditMap.get(altkey);
				if(compAudit == null) compAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP", altkey);
				compAudit.setSurrogateKeyColumn("ALT_KEY_COMP");
				compAudit.setState(STATE.DELETE);
				auditMap.put(altkey, compAudit);
				audit.addChildTable(compAudit);
			}
			
			else if (deletedRecord.modifiableRecord instanceof QualTXComponentPrice)
			{
				Long altkey = ((QualTXComponentPrice) deletedRecord.modifiableRecord).alt_key_price;
				BOMQualAuditEntity compPriceAudit = auditMap.get(altkey);
				if(compPriceAudit == null) compPriceAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP_PRICE", altkey);
				compPriceAudit.setSurrogateKeyColumn("ALT_KEY_PRICE");
				compPriceAudit.setState(STATE.DELETE);
				auditMap.put(altkey, compPriceAudit);
				audit.addChildTable(compPriceAudit);
			}

			else if (deletedRecord.modifiableRecord instanceof QualTXComponentDataExtension)
			{
				Long altkey = ((QualTXComponentDataExtension) deletedRecord.modifiableRecord).seq_num;
				BOMQualAuditEntity compDeAudit = auditMap.get(altkey);
				if(compDeAudit == null) compDeAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP", altkey, "IMPL_BOM_PROD_FAMILY:TEXTILES" , STATE.DELETE);
				compDeAudit.setSurrogateKeyColumn("SEQ_NUM");
				auditMap.put(altkey, compDeAudit);
				audit.addChildTable(compDeAudit);
			}
			
		}
		
		for (DataRecordModificationTracker<?> modRecord : workPackage.entityMgr.getTracker().getModifiedRecords())
		{
			if (modRecord.modifiableRecord instanceof QualTX)
			{
				for (DataRecordColumnModification columnMod : modRecord.getColumnModifications())
				{
					audit.setModifiedColumn(columnMod.getColumnName(), columnMod.getModifiedValue(), columnMod.getOriginalValue());
				}
				auditMap.put(workPackage.qualtx.alt_key_qualtx, audit);
			}
			
			else if (modRecord.modifiableRecord instanceof QualTXPrice)
			{
				Long altkey = ((QualTXPrice) modRecord.modifiableRecord).alt_key_price;
				BOMQualAuditEntity priceAudit = auditMap.get(altkey);
				if(priceAudit == null) priceAudit = new BOMQualAuditEntity("MDI_QUALTX_PRICE", altkey);
				for (DataRecordColumnModification columnMod : modRecord.getColumnModifications())
				{
					priceAudit.setModifiedColumn(columnMod.getColumnName(), columnMod.getModifiedValue(), columnMod.getOriginalValue());
				}
				priceAudit.setSurrogateKeyColumn("ALT_KEY_PRICE");
				auditMap.put(altkey, priceAudit);
				audit.addChildTable(priceAudit);
			}
			else if (modRecord.modifiableRecord instanceof QualTXComponent)
			{
				Long altkey = ((QualTXComponent) modRecord.modifiableRecord).alt_key_comp;
				BOMQualAuditEntity compAudit = auditMap.get(altkey);
				if(compAudit == null) compAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP", altkey);
				compAudit.setSurrogateKeyColumn("ALT_KEY_COMP");
				compAudit.setState(STATE.MODIFY);
				for (DataRecordColumnModification columnMod : modRecord.getColumnModifications())
				{
					compAudit.setModifiedColumn(columnMod.getColumnName(), columnMod.getModifiedValue(), columnMod.getOriginalValue());
				}
				auditMap.put(altkey, compAudit);
				audit.addChildTable(compAudit);
			}
			
			else if (modRecord.modifiableRecord instanceof QualTXComponentPrice)
			{
				Long altkey = ((QualTXComponentPrice) modRecord.modifiableRecord).alt_key_price;
				BOMQualAuditEntity compPriceAudit = auditMap.get(altkey);
				if(compPriceAudit == null) compPriceAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP_PRICE", altkey);
				compPriceAudit.setSurrogateKeyColumn("ALT_KEY_PRICE");
				compPriceAudit.setState(STATE.MODIFY);
				for (DataRecordColumnModification columnMod : modRecord.getColumnModifications())
				{
					compPriceAudit.setModifiedColumn(columnMod.getColumnName(), columnMod.getModifiedValue(), columnMod.getOriginalValue());
				}
				auditMap.put(altkey, compPriceAudit);
				audit.addChildTable(compPriceAudit);
			}

			else if (modRecord.modifiableRecord instanceof QualTXComponentDataExtension)
			{
				Long altkey = ((QualTXComponentDataExtension) modRecord.modifiableRecord).seq_num;
				BOMQualAuditEntity compDeAudit = auditMap.get(altkey);
				if(compDeAudit == null) compDeAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP", altkey, "IMPL_BOM_PROD_FAMILY:TEXTILES" , STATE.MODIFY);
				compDeAudit.setSurrogateKeyColumn("SEQ_NUM");
				for (DataRecordColumnModification columnMod : modRecord.getColumnModifications())
				{
					compDeAudit.setModifiedColumn(columnMod.getColumnName(), columnMod.getModifiedValue(), columnMod.getOriginalValue());
				}
				auditMap.put(altkey, compDeAudit);
				audit.addChildTable(compDeAudit);
			}
			
		}
		
		for (DataRecordModificationTracker<?> newRecord : workPackage.entityMgr.getTracker().getNewRecords())
		{
			
			if (newRecord.modifiableRecord instanceof QualTXPrice)
			{
				QualTXPrice qualTXPrice = (QualTXPrice) newRecord.modifiableRecord;
				Long altkey = qualTXPrice.alt_key_price;
				BOMQualAuditEntity priceAudit = auditMap.get(altkey); 
				if(priceAudit == null) priceAudit = new BOMQualAuditEntity("MDI_QUALTX_PRICE", altkey);
				priceAudit.setState(STATE.CREATE);
				priceAudit.setSurrogateKeyColumn("ALT_KEY_PRICE");
				priceAudit.setModifiedColumn("price_type", qualTXPrice.price_type, null);
				priceAudit.setModifiedColumn("price", qualTXPrice.price, null);
				priceAudit.setModifiedColumn("currency_code", qualTXPrice.currency_code, null);
				auditMap.put(altkey, priceAudit);
				audit.addChildTable(priceAudit);
			}
			else if (newRecord.modifiableRecord instanceof QualTXComponent)
			{
				QualTXComponent qualTXComponent = (QualTXComponent) newRecord.modifiableRecord;
				Long altkey = qualTXComponent.alt_key_comp;
				BOMQualAuditEntity compAudit = auditMap.get(altkey);
				if(compAudit == null) compAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP", altkey);
				compAudit.setSurrogateKeyColumn("ALT_KEY_COMP");
				compAudit.setState(STATE.CREATE);
				compAudit.setModifiedColumn("area", qualTXComponent.area, null);
				compAudit.setModifiedColumn("area_uom", qualTXComponent.area_uom, null);
				compAudit.setModifiedColumn("component_type", qualTXComponent.component_type, null);
				compAudit.setModifiedColumn("cost", qualTXComponent.cost, null);
				compAudit.setModifiedColumn("description", qualTXComponent.description, null);
				compAudit.setModifiedColumn("essential_character", qualTXComponent.essential_character, null);
				compAudit.setModifiedColumn("gross_weight", qualTXComponent.gross_weight, null);
				compAudit.setModifiedColumn("weight_uom", qualTXComponent.weight_uom, null);
				compAudit.setModifiedColumn("prod_key", qualTXComponent.prod_key, null);
				compAudit.setModifiedColumn("prod_src_key", qualTXComponent.prod_src_key, null);
				compAudit.setModifiedColumn("manufacturer_key", qualTXComponent.manufacturer_key, null);
				compAudit.setModifiedColumn("seller_key", qualTXComponent.seller_key, null);
				compAudit.setModifiedColumn("net_weight", qualTXComponent.net_weight, null);
				compAudit.setModifiedColumn("unit_weight", qualTXComponent.unit_weight, null);
				compAudit.setModifiedColumn("qty_per", qualTXComponent.qty_per, null);
				compAudit.setModifiedColumn("unit_cost", qualTXComponent.unit_cost, null);
				auditMap.put(altkey, compAudit);
				audit.addChildTable(compAudit);
			}
			
			else if (newRecord.modifiableRecord instanceof QualTXComponentPrice)
			{
				QualTXComponentPrice qualTXCompPrice = (QualTXComponentPrice) newRecord.modifiableRecord;
				Long altkey = qualTXCompPrice.alt_key_price;
				BOMQualAuditEntity compPriceAudit = auditMap.get(altkey);
				if(compPriceAudit == null) compPriceAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP_PRICE", altkey);
				compPriceAudit.setSurrogateKeyColumn("ALT_KEY_PRICE");
				compPriceAudit.setModifiedColumn("price_type", qualTXCompPrice.price_type, null);
				compPriceAudit.setModifiedColumn("price", qualTXCompPrice.price, null);
				compPriceAudit.setModifiedColumn("currency_code", qualTXCompPrice.currency_code, null);
				compPriceAudit.setState(STATE.CREATE);
				auditMap.put(altkey, compPriceAudit);
				audit.addChildTable(compPriceAudit);
			}

			else if (newRecord.modifiableRecord instanceof QualTXComponentDataExtension)
			{
				QualTXComponentDataExtension qualTXCompDe = (QualTXComponentDataExtension) newRecord.modifiableRecord;
				Long altkey = qualTXCompDe.seq_num;
				BOMQualAuditEntity compDeAudit = auditMap.get(altkey);
				if(compDeAudit == null) compDeAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP", altkey, "IMPL_BOM_PROD_FAMILY:TEXTILES" , STATE.CREATE);
				compDeAudit.setSurrogateKeyColumn("SEQ_NUM");
				compDeAudit.setModifiedColumn("FLEXFIELD_VAR1", qualTXCompDe.getValue("FLEXFIELD_VAR1"), null);
				compDeAudit.setModifiedColumn("FLEXFIELD_VAR2", qualTXCompDe.getValue("FLEXFIELD_VAR2"), null);
				compDeAudit.setModifiedColumn("FLEXFIELD_VAR3", qualTXCompDe.getValue("FLEXFIELD_VAR3"), null);
				compDeAudit.setModifiedColumn("FLEXFIELD_VAR4", qualTXCompDe.getValue("FLEXFIELD_VAR4"), null);
				compDeAudit.setModifiedColumn("FLEXFIELD_VAR5", qualTXCompDe.getValue("FLEXFIELD_VAR5"), null);
				compDeAudit.setModifiedColumn("FLEXFIELD_VAR6", qualTXCompDe.getValue("FLEXFIELD_VAR6"), null);
				compDeAudit.setModifiedColumn("FLEXFIELD_VAR7", qualTXCompDe.getValue("FLEXFIELD_VAR7"), null);
				compDeAudit.setModifiedColumn("FLEXFIELD_NUM1", qualTXCompDe.getValue("FLEXFIELD_NUM1"), null);
				auditMap.put(altkey, compDeAudit);
				audit.addChildTable(compDeAudit);
			}
			
		}
		
		for (EntityManager<QualTXComponent> qualtxCompEntityMgr : qualtxCompEntityList)
		{
			if(null == qualtxCompEntityMgr) continue;
			for (DataRecordModificationTracker<?> deletedRecord :  qualtxCompEntityMgr.getTracker().getDeletedRecords())
			{
				if (deletedRecord.modifiableRecord instanceof QualTXComponent)
				{
					Long altkey = ((QualTXComponent) deletedRecord.modifiableRecord).alt_key_comp;
					BOMQualAuditEntity compAudit = auditMap.get(altkey);
					if(compAudit == null) compAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP", altkey);
					compAudit.setSurrogateKeyColumn("ALT_KEY_COMP");
					compAudit.setState(STATE.DELETE);
					auditMap.put(altkey, compAudit);
					audit.addChildTable(compAudit);
				}
				else if (deletedRecord.modifiableRecord instanceof QualTXComponentPrice)
				{
					Long altkey = ((QualTXComponentPrice) deletedRecord.modifiableRecord).alt_key_price;
					BOMQualAuditEntity compPriceAudit = auditMap.get(altkey);
					if(compPriceAudit == null) compPriceAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP_PRICE", altkey);
					compPriceAudit.setSurrogateKeyColumn("ALT_KEY_PRICE");
					compPriceAudit.setState(STATE.DELETE);
					auditMap.put(altkey, compPriceAudit);
					audit.addChildTable(compPriceAudit);
				}
				else if (deletedRecord.modifiableRecord instanceof QualTXComponentDataExtension)
				{
					Long altkey = ((QualTXComponentDataExtension) deletedRecord.modifiableRecord).seq_num;
					BOMQualAuditEntity compDeAudit = auditMap.get(altkey);
					if(compDeAudit == null) compDeAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP", altkey, "IMPL_BOM_PROD_FAMILY:TEXTILES" , STATE.DELETE);
					compDeAudit.setSurrogateKeyColumn("SEQ_NUM");
					auditMap.put(altkey, compDeAudit);
					audit.addChildTable(compDeAudit);
				}
			}
			
			for (DataRecordModificationTracker<?> newRecord :  qualtxCompEntityMgr.getTracker().getNewRecords())
			{
				if (newRecord.modifiableRecord instanceof QualTXComponent)
				{
					Long altkey = ((QualTXComponent) newRecord.modifiableRecord).alt_key_comp;
					BOMQualAuditEntity compAudit = auditMap.get(altkey);
					if(compAudit == null) compAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP", altkey);
					compAudit.setSurrogateKeyColumn("ALT_KEY_COMP");
					compAudit.setState(STATE.CREATE);
					for (DataRecordColumnModification columnMod : newRecord.getColumnModifications())
					{
						compAudit.setModifiedColumn(columnMod.getColumnName(), columnMod.getModifiedValue(), columnMod.getOriginalValue());
					}
					auditMap.put(altkey, compAudit);
					audit.addChildTable(compAudit);
				}
				
				else if (newRecord.modifiableRecord instanceof QualTXComponentPrice)
				{
					Long altkey = ((QualTXComponentPrice) newRecord.modifiableRecord).alt_key_price;
					BOMQualAuditEntity compPriceAudit = auditMap.get(altkey);
					if(compPriceAudit == null) compPriceAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP_PRICE", altkey);
					compPriceAudit.setSurrogateKeyColumn("ALT_KEY_PRICE");
					compPriceAudit.setState(STATE.CREATE);
					for (DataRecordColumnModification columnMod : newRecord.getColumnModifications())
					{
						compPriceAudit.setModifiedColumn(columnMod.getColumnName(), columnMod.getModifiedValue(), columnMod.getOriginalValue());
					}
					auditMap.put(altkey, compPriceAudit);
					audit.addChildTable(compPriceAudit);
				}

				else if (newRecord.modifiableRecord instanceof QualTXComponentDataExtension)
				{
					Long altkey = ((QualTXComponentDataExtension) newRecord.modifiableRecord).seq_num;
					BOMQualAuditEntity compDeAudit = auditMap.get(altkey);
					if(compDeAudit == null) compDeAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP", altkey, "IMPL_BOM_PROD_FAMILY:TEXTILES" , STATE.CREATE);
					compDeAudit.setSurrogateKeyColumn("SEQ_NUM");
					for (DataRecordColumnModification columnMod : newRecord.getColumnModifications())
					{
						compDeAudit.setModifiedColumn(columnMod.getColumnName(), columnMod.getModifiedValue(), columnMod.getOriginalValue());
					}
					auditMap.put(altkey, compDeAudit);
					audit.addChildTable(compDeAudit);
				}
			}
			
			for (DataRecordModificationTracker<?> modRecord :  qualtxCompEntityMgr.getTracker().getModifiedRecords())
			{
				if (modRecord.modifiableRecord instanceof QualTXComponent)
				{
					Long altkey = ((QualTXComponent) modRecord.modifiableRecord).alt_key_comp;
					BOMQualAuditEntity compAudit = auditMap.get(altkey);
					if(compAudit == null) compAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP", altkey);
					compAudit.setSurrogateKeyColumn("ALT_KEY_COMP");
					compAudit.setState(STATE.MODIFY);
					for (DataRecordColumnModification columnMod : modRecord.getColumnModifications())
					{
						compAudit.setModifiedColumn(columnMod.getColumnName(), columnMod.getModifiedValue(), columnMod.getOriginalValue());
					}
					auditMap.put(altkey, compAudit);
					audit.addChildTable(compAudit);
				}
				
				else if (modRecord.modifiableRecord instanceof QualTXComponentPrice)
				{
					Long altkey = ((QualTXComponentPrice) modRecord.modifiableRecord).alt_key_price;
					BOMQualAuditEntity compPriceAudit = auditMap.get(altkey);
					if(compPriceAudit == null) compPriceAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP_PRICE", altkey);
					compPriceAudit.setSurrogateKeyColumn("ALT_KEY_PRICE");
					compPriceAudit.setState(STATE.MODIFY);
					for (DataRecordColumnModification columnMod : modRecord.getColumnModifications())
					{
						compPriceAudit.setModifiedColumn(columnMod.getColumnName(), columnMod.getModifiedValue(), columnMod.getOriginalValue());
					}
					auditMap.put(altkey, compPriceAudit);
					audit.addChildTable(compPriceAudit);
				}

				else if (modRecord.modifiableRecord instanceof QualTXComponentDataExtension)
				{
					Long altkey = ((QualTXComponentDataExtension) modRecord.modifiableRecord).seq_num;
					BOMQualAuditEntity compDeAudit = auditMap.get(altkey);
					if(compDeAudit == null) compDeAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP", altkey, "IMPL_BOM_PROD_FAMILY:TEXTILES" , STATE.MODIFY);
					compDeAudit.setSurrogateKeyColumn("SEQ_NUM");
					for (DataRecordColumnModification columnMod : modRecord.getColumnModifications())
					{
						compDeAudit.setModifiedColumn(columnMod.getColumnName(), columnMod.getModifiedValue(), columnMod.getOriginalValue());
					}
					auditMap.put(altkey, compDeAudit);
					audit.addChildTable(compDeAudit);
				}
			} 
		}
		
		return audit;
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
