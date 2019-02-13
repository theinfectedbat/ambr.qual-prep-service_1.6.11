package com.ambr.gtm.fta.qts.workmgmt;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;

import com.ambr.gtm.fta.qps.qualtx.engine.QualTX;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponent;
import com.ambr.gtm.fta.qts.QTXCompWork;
import com.ambr.gtm.fta.qts.QTXCompWorkHS;
import com.ambr.gtm.fta.qts.QTXCompWorkIVA;
import com.ambr.gtm.fta.qts.QTXConsolWork;
import com.ambr.gtm.fta.qts.QTXStage;
import com.ambr.gtm.fta.qts.QTXWork;
import com.ambr.gtm.fta.qts.QTXWorkDetails;
import com.ambr.gtm.fta.qts.QTXWorkHS;
import com.ambr.gtm.fta.qts.QtxStatusUpdateRequest;
import com.ambr.gtm.fta.qts.ReQualificationReasonCodes;
import com.ambr.gtm.fta.qts.RequalificationWorkCodes;
import com.ambr.gtm.fta.qts.TrackerCodes;
import com.ambr.gtm.fta.qts.TrackerCodes.QualtxStatus;
import com.ambr.gtm.fta.qts.config.QEConfig;
import com.ambr.gtm.fta.qts.config.QEConfigCache;

//TODO should priority be used here?  this could cause out of sequence processing : new high priority applied before older lower priority targeting same record.
//TODO need to schedule this process to run at an exact time per company.  could be hourly, daily, or at midnight - review leave scheduling in TA for now, it can make rest call to requal to process ar_qtx_stage
//TODO remove the JSON structures and third party dependency  and replace with POJO
//TODO review getimpacted qualtx keys - can these be run in parallel and consolidated when complete.  then create ar_qtx_work items.
//TODO check priority of stage record, carry forward to ar_qtx_work record
//TODO what is final user id for merged set of records.
//TODO what should time_stamp be for created ar_qtx_work records?  last time_stamp on ar_qtx_stage for merged set of records or "now"
public class StageWorkConverter
{
	private static final Logger logger = LogManager.getLogger(StageWorkConverter.class);
	
	private ArQtxWorkUtility utility;
	private QEConfigCache qeConfigCache;
	
	private long totalStageSize = 0L;
	private long totalWorkSize = 0L;

	private Map<Long, ArrayList<Long>> bomRequalMap = new HashMap<>();
	private Map<Long, ArrayList<Long>> prodRequalMap = new HashMap<>();
	private Map<Long, String> newCtryCmpMap = new HashMap<>();
	private Map<Long, QualTX> contentRequalList = new HashMap<>();
	private Map<Long, QTXConsolWork> bomConsolMap = new HashMap<>();
	private Map<Long, QTXConsolWork> prodConsolMap = new HashMap<>();
	private Map<Long, QtxStageData> configRequalMap = new HashMap<>();
	
	private JdbcTemplate template;
	private int batchSize;

	public StageWorkConverter(ArQtxWorkUtility utility, QEConfigCache qeConfigCache, JdbcTemplate template, int batchSize)
	{
		this.utility = utility;
		this.qeConfigCache = qeConfigCache;
		this.template = template;
		this.batchSize = batchSize;
	}
	
	//TODO discuss "created by" following sql doesnt select user that created work record (submitted_by_user_id.  most logic is using mdi_qualtx.last_modified_by
	public long generateWorkFromStagedData(QTXStage stageData) throws Exception
	{
		if (stageData.batch_id == null || stageData.batch_id.isEmpty())
		{
			JSONObject workData = new JSONObject(stageData.data);
	
			if (workData.opt("PROD_DTLS") != null)
			{
				processProdRequalWork(stageData, workData);
			}
			else if (workData.opt("CONTENT_DTLS") != null)
			{
				processContentRequalWork(stageData, workData);
			}
			else if (workData.opt("MASS_QUALIFICATION") != null)
			{
				//processConfigRequalWork(stageData, workData, configRequalMap, bomConsolMap);
			}
			else
			{
				processBomUpdateWork(stageData, workData);
			}
		}
		else
		{
			JSONObject workData = new JSONObject(stageData.data);

			if (workData.opt("MASS_QUALIFICATION") != null)
			{
				processMassRequalWork(stageData, workData);
			}
			//TODO what happens in else?
		}
		
		return this.totalStageSize;
	}
	
	private Iterator<Entry<Long, ArrayList<Long>>> bomRequalIterator = null;
	private Iterator<Entry<Long, QualTX>> contentRequalIterator = null;
	private Iterator<Entry<Long, ArrayList<Long>>> prodRequalIterator = null;
	private Iterator<Entry<Long, String>> newCtryCmpIterator = null;
	private Iterator<Entry<Long, QtxStageData>> configRequalIterator = null;
	public Map<Long, QTXWork> getConsolidatedWork(long maxWork, Timestamp bestTime) throws Exception
	{
		if (this.bomRequalIterator == null) this.bomRequalIterator = bomRequalMap.entrySet().iterator();
		if (this.contentRequalIterator == null) this.contentRequalIterator = contentRequalList.entrySet().iterator();
		if (this.prodRequalIterator == null) this.prodRequalIterator = prodRequalMap.entrySet().iterator();
		if (this.newCtryCmpIterator == null) this.newCtryCmpIterator = this.newCtryCmpMap.entrySet().iterator();
		if (this.configRequalIterator == null) this.configRequalIterator = this.configRequalMap.entrySet().iterator();
		
				
		this.totalWorkSize = 0L;
		Map<Long, QTXWork> consolidatedWork = new HashMap<>();
		
		//Process all of these items.  gets impacted keys and updates qualtx to invalid.  no memory concerns.
		while (this.configRequalIterator.hasNext())
		{
			this.processMassQualificationWork(this.configRequalIterator.next(), consolidatedWork,bestTime);
			
			if (this.totalWorkSize > maxWork)
				return consolidatedWork;
	
		}

		while (this.bomRequalIterator.hasNext())
		{
			Map.Entry<Long, ArrayList<Long>> entry = this.bomRequalIterator.next();
			this.getConsolidatedQualtxForBomUpdate(entry, consolidatedWork, bestTime);
			
			if (this.totalWorkSize > maxWork)
				return consolidatedWork;
		}
		
		while (this.prodRequalIterator.hasNext())
		{
			Map.Entry<Long,ArrayList<Long>> entry = this.prodRequalIterator.next();
			this.getConsolidatedQualtxForProdUpdate(entry, consolidatedWork, bestTime);			
			
			if (this.totalWorkSize > maxWork)
				return consolidatedWork;
		}
		
		while (this.newCtryCmpIterator.hasNext())
		{
			Entry<Long, String> entry = this.newCtryCmpIterator.next();
			this.getConsolidatedQualtxForCtryUpdate(entry, consolidatedWork, bestTime);
			
			if (this.totalWorkSize > maxWork)
				return consolidatedWork;
		}
		
		while (this.contentRequalIterator.hasNext())
		{
			Map.Entry<Long, QualTX> entry = this.contentRequalIterator.next();
			this.getConsolidatedQualtxForContentUpdate(entry, consolidatedWork, bestTime);
			
			if (this.totalWorkSize > maxWork)
				return consolidatedWork;
		}
		
		return consolidatedWork;
	}
	
//	private List<QTXStage> processMassQualificationWorks(List<QTXStage> stageList) throws Exception
//	{
//		List<QTXStage> stageWorkListMassQual = new ArrayList<>();
//		List<QTXStage> stageWorkListQual = new ArrayList<>();
//		Map<Long, QtxStageData> configRequalMap = new HashMap<>();
//
//		for (QTXStage stageWork : stageList)
//		{
//			if (stageWork.batch_id == null || stageWork.batch_id.isEmpty())
//			{
//				stageWorkListQual.add(stageWork);
//			}
//			else
//			{
//				stageWorkListMassQual.add(stageWork);
//			}
//		}
//
//		for (QTXStage stageData : stageWorkListMassQual)
//		{
//			JSONObject workData = new JSONObject(stageData.data);
//
//			if (workData.opt("MASS_QUALIFICATION") != null)
//			{
//				processMassRequalWork(stageData, workData);
//			}
//			//TODO what happens in else?
//		}
//
//		processMassQualificationWork(configRequalMap);
//
//		return stageWorkListQual;
//	}

	private void getConsolidatedQualtxForBomUpdate(Map.Entry<Long, ArrayList<Long>> entry, Map<Long, QTXWork> consolidatedWork, Timestamp bestTime) throws Exception
	{
		long reasonCode = 0;
		try {
			reasonCode = entry.getKey();
			ArrayList<Long> theAltKeyList = entry.getValue();

			if(reasonCode == ReQualificationReasonCodes.BOM_QUAL_MPQ_CHG )
			{
				List<QualTX> theBOMHeaderChanges = this.utility.getImpactedQtxKeys(theAltKeyList, reasonCode);
				this.createArQtxBomCompBean(theBOMHeaderChanges, consolidatedWork, reasonCode, true, bomConsolMap, false, bestTime);
			}
			else if (reasonCode == ReQualificationReasonCodes.BOM_HDR_CHG  || reasonCode == ReQualificationReasonCodes.BOM_PRC_CHG || reasonCode == ReQualificationReasonCodes.BOM_PROD_AUTO_DE || reasonCode == ReQualificationReasonCodes.BOM_PROD_TXT_DE || reasonCode == ReQualificationReasonCodes.BOM_TXREF_CHG || reasonCode == ReQualificationReasonCodes.BOM_PRIORITIZE_QUALIFICATION)
			{
				List<QualTX> theBOMHeaderChanges = this.utility.getImpactedQtxKeys(theAltKeyList);
				this.createArQtxBomCompBean(theBOMHeaderChanges, consolidatedWork, reasonCode, true, bomConsolMap, false, bestTime);
			}
			else
			{
				List<QualTX> compChanges;
				if (reasonCode == ReQualificationReasonCodes.BOM_COMP_ADDED)
				{
					compChanges = this.utility.getImpactedQtxCompKeysForNewComp(theAltKeyList, reasonCode);
				}
				else
				{
					compChanges = this.utility.getImpactedQtxCompKeys(theAltKeyList, reasonCode);
				}

				this.createArQtxBomCompBean(compChanges, consolidatedWork, reasonCode, false, bomConsolMap, false, bestTime);
			}
		}catch(Exception e){
			logger.error("getConsolidatedQualtxForBomUpdate, Error while processing the reasonCode:" + reasonCode + "bomRequalMap ="+bomRequalMap.keySet());
		}
	}
	
	private void getConsolidatedQualtxForContentUpdate(Map.Entry<Long, QualTX> entry, Map<Long, QTXWork> consolidatedWork, Timestamp bestTime) throws Exception
	{
		long qualKey = entry.getKey();
		QualTX thequaltx = entry.getValue();

		if (consolidatedWork.containsKey(qualKey)) return;

		//logger.error("@@@@ getConsolidatedQualtxForContentUpdate=qualtx.src_key= " +thequaltx.src_key + " qualtx.alt_key_qualtx="+thequaltx.alt_key_qualtx+" bomConsolMap"+bomConsolMap.keySet());
		QTXWork theQtxWork = this.utility.createQtxWorkObj(thequaltx, 0, bomConsolMap, thequaltx.src_key);
		theQtxWork.setWorkStatus(QualtxStatus.READY_FOR_QUALIFICATION);
		consolidatedWork.put(thequaltx.alt_key_qualtx, theQtxWork);
	}

	private void processBomUpdateWork(QTXStage stageData, JSONObject workData) throws Exception
	{
		if(workData.opt("BOM_DTLS") == null) return ;  //This is to handle bad data, which doesn't starts with either PROD_DTLS,CONTENT_DTLS,BOM_DTLS
		JSONObject theBomDtlsObj = workData.getJSONObject("BOM_DTLS");
		if (null == theBomDtlsObj) return;

		if (!theBomDtlsObj.isNull("REASON_CODE"))
		{
			long bomkey = theBomDtlsObj.optLong("BOM_KEY");
			
			QTXConsolWork qtxConsolWork = bomConsolMap.get(bomkey);
			if(qtxConsolWork == null)
			{
				qtxConsolWork = new QTXConsolWork();
				qtxConsolWork.user_id = stageData.user_id;
				qtxConsolWork.priority= stageData.priority;
				qtxConsolWork.time_stamp = stageData.time_stamp;
				this.addToBOMConsollMap(bomkey, qtxConsolWork);
			}
			else
			{
				qtxConsolWork.priority = (qtxConsolWork.priority < stageData.priority) ? stageData.priority : qtxConsolWork.priority;
			}

			JSONArray theHeaderReasonCodes = theBomDtlsObj.getJSONArray("REASON_CODE");

			for (int index = 0; index < theHeaderReasonCodes.length(); index++)
			{
				ArrayList<Long> bomKeyList = null;
				if (bomRequalMap.containsKey(theHeaderReasonCodes.optLong(index)))
				{

					bomKeyList = bomRequalMap.get(theHeaderReasonCodes.optLong(index));
					bomKeyList.add(bomkey);
					this.totalStageSize++;
				}
				else
				{
					bomKeyList = new ArrayList<>();
					bomKeyList.add(bomkey);
					this.totalStageSize++;
				}

				if(ReQualificationReasonCodes.BOM_PRIORITIZE_QUALIFICATION == theHeaderReasonCodes.optLong(index)) continue;
				
				this.addToBOMRequalMap(theHeaderReasonCodes.optLong(index), bomKeyList);
			}
		}

		JSONArray theCompDtls = theBomDtlsObj.getJSONArray("COMP_DTLS");

		if (!theCompDtls.isNull(0))
		{
			for (int index = 0; index < theCompDtls.length(); index++)
			{
				ArrayList<Long> compKeyList = null;
				JSONObject theCompDtl = theCompDtls.getJSONObject(index);

				if (bomRequalMap.containsKey(theCompDtl.optLong("REASON_CODES")))
				{
					compKeyList = bomRequalMap.get(theCompDtl.optLong("REASON_CODES"));
					compKeyList.add(theCompDtl.optLong("COMP_KEY"));
					this.totalStageSize++;
				}
				else
				{
					compKeyList = new ArrayList<>();
					compKeyList.add(theCompDtl.optLong("COMP_KEY"));
					this.totalStageSize++;
				}
				
				this.addToBOMRequalMap(theCompDtl.optLong("REASON_CODES"), compKeyList);
			}
		}
	}
		
	private void addToBOMConsollMap(Long key, QTXConsolWork value)
	{
		this.bomConsolMap.put(key, value);
		this.totalStageSize++;
	}
	
	private void addToBOMRequalMap(Long key, ArrayList<Long> value)
	{
		this.bomRequalMap.put(key, value);
	}
	
	private void processContentRequalWork(QTXStage stageData, JSONObject workData) throws Exception
	{
		JSONArray theContentDtlsObj = workData.getJSONArray("CONTENT_DTLS");

		if (!theContentDtlsObj.isNull(0))
		{
			for (int index = 0; index < theContentDtlsObj.length(); index++)
			{
				JSONObject theContentDtl = theContentDtlsObj.getJSONObject(index);
				QualTX qualtx = new QualTX();
				
				qualtx.org_code = theContentDtl.optString("ORG_CODE");
				qualtx.alt_key_qualtx =  theContentDtl.optLong("QUALIFICATION_KEY");
				qualtx.src_key = theContentDtl.optLong("BOM_KEY");
				
				this.addContentRequalList(theContentDtl.optLong("QUALIFICATION_KEY"), qualtx);
				QTXConsolWork qtxConsolWork = bomConsolMap.get(theContentDtl.optLong("BOM_KEY"));
				if(qtxConsolWork == null)
				{
					qtxConsolWork = new QTXConsolWork();
					qtxConsolWork.user_id = stageData.user_id;
					qtxConsolWork.priority= stageData.priority;
					qtxConsolWork.time_stamp = stageData.time_stamp;
					this.addToBOMConsollMap(theContentDtl.optLong("BOM_KEY"), qtxConsolWork);
				}
				else
				{
					qtxConsolWork.priority = (qtxConsolWork.priority < stageData.priority) ? stageData.priority : qtxConsolWork.priority;
				}
			}
		}
	}
	
	private void addContentRequalList(Long key, QualTX value)
	{
		this.contentRequalList.put(key, value);
		this.totalStageSize++;
	}

	public void createArQtxBomCompBean(List<QualTX> qualtxList, Map<Long, QTXWork> consolidatedWork, long reasonCode, boolean isHeader, Map<Long, QTXConsolWork> bomConsolMap, boolean isForceQualification, Timestamp bestTime) throws Exception
	{

		long workReasonCode = this.utility.getQtxWorkReasonCodes(reasonCode);
		for (QualTX qualtx : qualtxList)
		{
			QTXWork theQtxWork = null;
			QTXWorkDetails theQtxWorkDe = null;
			QTXCompWork theQtxCompWork = null;
			QTXCompWorkIVA theQtxCompIvaWork = null;
			List<QTXCompWork> theQtxCompList = null;
			boolean isValQualtx = false;

		    isValQualtx = isValidQualtx(qualtx, true, bestTime);
		    if(!isValQualtx) 
		    	continue;
		    
			boolean isNewComp = false;

			if (isHeader)
			{
				if (consolidatedWork.containsKey(qualtx.alt_key_qualtx))
				{
					theQtxWork = consolidatedWork.get(qualtx.alt_key_qualtx);
					theQtxWorkDe = theQtxWork.details;
					theQtxWorkDe.setReasonCodeFlag(workReasonCode);
				}
				else
				{

					//logger.error("@@@@ createArQtxBomCompBean=qualtx.src_key= " +qualtx.src_key + " qualtx.alt_key_qualtx="+qualtx.alt_key_qualtx+" workReasonCode="+workReasonCode+ " bomConsolMap"+bomConsolMap.keySet());
					theQtxWork = this.utility.createQtxWorkObj(qualtx, workReasonCode, bomConsolMap, qualtx.src_key);
				}

				if (theQtxWork.details.reason_code == RequalificationWorkCodes.BOM_TXREF_CHG || theQtxWork.details.reason_code == RequalificationWorkCodes.BOM_QUAL_MPQ_CHG || (theQtxWork.details.reason_code == RequalificationWorkCodes.HEADER_CONFIG_CHANGE))
				{
					theQtxWork.setWorkStatus(QualtxStatus.READY_FOR_QUALIFICATION);
				}
			}
			else
			{

				if(qualtx.compList.isEmpty()) 
				{
					logger.info("Data issue qualtx =" +qualtx.alt_key_qualtx);
					continue;
				}
				QualTXComponent qualtxComp = qualtx.compList.get(0);	//based on sql there will always be a single QualTXComponent
				
			    isValQualtx = isValidQualtxComp(qualtxComp);
			    if(!isValQualtx) 
			    	continue;
			    
			    if (consolidatedWork.containsKey(qualtx.alt_key_qualtx))
				{
					theQtxWork = consolidatedWork.get(qualtx.alt_key_qualtx);
					theQtxCompList = theQtxWork.compWorkList;

					for (QTXCompWork compWork : theQtxCompList)
					{
						if (compWork.bom_comp_key == qualtxComp.src_key)
						{
							isNewComp = false;
							if (reasonCode == ReQualificationReasonCodes.BOM_COMP_SRC_CHG)
							{
								if (!compWork.compWorkIVAList.isEmpty())
								{
									if ((compWork.compWorkIVAList.get(0).iva_key == qualtxComp.prod_src_iva_key))
									{
										compWork.compWorkIVAList.get(0).setReasonCodeFlag(workReasonCode);
									}
								}
								else
								{
									theQtxCompIvaWork = this.utility.createQtxCompIVAWorkObj(qualtxComp, workReasonCode, compWork.qtx_comp_wid, compWork.qtx_wid);
									theQtxCompIvaWork.time_stamp = theQtxWork.time_stamp;
									compWork.compWorkIVAList.add(theQtxCompIvaWork);
								}
							}
							else
							{
								compWork.setReasonCodeFlag(workReasonCode);
							}
						}
						else
						{
							isNewComp = true;
						}
					}

					if (theQtxCompList.isEmpty() || isNewComp)
					{
						theQtxCompWork = this.utility.createQtxCompWorkObj(qualtx, qualtxComp, 0, theQtxWork.qtx_wid);
						theQtxCompWork.time_stamp = theQtxWork.time_stamp;
						theQtxCompWork.status.time_stamp = theQtxWork.time_stamp;
						theQtxWork.compWorkList.add(theQtxCompWork);
						if (reasonCode == ReQualificationReasonCodes.BOM_COMP_SRC_CHG)
						{
							theQtxCompIvaWork = this.utility.createQtxCompIVAWorkObj(qualtxComp, workReasonCode, theQtxCompWork.qtx_comp_wid, theQtxCompWork.qtx_wid);
							theQtxCompIvaWork.time_stamp = theQtxWork.time_stamp;
							theQtxCompWork.compWorkIVAList.add(theQtxCompIvaWork);
						}
						else
						{
							theQtxCompWork.setReasonCodeFlag(workReasonCode);
						}
					}
				}
				else
				{
					//logger.error("@@@@ createArQtxBomCompBean 2=qualtx.src_key= " +qualtx.src_key + " qualtx.alt_key_qualtx="+qualtx.alt_key_qualtx+" workReasonCode="+workReasonCode+ " bomConsolMap"+bomConsolMap.keySet());
					theQtxWork = this.utility.createQtxWorkObj(qualtx, 0, bomConsolMap, qualtx.src_key);
					theQtxCompWork = this.utility.createQtxCompWorkObj(qualtx, qualtxComp, workReasonCode, theQtxWork.qtx_wid);
					theQtxCompWork.time_stamp = theQtxWork.time_stamp;
					theQtxCompWork.status.time_stamp = theQtxWork.time_stamp;
					theQtxWork.compWorkList.add(theQtxCompWork);
					if (ReQualificationReasonCodes.BOM_COMP_SRC_CHG == reasonCode)
					{
						theQtxCompIvaWork = this.utility.createQtxCompIVAWorkObj(qualtxComp, workReasonCode, theQtxCompWork.qtx_comp_wid, theQtxCompWork.qtx_wid);
						theQtxCompIvaWork.time_stamp = theQtxWork.time_stamp;
						theQtxCompWork.compWorkIVAList.add(theQtxCompIvaWork);
						theQtxCompWork.setReasonCodeFlag(0);
					}
				}
			}

			consolidatedWork.put(qualtx.alt_key_qualtx, theQtxWork);
		}
	}

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
	
	private void addProdConsolMap(Long key, QTXConsolWork value)
	{
		this.prodConsolMap.put(key, value);
		this.totalStageSize++;
	}

	public void processProdRequalWork(QTXStage stageData, JSONObject theWorkObj) throws Exception
	{

		if (theWorkObj.isNull("PROD_DTLS")) return;

		JSONObject theProdDtlsObj = theWorkObj.getJSONObject("PROD_DTLS");
		long key = theProdDtlsObj.optLong("PROD_KEY");
		QTXConsolWork qtxConsolWork = prodConsolMap.get(key);
		if(qtxConsolWork == null)
		{
			qtxConsolWork = new QTXConsolWork();
			qtxConsolWork.time_stamp  = stageData.time_stamp;
			qtxConsolWork.user_id = stageData.user_id;
			qtxConsolWork.priority = stageData.priority;
			this.addProdConsolMap(key, qtxConsolWork);
		}
			
		JSONArray theSrcDtls = theProdDtlsObj.getJSONArray("SRC_DTLS");

		if (!theSrcDtls.isNull(0))
		{
			for (int index = 0; index < theSrcDtls.length(); index++)
			{
				JSONObject theSrcDtl = theSrcDtls.getJSONObject(index);
				ArrayList<Long> srcKeyList = null;
				if (prodRequalMap.containsKey(theSrcDtl.optLong("REASON_CODE")))
				{
					srcKeyList = prodRequalMap.get(theSrcDtl.optLong("REASON_CODE"));
					srcKeyList.add(theSrcDtl.optLong("SRC_KEY"));
					this.totalStageSize++;
				}
				else
				{
					srcKeyList = new ArrayList<>();
					srcKeyList.add(theSrcDtl.optLong("SRC_KEY"));
					this.totalStageSize++;
				}
				
				this.addProdRequalMap(theSrcDtl.optLong("REASON_CODE"), srcKeyList);
			}
		}

		JSONArray theIvaDtls = theProdDtlsObj.getJSONArray("IVA_DTLS");
		if (!theIvaDtls.isNull(0))
		{
			for (int index = 0; index < theIvaDtls.length(); index++)
			{
				JSONObject theIvaDtl = theIvaDtls.getJSONObject(index);
				ArrayList<Long> ivaKeyList = null;
				if (prodRequalMap.containsKey(theIvaDtl.optLong("REASON_CODE")))
				{
					ivaKeyList = prodRequalMap.get(theIvaDtl.optLong("REASON_CODE"));
					ivaKeyList.add(theIvaDtl.optLong("IVA_KEY"));
					this.totalStageSize++;
				}
				else
				{
					ivaKeyList = new ArrayList<>();
					ivaKeyList.add(theIvaDtl.optLong("IVA_KEY"));
					this.totalStageSize++;
				}
				
				this.addProdRequalMap(theIvaDtl.optLong("REASON_CODE"), ivaKeyList);
			}
		}

		JSONArray theCtryCmplDtls = theProdDtlsObj.getJSONArray("CTRY_CMPL_DTLS");
		if (!theCtryCmplDtls.isNull(0))
		{
			for (int index = 0; index < theCtryCmplDtls.length(); index++)
			{

				JSONObject theCtryCmplDtl = theCtryCmplDtls.getJSONObject(index);

				if (theCtryCmplDtl.optLong("REASON_CODE") == ReQualificationReasonCodes.GPM_CTRY_CMPL_ADDED)
				{

					newCtryCmpMap.put(theCtryCmplDtl.optLong("PROD_SRC_IVA_KEY"), theCtryCmplDtl.optString("CTRY_CPML_CTRY") + "|" + theCtryCmplDtl.optLong("CTRY_CPML_KEY"));
					continue;
				}

				ArrayList<Long> ctryCmpKeyList = null;
				if (prodRequalMap.containsKey(theCtryCmplDtl.optLong("REASON_CODE")))
				{
					ctryCmpKeyList = prodRequalMap.get(theCtryCmplDtl.optLong("REASON_CODE"));
					ctryCmpKeyList.add(theCtryCmplDtl.optLong("CTRY_CPML_KEY"));
					this.totalStageSize++;
				}
				else
				{
					ctryCmpKeyList = new ArrayList<>();
					ctryCmpKeyList.add(theCtryCmplDtl.optLong("CTRY_CPML_KEY"));
					this.totalStageSize++;
				}
				
				this.addProdRequalMap(theCtryCmplDtl.optLong("REASON_CODE"), ctryCmpKeyList);
			}
		}
		
		JSONArray theCooDtls = theProdDtlsObj.optJSONArray("COO_DTLS");
		ArrayList<Long> CooDtlsList = null;
		if (null != theCooDtls && !theCooDtls.isNull(0))
		{
			for (int index = 0; index < theCooDtls.length(); index++)
			{
				JSONObject theCooDtl = theCooDtls.getJSONObject(index);
				if (!prodRequalMap.containsKey(theCooDtl.optLong("REASON_CODE")))
				{
					CooDtlsList = new ArrayList<>();
					CooDtlsList.add(key);
					this.totalStageSize++;
					this.addProdRequalMap(theCooDtl.optLong("REASON_CODE"), CooDtlsList);
				}
			}
		}
	}
	
	private void addProdRequalMap(Long key, ArrayList<Long> value)
	{
		this.prodRequalMap.put(key, value);
	}

	private void getConsolidatedQualtxForProdUpdate(Map.Entry<Long, ArrayList<Long>> entry, Map<Long, QTXWork> consolidatedWork, Timestamp bestTime) throws Exception
	{
		long reasonCode = entry.getKey();
		try {
			ArrayList<Long> keyList = entry.getValue();

			List<QualTX> qualtxCompList = this.utility.getImpactedQtxCompKeys(keyList, reasonCode);

			buildCompProdQtxWorkBean(qualtxCompList, reasonCode, consolidatedWork, prodConsolMap, bestTime, bomConsolMap);

			if (reasonCode == ReQualificationReasonCodes.GPM_CTRY_CMPL_CHANGE || reasonCode == ReQualificationReasonCodes.GPM_CTRY_CMPL_DELETED || reasonCode == ReQualificationReasonCodes.GPM_NEW_IVA_IDENTIFED || reasonCode == ReQualificationReasonCodes.GPM_IVA_CHANGE_M_I || reasonCode == ReQualificationReasonCodes.GPM_SRC_IVA_DELETED || reasonCode == ReQualificationReasonCodes.GPM_IVA_AND_CLAIM_DTLS_CHANGE || reasonCode == ReQualificationReasonCodes.GPM_SRC_CHANGE)
			{
				List<QualTX> qualtxList = this.utility.getImpactedQtxKeys(keyList, reasonCode);
				buildheaderProdQtxWorkBean(qualtxList, reasonCode, consolidatedWork, prodConsolMap, bestTime, bomConsolMap);
			}
		}catch(Exception e){
				logger.error("getConsolidatedQualtxForProdUpdate, Error while processing the reasonCode:" + reasonCode + "newCtryCmpMap ="+newCtryCmpMap.keySet());
		}
	}
	
	private void getConsolidatedQualtxForCtryUpdate(Map.Entry<Long, String> entry, Map<Long, QTXWork> consolidatedWork, Timestamp bestTime) throws Exception
	{	
		// New Ctry Compliance added in Component so need to explicitly pass the
		// new ctry compliance key and target HS country
		try{
			long altKeyIva = entry.getKey();
			ArrayList<Long> ivaKey = new ArrayList<>();
			ivaKey.add(altKeyIva);
			String iva = entry.getValue();
			if (null != iva)
			{
				String[] ivaValueArr = iva.split("\\|");
				String ctryCmplkey = ivaValueArr[1];
				String ctryCmplCode = ivaValueArr[0];

				// Processing new country compliance addition in component product.
				List<QualTX> qualtxCompList = this.utility.getImpactedQtxCompKeys(ivaKey, ReQualificationReasonCodes.GPM_CTRY_CMPL_ADDED);
				buildQtxForNewCtryCmpl(qualtxCompList, ctryCmplkey, ctryCmplCode, consolidatedWork, ReQualificationReasonCodes.GPM_CTRY_CMPL_ADDED, prodConsolMap, false, bestTime);

				// Processing new country compliance addition in main product.
				List<QualTX> qualtxList = this.utility.getImpactedQtxKeys(ivaKey, ReQualificationReasonCodes.GPM_CTRY_CMPL_ADDED);
				buildQtxForNewCtryCmpl(qualtxList, ctryCmplkey, ctryCmplCode, consolidatedWork, ReQualificationReasonCodes.GPM_CTRY_CMPL_ADDED, prodConsolMap, true, bestTime);
			}
		}catch(Exception e){
			logger.error("getConsolidatedQualtxForProdUpdate, Error while processing the ReQualificationReasonCodes.GPM_CTRY_CMPL_ADDED:");
		}
	}

	private void buildQtxForNewCtryCmpl(List<QualTX> qualtxList, String ctryCmplkey, String ctryCmplCode, Map<Long, QTXWork> consolidatedWork, long reasonCode, Map<Long, QTXConsolWork> prodConsolMap, boolean isHeader, Timestamp bestTime) throws Exception
	{
		long workCode = this.utility.getQtxWorkReasonCodes(reasonCode);
		for (QualTX qualtx : qualtxList)
		{
			QTXWork theQtxWork = null;
			QTXCompWork theQtxCompWork = null;
			QTXCompWorkHS theQtxCompHsWork = null;
			QTXWorkHS theQtxHsWork = null;

			boolean isNewComp = false;
			List<QTXCompWork> theQtxCompList = null;
			boolean isValQualtx = false;

		    isValQualtx = isValidQualtx(qualtx, false, bestTime);
		    if(!isValQualtx) 
		    	continue;

			if (isHeader)
			{

				if (consolidatedWork.containsKey(qualtx.alt_key_qualtx))
				{
					theQtxWork = consolidatedWork.get(qualtx.alt_key_qualtx);

					theQtxHsWork = this.createQtxHSWorkObj(qualtx, workCode, theQtxWork.qtx_wid);
					theQtxHsWork.time_stamp = theQtxWork.time_stamp;
				}
				else
				{
					theQtxWork = this.createQtxWorkObj(qualtx, 0, prodConsolMap, qualtx.prod_key);

					theQtxHsWork = this.createQtxHSWorkObj(qualtx, workCode, theQtxWork.qtx_wid);
				}

				theQtxHsWork.ctry_cmpl_key = Long.valueOf(ctryCmplkey);
				theQtxHsWork.target_hs_ctry = ctryCmplCode;
				theQtxWork.workHSList.add(theQtxHsWork);

			}
			else
			{

			QualTXComponent qualtxComp = qualtx.compList.get(0);	//There will always be one QualTXComponent present (as defined by sql that pulled this data
			
			if(!isValidQualtxComp(qualtxComp)) continue;
			if(workCode == RequalificationWorkCodes.GPM_CTRY_CMPL_ADDED )
		    {
		    	String	criticalIndicator = qualtxComp.critical_indicator == null ? "" : qualtxComp.critical_indicator;
		    	QEConfig  QEConfig = qeConfigCache.getQEConfig(qualtx.org_code);
				if(QEConfig != null && QEConfig.getBomAnalysisConfigData() != null && QEConfig.getBomAnalysisConfigData().componentHsTrigger().equals("Y"))
				{
					if((QEConfig.getBomAnalysisConfigData().getComponentHsTriggerValue().equals("ANY") || QEConfig.getBomAnalysisConfigData().getComponentHsTriggerValue().equals(RequalificationWorkCodes.CRITICAL)) && (!criticalIndicator.equals(RequalificationWorkCodes.CRITICAL_HS) || !criticalIndicator.equals(RequalificationWorkCodes.CRITICAL_ANY)))
					{
						continue;
					}
				}
				if(QEConfig != null && QEConfig.getBomAnalysisConfigData() != null && QEConfig.getBomAnalysisConfigData().componentDecisionTrigger().equals("Y"))
				{
					if((QEConfig.getBomAnalysisConfigData().getComponentDecisionTriggerValue().equals("ANY") || QEConfig.getBomAnalysisConfigData().getComponentDecisionTriggerValue().equals(RequalificationWorkCodes.CRITICAL)) && (!criticalIndicator.equals(RequalificationWorkCodes.CRITICAL_QUALIFIED) || !criticalIndicator.equals(RequalificationWorkCodes.CRITICAL_QUALIFIED_NO_SHIFT) || !criticalIndicator.equals(RequalificationWorkCodes.CRITICAL_ANY)))
					{
						continue;
					}
				}
			}
			if (consolidatedWork.containsKey(qualtx.alt_key_qualtx))
			{
				theQtxWork = consolidatedWork.get(qualtx.alt_key_qualtx);
				theQtxCompList = theQtxWork.compWorkList;

				for (QTXCompWork compWork : theQtxCompList)
				{
					if (compWork.qualtx_comp_key == qualtxComp.alt_key_comp)
					{
						isNewComp = false;
						if (reasonCode == ReQualificationReasonCodes.GPM_CTRY_CMPL_ADDED)
						{
							theQtxCompHsWork = this.createQtxCompHSWorkObj(qualtxComp, workCode, compWork.qtx_comp_wid, compWork.qtx_wid);
							theQtxCompHsWork.time_stamp = theQtxWork.time_stamp;
							theQtxCompHsWork.ctry_cmpl_key = Long.valueOf(ctryCmplkey);
							theQtxCompHsWork.target_hs_ctry = ctryCmplCode;
							compWork.compWorkHSList.add(theQtxCompHsWork);
						}
					}
					else
					{
						isNewComp = true;
					}
				}

				if (theQtxCompList.isEmpty() || isNewComp)
				{
					theQtxCompWork = this.createQtxCompWorkObj(qualtx, qualtxComp, 0, theQtxWork.qtx_wid);
					theQtxCompWork.time_stamp = theQtxWork.time_stamp;
					theQtxCompWork.status.time_stamp = theQtxWork.time_stamp;
					theQtxWork.compWorkList.add(theQtxCompWork);

					if (reasonCode == ReQualificationReasonCodes.GPM_CTRY_CMPL_ADDED)
					{
						theQtxCompHsWork = this.createQtxCompHSWorkObj(qualtxComp, workCode, theQtxCompWork.qtx_comp_wid, theQtxCompWork.qtx_wid);
						theQtxCompHsWork.ctry_cmpl_key = Long.valueOf(ctryCmplkey);
						theQtxCompHsWork.target_hs_ctry = ctryCmplCode;
						theQtxCompWork.compWorkHSList.add(theQtxCompHsWork);
					}
				}
			}
			else
			{
				//logger.error("@@@@ buildQtxForNewCtryCmpl=qualtx.src_key= " +qualtx.src_key + "qualtxComp.prod_key= "+qualtxComp.prod_key+" qualtx.alt_key_qualtx="+qualtx.alt_key_qualtx+" prodConsolMap"+prodConsolMap.keySet());
				theQtxWork = this.createQtxWorkObj(qualtx, 0, prodConsolMap, qualtxComp.prod_key);
				theQtxCompWork = this.createQtxCompWorkObj(qualtx, qualtxComp, 0, theQtxWork.qtx_wid);
				theQtxCompWork.time_stamp = theQtxWork.time_stamp;
				theQtxCompWork.status.time_stamp = theQtxWork.time_stamp;
				theQtxWork.compWorkList.add(theQtxCompWork);

				if (reasonCode == ReQualificationReasonCodes.GPM_CTRY_CMPL_ADDED)
				{
					theQtxCompHsWork = this.createQtxCompHSWorkObj(qualtxComp, workCode, theQtxCompWork.qtx_comp_wid, theQtxCompWork.qtx_wid);
					theQtxCompHsWork.time_stamp = theQtxWork.time_stamp;
					theQtxCompHsWork.ctry_cmpl_key = Long.valueOf(ctryCmplkey);
					theQtxCompHsWork.target_hs_ctry = ctryCmplCode;

					theQtxCompWork.compWorkHSList.add(theQtxCompHsWork);
				}
			}
		  }

			consolidatedWork.put(qualtx.alt_key_qualtx, theQtxWork);
		}
	}

	private void buildheaderProdQtxWorkBean(List<QualTX> qualtxList, long reasonCode, Map<Long, QTXWork> qtxWorkList, Map<Long, QTXConsolWork> prodConsolMap, Timestamp bestTime, Map<Long, QTXConsolWork> bomConsolMap) throws Exception
	{
		long workCode = this.utility.getQtxWorkReasonCodes(reasonCode);
		for (QualTX qualtx : qualtxList)
		{
			QTXWork theQtxWork = null;
			QTXWorkHS theQtxHsWork = null;
			boolean isValQualtx = false;

		    isValQualtx = isValidQualtx(qualtx, false, bestTime);
		    if(!isValQualtx)
		    	continue;

			if (qtxWorkList.containsKey(qualtx.alt_key_qualtx))
			{
				theQtxWork = qtxWorkList.get(qualtx.alt_key_qualtx);
				if (reasonCode == ReQualificationReasonCodes.GPM_NEW_IVA_IDENTIFED || reasonCode == ReQualificationReasonCodes.GPM_IVA_CHANGE_M_I || reasonCode == ReQualificationReasonCodes.GPM_SRC_IVA_DELETED || reasonCode == ReQualificationReasonCodes.GPM_IVA_AND_CLAIM_DTLS_CHANGE || reasonCode == ReQualificationReasonCodes.GPM_SRC_CHANGE)
				{
					theQtxWork.details.setReasonCodeFlag(workCode);
					theQtxWork.setWorkStatus(QualtxStatus.INIT);
					continue;
				}
				theQtxHsWork = this.createQtxHSWorkObj(qualtx, workCode, theQtxWork.qtx_wid);
				theQtxHsWork.time_stamp = theQtxWork.time_stamp;
				theQtxWork.workHSList.add(theQtxHsWork);
			}
			else
			{
				//logger.error("@@@@ buildheaderProdQtxWorkBean=qualtx.src_key= " +qualtx.src_key + " qualtx.alt_key_qualtx="+qualtx.alt_key_qualtx+" prodConsolMap"+prodConsolMap.keySet());
				theQtxWork = this.createQtxWorkObj(qualtx, 0, prodConsolMap, qualtx.prod_key);

				if (null != bomConsolMap)
				{
					QTXConsolWork qtxConsolWork = bomConsolMap.get(qualtx.src_key);
					if (null != qtxConsolWork) theQtxWork.priority = qtxConsolWork.priority;
				}
				if (reasonCode == ReQualificationReasonCodes.GPM_NEW_IVA_IDENTIFED || reasonCode == ReQualificationReasonCodes.GPM_IVA_CHANGE_M_I || reasonCode == ReQualificationReasonCodes.GPM_SRC_IVA_DELETED || reasonCode == ReQualificationReasonCodes.GPM_IVA_AND_CLAIM_DTLS_CHANGE || reasonCode == ReQualificationReasonCodes.GPM_SRC_CHANGE)
				{
					theQtxWork.details.setReasonCodeFlag(workCode);
					theQtxWork.setWorkStatus(QualtxStatus.INIT);
					qtxWorkList.put(qualtx.alt_key_qualtx, theQtxWork);
					continue;
				}

				theQtxHsWork = this.createQtxHSWorkObj(qualtx, workCode, theQtxWork.qtx_wid);

				theQtxWork.workHSList.add(theQtxHsWork);
			}

			qtxWorkList.put(qualtx.alt_key_qualtx, theQtxWork);
		}
	}

	private void buildCompProdQtxWorkBean(List<QualTX> qualtxList, long reasonCode, Map<Long, QTXWork> qtxWorkList, Map<Long, QTXConsolWork> prodConsolMap, Timestamp bestTime, Map<Long, QTXConsolWork> bomConsolMap) throws Exception
	{
		long workCode = this.utility.getQtxWorkReasonCodes(reasonCode);
		for (QualTX qualtx : qualtxList)
		{
			QualTXComponent qualtxComp = qualtx.compList.get(0); //Based on sql used there will always be a single QualTXComponent present
			QTXWork theQtxWork = null;
			QTXCompWork theQtxCompWork = null;
			QTXCompWorkIVA theQtxCompIvaWork = null;
			QTXCompWorkHS theQtxCompHsWork = null;
			boolean isNewComp = false;
			List<QTXCompWork> theQtxCompList = null;
			boolean isValQualtx = false;

		    isValQualtx = isValidQualtx(qualtx, false, bestTime);
		    if(!isValQualtx) 
		    	continue;

		    if(!isValidQualtxComp(qualtxComp)) continue;
		    if(workCode == RequalificationWorkCodes.GPM_CTRY_CMPL_CHANGE || workCode == RequalificationWorkCodes.GPM_CTRY_CMPL_DELETED || workCode == RequalificationWorkCodes.GPM_CTRY_CMPL_ADDED )
		    {
		    	String	criticalIndicator = qualtxComp.critical_indicator == null ? "" : qualtxComp.critical_indicator;
		    	QEConfig  QEConfig = qeConfigCache.getQEConfig(qualtx.org_code);
				if(QEConfig != null && QEConfig.getBomAnalysisConfigData() != null && QEConfig.getBomAnalysisConfigData().componentHsTrigger().equals("Y"))
				{
					if((QEConfig.getBomAnalysisConfigData().getComponentHsTriggerValue().equals("ANY") || QEConfig.getBomAnalysisConfigData().getComponentHsTriggerValue().equals(RequalificationWorkCodes.CRITICAL)) && !isRequalificationRequired(criticalIndicator, true))
					{
						continue;
					}
				}
				if(QEConfig != null && QEConfig.getBomAnalysisConfigData() != null && QEConfig.getBomAnalysisConfigData().componentDecisionTrigger().equals("Y"))
				{
					if((QEConfig.getBomAnalysisConfigData().getComponentDecisionTriggerValue().equals("ANY") || QEConfig.getBomAnalysisConfigData().getComponentDecisionTriggerValue().equals(RequalificationWorkCodes.CRITICAL)) && !isRequalificationRequired(criticalIndicator, false))
					{
						continue;
					}
				}
			}
		   
			if (qtxWorkList.containsKey(qualtx.alt_key_qualtx))
			{
				theQtxWork = qtxWorkList.get(qualtx.alt_key_qualtx);
				theQtxCompList = theQtxWork.compWorkList;

				for (QTXCompWork compWork : theQtxCompList)
				{
					if (compWork.bom_comp_key == qualtxComp.src_key)
					{
						isNewComp = false;
						if (reasonCode == ReQualificationReasonCodes.GPM_CTRY_CMPL_CHANGE || reasonCode == ReQualificationReasonCodes.GPM_CTRY_CMPL_DELETED)
						{
							if (!compWork.compWorkHSList.isEmpty())
							{
								if ((compWork.compWorkHSList.get(0).ctry_cmpl_key == qualtxComp.prod_ctry_cmpl_key))
								{
									compWork.compWorkHSList.get(0).setReasonCodeFlag(workCode);
								}
							}
							else
							{
								theQtxCompHsWork = this.createQtxCompHSWorkObj(qualtxComp, workCode, compWork.qtx_comp_wid, compWork.qtx_wid);
								theQtxCompHsWork.time_stamp = theQtxWork.time_stamp;
								compWork.compWorkHSList.add(theQtxCompHsWork);
							}
						}
						else if (reasonCode == ReQualificationReasonCodes.GPM_SRC_ADDED || reasonCode == ReQualificationReasonCodes.GPM_SRC_DELETED || reasonCode == ReQualificationReasonCodes.GPM_SRC_CHANGE || reasonCode == ReQualificationReasonCodes.GPM_SRC_IVA_DELETED || reasonCode == ReQualificationReasonCodes.GPM_COMP_FINAL_DECISION_CHANGE || reasonCode == ReQualificationReasonCodes.GPM_IVA_CHANGE_M_I || reasonCode == ReQualificationReasonCodes.GPM_COMP_PREV_YEAR_QUAL_CHANGE || reasonCode == ReQualificationReasonCodes.GPM_COMP_CUMULATION_CHANGE || reasonCode == ReQualificationReasonCodes.GPM_COMP_TRACE_VALUE_CHANGE)
						{

							if (!compWork.compWorkIVAList.isEmpty())
							{
								if ((compWork.compWorkIVAList.get(0).iva_key == qualtxComp.prod_src_iva_key))
								{
									compWork.compWorkIVAList.get(0).setReasonCodeFlag(workCode);
								}

							}
							else
							{
								theQtxCompIvaWork = this.createQtxCompIVAWorkObj(qualtxComp, workCode, compWork.qtx_comp_wid, compWork.qtx_wid);
								theQtxCompIvaWork.time_stamp = theQtxWork.time_stamp;
								compWork.compWorkIVAList.add(theQtxCompIvaWork);
							}
						}
						else
						{
							compWork.setReasonCodeFlag(workCode);
						}
					}
					else
					{
						isNewComp = true;
					}
				}

				if (theQtxCompList.isEmpty() || isNewComp)
				{
					theQtxCompWork = this.createQtxCompWorkObj(qualtx, qualtxComp, 0, theQtxWork.qtx_wid);
					theQtxCompWork.time_stamp = theQtxWork.time_stamp;
					theQtxCompWork.status.time_stamp = theQtxWork.time_stamp;
					theQtxWork.compWorkList.add(theQtxCompWork);

					if (reasonCode == ReQualificationReasonCodes.GPM_CTRY_CMPL_CHANGE || reasonCode == ReQualificationReasonCodes.GPM_CTRY_CMPL_DELETED)
					{
						theQtxCompHsWork = this.createQtxCompHSWorkObj(qualtxComp, workCode, theQtxCompWork.qtx_comp_wid, theQtxCompWork.qtx_wid);
						theQtxCompHsWork.time_stamp = theQtxWork.time_stamp;
						theQtxCompWork.compWorkHSList.add(theQtxCompHsWork);
					}
					else if (reasonCode == ReQualificationReasonCodes.GPM_SRC_ADDED || reasonCode == ReQualificationReasonCodes.GPM_SRC_DELETED || reasonCode == ReQualificationReasonCodes.GPM_SRC_CHANGE || reasonCode == ReQualificationReasonCodes.GPM_SRC_IVA_DELETED || reasonCode == ReQualificationReasonCodes.GPM_COMP_FINAL_DECISION_CHANGE || reasonCode == ReQualificationReasonCodes.GPM_IVA_CHANGE_M_I || reasonCode == ReQualificationReasonCodes.GPM_COMP_PREV_YEAR_QUAL_CHANGE || reasonCode == ReQualificationReasonCodes.GPM_COMP_CUMULATION_CHANGE || reasonCode == ReQualificationReasonCodes.GPM_COMP_TRACE_VALUE_CHANGE)
					{
						theQtxCompIvaWork = this.createQtxCompIVAWorkObj(qualtxComp, workCode, theQtxCompWork.qtx_comp_wid, theQtxCompWork.qtx_wid);
						theQtxCompIvaWork.time_stamp = theQtxWork.time_stamp;
						theQtxCompWork.compWorkIVAList.add(theQtxCompIvaWork);
					}
					else
					{
						theQtxCompWork.setReasonCodeFlag(workCode);
					}
				}
			}
			else
			{
				//logger.error("@@@@ buildCompProdQtxWorkBean=qualtx.src_key= " +qualtx.src_key + " qualtxComp.prod_key " +qualtxComp.prod_key+ "qualtxComp.src_key =" +qualtxComp.src_key+" qualtx.alt_key_qualtx="+qualtx.alt_key_qualtx+" prodConsolMap"+prodConsolMap.keySet());
				theQtxWork = this.createQtxWorkObj(qualtx, 0, prodConsolMap, qualtxComp.prod_key);
				if (null != bomConsolMap)
				{
					QTXConsolWork qtxConsolWork = bomConsolMap.get(qualtx.src_key);
					if (null != qtxConsolWork) theQtxWork.priority = qtxConsolWork.priority;
				}
				theQtxCompWork = this.createQtxCompWorkObj(qualtx, qualtxComp, 0, theQtxWork.qtx_wid);
				theQtxCompWork.time_stamp = theQtxWork.time_stamp;
				theQtxCompWork.status.time_stamp = theQtxWork.time_stamp;
				theQtxWork.compWorkList.add(theQtxCompWork);

				if (reasonCode == ReQualificationReasonCodes.GPM_CTRY_CMPL_CHANGE || reasonCode == ReQualificationReasonCodes.GPM_CTRY_CMPL_DELETED)
				{
					theQtxCompHsWork = this.createQtxCompHSWorkObj(qualtxComp, workCode, theQtxCompWork.qtx_comp_wid, theQtxCompWork.qtx_wid);
					theQtxCompHsWork.time_stamp = theQtxWork.time_stamp;
					theQtxCompWork.compWorkHSList.add(theQtxCompHsWork);
				}
				else if (reasonCode == ReQualificationReasonCodes.GPM_SRC_ADDED || reasonCode == ReQualificationReasonCodes.GPM_SRC_DELETED || reasonCode == ReQualificationReasonCodes.GPM_SRC_CHANGE || reasonCode == ReQualificationReasonCodes.GPM_SRC_IVA_DELETED || reasonCode == ReQualificationReasonCodes.GPM_COMP_FINAL_DECISION_CHANGE || reasonCode == ReQualificationReasonCodes.GPM_IVA_CHANGE_M_I || reasonCode == ReQualificationReasonCodes.GPM_COMP_PREV_YEAR_QUAL_CHANGE || reasonCode == ReQualificationReasonCodes.GPM_COMP_PREV_YEAR_QUAL_CHANGE || reasonCode == ReQualificationReasonCodes.GPM_COMP_TRACE_VALUE_CHANGE)
				{
					theQtxCompIvaWork = this.createQtxCompIVAWorkObj(qualtxComp, workCode, theQtxCompWork.qtx_comp_wid, theQtxCompWork.qtx_wid);
					theQtxCompIvaWork.time_stamp = theQtxWork.time_stamp;
					theQtxCompWork.compWorkIVAList.add(theQtxCompIvaWork);
				}
				else
				{
					theQtxCompWork.setReasonCodeFlag(workCode);
				}
			}

			qtxWorkList.put(qualtx.alt_key_qualtx, theQtxWork);
		}
	}

	private boolean isRequalificationRequired(String criticalIndicator, boolean isHsChange) 
	{
		if(isHsChange)
		{
			return criticalIndicator.equals("") || criticalIndicator.equals(RequalificationWorkCodes.CRITICAL_HS) || criticalIndicator.equals(RequalificationWorkCodes.CRITICAL_ANY);
		}
		else
		{
			return criticalIndicator.equals("") || criticalIndicator.equals(RequalificationWorkCodes.CRITICAL_QUALIFIED) || criticalIndicator.equals(RequalificationWorkCodes.CRITICAL_QUALIFIED_NO_SHIFT) || criticalIndicator.equals(RequalificationWorkCodes.CRITICAL_ANY);	
		}
		
	}

	private boolean isValidQualtx(QualTX qualtx, boolean isBOMChange, Timestamp bestTime)
	{
		if (qualtx.src_key == null || qualtx.iva_code == null || qualtx.fta_code == null || qualtx.ctry_of_import == null || (qualtx.is_active != null && (qualtx.is_active).equalsIgnoreCase("N"))) return false;
		if(new Timestamp(qualtx.created_date.getTime()).after(bestTime)) return false;
		
		// Commenting the code as Re-qualification flag check is only applicable for GPM updates not for Bom updates.
/*		if(!isBOMChange) return true;
		if (qualtx.fta_code != null && qualtx.ctry_of_import != null)
		{

			try
			{

				TradeLane aTradeLane = getTradeLane(qualtx.org_code, qualtx.fta_code, qualtx.ctry_of_import);

				if (aTradeLane != null)
				{
					TradeLaneContainer laneContainer = this.qeConfigCache.getQEConfig(qualtx.org_code).getTradeLaneContainer();
					if (laneContainer != null)
					{
						TradeLaneData laneData = laneContainer.getTradeLaneData(aTradeLane);
						if (null != laneData && !laneData.isRequalification()) return false;

					}
				}
			}
			catch (Exception e)
			{
				MessageFormatter.error(logger, "isValidQualtx", e, " Error while getting requalification flag value from QEconfig for qualtx : [{0}] ", qualtx.alt_key_qualtx);

			}

		}
*/
		return true;
	}

	private boolean isValidQualtxComp(QualTXComponent qualtxComp)
	{
		boolean isValid = true;
		
		if(qualtxComp != null && qualtxComp.src_id != null)
		{
			if((qualtxComp.src_id).contains("~")) 
				isValid = false;
		}
		
		return isValid;
	}

	private void processMassQualificationWork(Entry<Long, QtxStageData> entry, Map<Long, QTXWork> consolidatedWork, Timestamp bestTime) throws Exception
	{
		try{
			long reasonCode = entry.getKey();

			QtxStageData stageData = entry.getValue();

			List<QualTX> qualtxList = this.utility.getImpactedQtxKeysForMass(stageData.altKeylist, reasonCode, stageData.agreementList);
			
			this.createArQtxBomCompBean(qualtxList, consolidatedWork, reasonCode, true, bomConsolMap, false, bestTime);
		
		}catch(Exception e){
			
			logger.error("Failed to update qualtx for mass qualification - issuing rollback", e);

			throw new Exception("Failed to update qualtx for mass qualification - issuing rollback", e);
		}
	}

	private void processMassRequalWork(QTXStage stageData, JSONObject workData)
	{
		if (workData.isNull("MASS_QUALIFICATION")) return;

		JSONObject theMassQualDtlsObj = workData.getJSONObject("MASS_QUALIFICATION");
		JSONArray theBomDtls = theMassQualDtlsObj.optJSONArray("BOM_DTLS");
		if (null != theBomDtls)
		{
			ArrayList<Long> bomKeyList = null;
			bomKeyList = new ArrayList<>();
			for (int index = 0; index < theBomDtls.length(); index++)
			{
				JSONObject theBomDtl = theBomDtls.getJSONObject(index);
				bomKeyList.add(theBomDtl.optLong("BOM_KEY"));
				
				QTXConsolWork qtxConsolWork = bomConsolMap.get(theBomDtl.optLong("BOM_KEY"));
				if(qtxConsolWork == null)
				{
					qtxConsolWork = new QTXConsolWork();
					qtxConsolWork.user_id = stageData.user_id;
					qtxConsolWork.priority= stageData.priority;
					qtxConsolWork.time_stamp = stageData.time_stamp;
					this.addToBOMConsollMap(theBomDtl.optLong("BOM_KEY"), qtxConsolWork);
				}
				else
				{
					qtxConsolWork.priority = (qtxConsolWork.priority < stageData.priority) ? stageData.priority : qtxConsolWork.priority;
				}

			}

			QtxStageData stageDatabean = new QtxStageData();
			String agreementCodes = theMassQualDtlsObj.optString("AGREEMENT_CODES");

			if (null != agreementCodes && !agreementCodes.isEmpty())
			{
				ArrayList<String> ftaList = new ArrayList<String>(Arrays.asList(agreementCodes.split(",")));
			
				stageDatabean.agreementList = ftaList;
			}
			stageDatabean.altKeylist = bomKeyList;
			this.totalStageSize+=bomKeyList.size();

			configRequalMap.put(ReQualificationReasonCodes.BOM_MASS_QUALIFICATION, stageDatabean);
		}
	}
	
//	private TradeLane getTradeLane(String orgCode, String ftaCode, String coi) throws Exception
//	{
//		List<TradeLane> tradeLaneList = this.qeConfigCache.getQEConfig(orgCode).getTradeLaneList();
//		if (tradeLaneList != null)
//		{
//			Optional<TradeLane> optTradeLane = tradeLaneList.stream().filter(p -> p.getFtaCode().equalsIgnoreCase(ftaCode) && (p.getCtryOfImport().equalsIgnoreCase(coi) || p.getCtryOfImport().equalsIgnoreCase("ALL"))).findFirst();
//			if (optTradeLane.isPresent()) return optTradeLane.get();
//		}
//		return null;
//	}

	private void updateQualtxToInactive(List<Long> qualtxList) throws Exception
	{

		String sql = "update mdi_qualtx set is_active='N' where alt_key_qualtx=?";

		try
		{
			 template.batchUpdate(sql, qualtxList, this.batchSize, new ParameterizedPreparedStatementSetter<Long>()
			{

				@Override
				public void setValues(PreparedStatement ps, Long argument) throws SQLException
				{
					ps.setLong(1, argument.longValue());
				}
			});
			 
		}
		catch (Exception e)
		{
			logger.error("Failed to update qualtx for mass qualification - issuing rollback", e);

			throw new Exception("Failed to update qualtx for mass qualification - issuing rollback", e);

		}

	}

	private QTXWork createQtxWorkObj(QualTX qualtx, long reasonCode, Map<Long, QTXConsolWork> bomConsolMap, long key) throws Exception
	{
		this.totalWorkSize++;
		return this.utility.createQtxWorkObj(qualtx, reasonCode, bomConsolMap, key);
	}

	private QTXCompWork createQtxCompWorkObj(QualTX qualtx, QualTXComponent qualtxComp, long reasonCode, long workId) throws Exception
	{
		this.totalWorkSize++;
		return this.utility.createQtxCompWorkObj(qualtx, qualtxComp, reasonCode, workId);
	}
	
	private QTXCompWorkIVA createQtxCompIVAWorkObj(QualTXComponent qualtxComp, long reasonCode, long compWorkId, long workId) throws Exception
	{
		this.totalWorkSize++;
		return this.utility.createQtxCompIVAWorkObj(qualtxComp, reasonCode, compWorkId, workId);
	}
	
	private QTXCompWorkHS createQtxCompHSWorkObj(QualTXComponent qualtxComp, long reasonCode, long compWorkId, long workId) throws Exception
	{
		this.totalWorkSize++;
		return this.utility.createQtxCompHSWorkObj(qualtxComp, reasonCode, compWorkId, workId);
	}
	
	private QTXWorkHS createQtxHSWorkObj(QualTX qualtx, long reasonCode, long workId) throws Exception
	{
		this.totalWorkSize++;
		return this.utility.createQtxHSWorkObj(qualtx, reasonCode, workId);
	}
	
//	public void init(int threads, int readAhead, int fetchSize, int batchSize, int sleepInterval, MDIQualTxRepository qualTxRepository, QTXWorkRepository workRepository, UniversalObjectIDGenerator idGenerator, BOMUniverse bomUniverse, QEConfigCache qeConfigCache) throws WorkManagementException
//	{
//		super.init(threads, readAhead, fetchSize, batchSize, sleepInterval, qualTxRepository, workRepository, idGenerator);
//		
//		this.utility = new ArQtxWorkUtility(this.getWorkRepository(), this.template, bomUniverse, qeConfigCache);
//		this.qeConfigCache = qeConfigCache;
//	}
	
//	public void setAPI(GetCacheRefreshInformationClientAPI cacheRefreshInformationClientAPI, TrackerClientAPI trackerClientAPI)
//	{
//		this.cacheRefreshInformationClientAPI = cacheRefreshInformationClientAPI;
//		this.trackerClientAPI = trackerClientAPI;
//	}

//	@Override
//	protected void findWork() throws Exception
//	{
//	}
	
//	private int updateStageRecordsStatus(List<QTXStage> stageList, QTXStageStatus status) throws SQLException
//	{
//		String sql = "update ar_qtx_stage set status=?,time_stamp=? where qtx_sid=?"; //Double check the time-stamp updation
//		
//		PerformanceTracker tracker = new PerformanceTracker(logger, Level.INFO, "updateWorkRecordsToPending");
//		int rowsAffected = 0;
//		
//		tracker.start();
//		try
//		{
//			int updateMatrix[][] = template.batchUpdate(sql, stageList, this.batchSize, new ParameterizedPreparedStatementSetter<QTXStage>() {
//					private Timestamp now = new Timestamp(System.currentTimeMillis());
//					
//					@Override
//					public void setValues(PreparedStatement ps, QTXStage argument) throws SQLException {
//						ps.setInt(1, status.ordinal());
//						ps.setTimestamp(2, this.now);
//						ps.setLong(3, argument.qtx_sid);
//					}
//				  });
//			
//			for (int updateList[] : updateMatrix)
//			{
//				for (int updateCount : updateList)
//					rowsAffected = rowsAffected + updateCount;
//			}
//		}
//		finally
//		{
//			tracker.stop("ar_qtx_stage records affected = {0}" , new Object[]{rowsAffected});
//		}
//		
//		return rowsAffected;
//	}

//	//TODO API needs to include searching by a specific org_code (or company_code)
//	//TODO only allow one company code request to be run at a time, different company codes can run concurrently
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
//			stageList = this.template.query("select ar_qtx_stage.qtx_sid, ar_qtx_stage.user_id, ar_qtx_stage_data.data, ar_qtx_stage.time_stamp, ar_qtx_stage.priority, ar_qtx_stage.batch_id from ar_qtx_stage left join ar_qtx_stage_data on ar_qtx_stage.qtx_sid=ar_qtx_stage_data.qtx_sid where ar_qtx_stage.time_stamp<=?  and status=? order by ar_qtx_stage.time_stamp", new Object[]{new java.sql.Timestamp(bestTime), QTXStageStatus.INIT.ordinal()}, extractor);
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
//	this.getConsolidatedQualtxForBomUpdate(bomRequalMap, consolidatedWork, bomConsolMap, bestTime);
//	this.getConsolidatedQualtxForProdUpdate(prodRequalMap, consolidatedWork, newCtryCmpMap, prodConsolMap, bestTime);
//	this.getConsolidatedQualtxForContentUpdate(contentRequalList, consolidatedWork, bomConsolMap, bestTime);
//
//	return consolidatedWork;
//}

//public void executeFindWork() throws Exception
//{
//	CacheRefreshInformation cacheInfo = this.cacheRefreshInformationClientAPI.execute();
//	Timestamp bestTime = new Timestamp(cacheInfo.cacheLoadStart);
//	List<QTXStage> stageWorkListQual = new ArrayList<>();
//	logger.debug("QTXStageProducer: Requalification loading work as of " + bestTime);
//
//	List<QTXStage> stageList = null;
//	TransactionStatus tranStatus = this.txMgr.getTransaction(new DefaultTransactionDefinition());
//
//	try
//	{
//
//		stageList = this.loadStageWork(cacheInfo.cacheLoadStart);
//
//		stageWorkListQual = processMassQualificationWorks(stageList);
//
//		this.txMgr.commit(tranStatus);
//
//	}catch(Exception e)
//	{
//		logger.error("Failed to expand work stage data - issuing rollback", e);
//
//		this.txMgr.rollback(tranStatus);
//
//		throw new Exception("Failed to expand work stage data - issuing rollback", e);
//
//	}
//	
//	TransactionStatus status = this.txMgr.getTransaction(new DefaultTransactionDefinition());
//	try
//	{			
//		Map<Long, QTXWork> consolidatedWork = this.generateWorkFromStagedData(stageWorkListQual, bestTime);
//		
//		logger.debug("generation complete");
//
//		this.updateStageRecordsStatus(stageList, QTXStageStatus.COMPLETED);
//		
//		logger.debug("stage updated to complete staged work = " + consolidatedWork.values());
//		
//		this.getWorkRepository().storeWork(consolidatedWork.values());
//
//		logger.debug("work stored");
//		
//		this.txMgr.commit(status);
//		
//		logger.debug("transaction commited");
//
//		this.updateTrackerStatus(consolidatedWork.values());
//	}
//	catch (Exception e)
//	{
//		logger.error("Failed to process work stage data - issuing rollback", e);
//		
//		this.txMgr.rollback(status);
//		
//		throw new Exception("Failed to process work stage data - issuing rollback", e);
//	}
//}
}