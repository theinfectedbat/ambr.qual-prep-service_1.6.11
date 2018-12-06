package com.ambr.gtm.fta.qts;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ambr.gtm.fta.qts.container.TrackerContainer;
import com.ambr.gtm.fta.qts.util.Env;
import com.ambr.gtm.fta.qts.work.BOMEligibility;
import com.ambr.gtm.fta.qts.work.QtxWorkInfo;
import com.ambr.gtm.fta.trade.BOMQualtxData;
import com.ambr.platform.utils.log.MessageFormatter;

public class BOMTracker
{

	static Logger									logger					= LogManager.getLogger(BOMTracker.class);
	public long										bomKey;
	private static TrackerCodes.QualTrackerStatus	QUALIFICATION_FAILED	= TrackerCodes.QualTrackerStatus.QUALIFICATION_FAILED;
	private static TrackerCodes.QualTrackerStatus	QUALIFICATION_COMPLETE	= TrackerCodes.QualTrackerStatus.QUALIFICATION_COMPLETE;
	private Set<QtxTracker>							qtxTrackerList			= new HashSet<QtxTracker>();
	private TrackerContainer						trackerContainer;
	private QTXWorkRepository						workRepository;
	private int										priority;

	
	public BOMTracker(long bomKey, QTXWorkRepository workRepository, TrackerContainer theContaner)
	{
		this.workRepository = workRepository;
		this.trackerContainer = theContaner;
		this.bomKey = bomKey;
		this.priority= -1;
	}
	

	public Set<QtxTracker> getQtxTrackerList()
	{
		return qtxTrackerList;
	}

	
	public int getPriority()
	{
		return priority;
	}


	public void setPriority(int priority)
	{
		this.priority = priority;
	}


	public QtxTracker addQualtx(QtxWorkInfo qtxWork) throws Exception
	{
		QtxTracker qtxTracker = null;
		Long aQtxKey = qtxWork.getQtxKey();
		Long aBomKey = qtxWork.getBomKey();
		if (aBomKey != null && aQtxKey != null)
		{
			try
			{
				qtxTracker = trackerContainer.getQtxTracker(aQtxKey);
				if (qtxTracker != null && !qtxTrackerList.contains(qtxTracker))
				{
					//qtxTracker.isEligibleForDelete = false;
					synchronized (qtxTrackerList)
					{
						qtxTrackerList.add(qtxTracker);
					}
				}
			}
			catch (Exception e)
			{
				MessageFormatter.error(logger, "addQualtx",e, "Exception while adding Qualtx record into BOM Tracker, BOM key [{0}] , QTX Key: [{1}]",aBomKey,aQtxKey);
			}
		}
		return qtxTracker;
	}

	public void triggerPostBOMValidationPolicy(JdbcTemplate aTemplate) throws Exception
	{
		BOMEligibility eligibility = isEligibleToTriggerPostBOMValidationPolicy();
		if (eligibility.isEligibleforPostPolicyTrigger && !eligibility.getEligibleQtxList().isEmpty())
		{
			BOMQualtxData bomQualtxData = new BOMQualtxData();
			bomQualtxData.bomkey = this.bomKey;
			if (this.priority >= 0) bomQualtxData.priority = this.priority;
			List<QtxTracker> eligibleQtxList = eligibility.getEligibleQtxList();
			Set<QtxWorkTracker> eligibleQtxWorkList = new HashSet<>();
			for (QtxTracker eligibleQtx : eligibleQtxList)
			{
				bomQualtxData.qualtxKeyList.add(eligibleQtx.getQualtxKey());
				for (QtxWorkTracker aQtxWorkTracker : eligibleQtx.getQtxQualCompletedWorkList())
				{
					bomQualtxData.qtxWorkIdList.add(aQtxWorkTracker.getQualtxWorkId());
					eligibleQtxWorkList.add(aQtxWorkTracker);
				}
			}
			if (!bomQualtxData.qualtxKeyList.isEmpty() && !bomQualtxData.qtxWorkIdList.isEmpty())
			{
				if (postBomQualUpdateWork(bomQualtxData))
				{
					if (updateQtxWorkStatus(bomQualtxData.qtxWorkIdList))
					{
						
						for (QtxTracker qtxTracker : eligibleQtxList)
						{
							qtxTracker.isEligibleForDelete = true;
							qtxTracker.clearQtxCompltedWorkTracker();
						}
						
						for (QtxWorkTracker qtxWorkTracker : eligibleQtxWorkList)
						{
							qtxWorkTracker.isEligibleForDelete = true;
						}
						
						
					}
				}
			}

		}
	}
	public boolean updateQtxWorkStatus(List<Long> qtxWorkList)
	{
		if (qtxWorkList != null)
		{
			MessageFormatter.debug(logger, "updateQtxWorkStatus", "Qualtx work update started for BOM Post policy posted status update.");
			List<QTXWorkStatus> qtxStatusList = new ArrayList<QTXWorkStatus>();
			for (Long workId : qtxWorkList)
			{
				QTXWorkStatus workStatus = new QTXWorkStatus();
				workStatus.qtx_wid = workId;
				workStatus.status = TrackerCodes.QualtxStatus.BOM_POST_POLICY_WORK_POSTED;
				qtxStatusList.add(workStatus);
			}
			try
			{
				this.workRepository.updateWorkStatus(qtxStatusList);
				return true;
			}
			catch (Exception e)
			{
				MessageFormatter.error(logger, "updateQtxWorkStatus", e,"Exception updating the qtxwork status as BOM Post policy work posted");
			}
		}
		return false;
	}
	
	public boolean postBomQualUpdateWork(BOMQualtxData bomQualtxData)
	{
		try
		{
			String aWorkId = Env.getSingleton().getTradeQualtxClient().createWorkForBOMQualUpdate(bomQualtxData);
			
			//TODO Remove this code after testing
			if (logger.isDebugEnabled())
			{
				MessageFormatter.debug(logger, "postBomQualUpdateWork", "Sent data for BOM Key :[{0}] ",bomQualtxData.bomkey);
				for (Long keyList : bomQualtxData.qualtxKeyList)
				{
					MessageFormatter.debug(logger, "postBomQualUpdateWork", "Qualtx keys: [{0}] ",keyList);
				}
			}
			
			
			MessageFormatter.debug(logger, "postBomQualUpdateWork", "Posted work for BOM Post policy work id :: [{0}] ",aWorkId);
			
			if (aWorkId != null) return true;
		}
		catch (Exception exe)
		{
			MessageFormatter.error(logger, "postBomQualUpdateWork",exe, "Exception while posting BOM Qual work");
		}
		return false;
	}

	public BOMEligibility isEligibleToTriggerPostBOMValidationPolicy() throws Exception
	{
		BOMEligibility eligibility = new BOMEligibility();
		if (this.qtxTrackerList.isEmpty())
		{
			MessageFormatter.debug(logger, "isEligibleToTriggerPostBOMValidationPolicy", "Qtx tracker List is empty for BOM Key :[{0}] ", this.bomKey);
			return eligibility;
		}
		eligibility.isEligibleforPostPolicyTrigger = true;
		synchronized (qtxTrackerList)
		{
			for (QtxTracker qtxTracker : this.qtxTrackerList)
			{
				if(qtxTracker.isEligibleForDelete) continue;
				TrackerCodes.QualTrackerStatus aQtxStatus = qtxTracker.getQtxStatus();
				if (!QUALIFICATION_FAILED.equals(aQtxStatus) && !QUALIFICATION_COMPLETE.equals(aQtxStatus))
				{
					eligibility.isEligibleforPostPolicyTrigger = false;
					MessageFormatter.debug(logger, "isEligibleToTriggerPostBOMValidationPolicy", "BOM Key :[{0}] is not eligible for post validation as Qtx key :[{1}] status is :[{2}] ", this.bomKey, qtxTracker.getQualtxKey(), aQtxStatus);
					break;

				}
				else if (QUALIFICATION_COMPLETE.equals(aQtxStatus))
				{
					eligibility.addEligibleQtxList(qtxTracker);
				}
				else eligibility.addProcessedQtxList(qtxTracker);
			}
		}
		return eligibility;
	}

	public void deleteQtxTrackers(Set<QtxTracker> qtxTrackerSet) throws Exception
	{
		if (qtxTrackerSet != null && qtxTrackerSet.size() > 0)
		{
			synchronized (qtxTrackerList)
			{
				qtxTrackerList.removeAll(qtxTrackerSet);
				
			}

		}
	}

}
