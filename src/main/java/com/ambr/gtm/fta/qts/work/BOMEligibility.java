package com.ambr.gtm.fta.qts.work;

import java.util.ArrayList;
import java.util.List;

import com.ambr.gtm.fta.qts.QtxTracker;

public class BOMEligibility
{

	public boolean		isEligibleforPostPolicyTrigger;
	private List<QtxTracker>	eligibleQtxList	= new ArrayList<QtxTracker>();
	private List<QtxTracker>	processedQtxList	= new ArrayList<QtxTracker>();

	public List<QtxTracker> getEligibleQtxList()
	{
		return eligibleQtxList;
	}

	public void addEligibleQtxList(QtxTracker qtxTracker)
	{
		this.eligibleQtxList.add(qtxTracker);
	}

	public List<QtxTracker> getProcessedQtxList()
	{
		return processedQtxList;
	}

	public void addProcessedQtxList(QtxTracker processedQtx)
	{
		this.processedQtxList.add(processedQtx);
	}
	

}
