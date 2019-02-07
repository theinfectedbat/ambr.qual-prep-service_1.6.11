package com.ambr.gtm.fta.qts.workmgmt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationProductContainer;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTX;
import com.ambr.gtm.fta.qts.QTXWork;
import com.ambr.platform.rdbms.orm.EntityManager;

public class WorkPackage
{
	public QTXWork work;

	public QualTX qualtx;
	public EntityManager<QualTX> entityMgr;

	public HashMap<Long, CompWorkPackage> compWorks = new HashMap<Long, CompWorkPackage>();
	public List<WorkPackage> mergedPackages = new ArrayList<WorkPackage>();
	
	private boolean headerProcessed = false;
	private int childCompletedCount = 0;

	//lockId is only set on rootPackage (lock only acquired once)
	private Long lockId;
	
	public Exception failure;
	
	public BOM bom;
	public GPMClassificationProductContainer gpmClassificationProductContainer;
	
	public boolean deleteBOMQual = false;
	public boolean isReadyForQualification = true;
	
	private WorkPackage linkedPackage = null;
	private WorkPackage rootPackage = null;
	
	public enum PackageState
	{
		INCOMPLETE,
		PACKAGE_COMPLETE,
		CHAIN_COMPLETE
	}
	
	public WorkPackage()
	{
	}
	
	public void setEntityManager(EntityManager<QualTX> entityMgr)
	{
		WorkPackage next = this;
		
		while(next != null)
		{
			next.entityMgr = entityMgr;
			
			next = next.linkedPackage;
		}
	}
	
	public Long getLockId()
	{
		if (this.rootPackage != null)
			return this.rootPackage.lockId;
		else
			return this.lockId;
	}
	
	public void setLockId(Long lockId)
	{
		if (this.rootPackage != null)
			this.rootPackage.lockId = lockId;
		else
			this.lockId = lockId;
	}
	
	public void setBOM(BOM bom)
	{
		this.bom = bom;
	}
	
	public void appendLinkedPackage(WorkPackage workPackage)
	{
		workPackage.rootPackage = this.getRootPackage();
		
		if (this.linkedPackage == null)
		{
			this.linkedPackage = workPackage;
			return;
		}
		
		WorkPackage currentPackage = this.linkedPackage;
		while (currentPackage.linkedPackage != null)
			currentPackage = currentPackage.linkedPackage;
		
		currentPackage.linkedPackage = workPackage;
	}
	
	public WorkPackage getLinkedPackage()
	{
		return this.linkedPackage;
	}
	
	public WorkPackage getRootPackage()
	{
		if (this.rootPackage != null)
			return this.rootPackage;
		else
			return this;
	}
	
	public Exception getFailure()
	{
		if (this.failure != null)
			return this.failure;
		
		for (CompWorkPackage compWorkPackage : this.compWorks.values())
		{
			if (compWorkPackage.failure != null)
				return compWorkPackage.failure;
		}
		
		return null;
	}
	
	/*
	 * This method is synchronized since multiple threads can register work completed concurrently.
	 * The boolean returned lets the producers know a piece of work is complete and can continue by persisting the work
	 * or processing the next linkedPackage.
	 * Key goal is to make sure two threads cannot update the package AND detect it is complete at the same time,
	 * otherwise the work could be submitted twice for persistence or double submission for linkedPackage.
	 */
	public synchronized PackageState compWorkCompleted(CompWorkPackage compWorkPackage)
	{
		this.childCompletedCount++;
		
		return this.getPackageState();
	}
	
	public synchronized PackageState headerWorkCompleted()
	{
		this.headerProcessed = true;
		
		return this.getPackageState();
	}
	
	private PackageState getPackageState()
	{
		if (this.isChainComplete())
			return PackageState.CHAIN_COMPLETE;
		else if (this.isWorkComplete())
			return PackageState.PACKAGE_COMPLETE;
		else
			return PackageState.INCOMPLETE;
	}
	
	private boolean isWorkComplete()
	{
		return headerProcessed && (this.childCompletedCount == this.compWorks.size());
	}
	
	private boolean isChainComplete()
	{
		WorkPackage start = this.getRootPackage();

		while (start != null)
		{
			if (start.isWorkComplete() == false) return false;
			
			start = start.linkedPackage;
		}
		
		return true;
	}
	
	public void addCompWorkPackage(CompWorkPackage compWorkPackage)
	{
		this.compWorks.put(compWorkPackage.compWork.qtx_comp_wid, compWorkPackage);
	}
	
	public CompWorkPackage getCompWorkPackage(long qtxCompWid)
	{
		return this.compWorks.get(qtxCompWid);
	}
}
