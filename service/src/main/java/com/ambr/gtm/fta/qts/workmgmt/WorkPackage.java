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
	
	public boolean headerProcessed = false;
	private int childCompletedCount = 0;

	public Long lockId;
	public Exception failure;
	
	public BOM bom;
	public GPMClassificationProductContainer gpmClassificationProductContainer;
	
	public boolean deleteBOMQual = false;
	
	private WorkPackage linkedPackage = null;
	private WorkPackage rootPackage = null;
	
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
	
	public int getLinkedPackageLength()
	{
		int length = 0;
		
		WorkPackage workPackage = this.linkedPackage;
		while (workPackage != null)
		{
			length++;
			workPackage = workPackage.linkedPackage;
		}
		
		return length;
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
	
	public void compWorkCompleted(CompWorkPackage compWorkPackage)
	{
		this.childCompletedCount++;
	}
	
	public boolean isWorkComplete()
	{
		return headerProcessed && (this.childCompletedCount == this.compWorks.size());
	}
	
	public boolean isChainComplete()
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
