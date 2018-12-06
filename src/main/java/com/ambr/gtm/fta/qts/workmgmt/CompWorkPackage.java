package com.ambr.gtm.fta.qts.workmgmt;

import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationProductContainer;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAProductContainer;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponent;
import com.ambr.gtm.fta.qts.QTXCompWork;
import com.ambr.platform.rdbms.orm.EntityManager;

public class CompWorkPackage
{
	private WorkPackage parent;
	
	public QTXCompWork compWork;
	public EntityManager<QualTXComponent> entityMgr;
	public QualTXComponent qualtxComp;
	public GPMClassificationProductContainer gpmClassificationProductContainer;
	public GPMSourceIVAProductContainer gpmSourceIVAProductContainer;
	
	public Exception failure;
	
	public CompWorkPackage(WorkPackage parent)
	{
		this.parent = parent;
	}
	
	public WorkPackage getParentWorkPackage()
	{
		return this.parent;
	}
}
