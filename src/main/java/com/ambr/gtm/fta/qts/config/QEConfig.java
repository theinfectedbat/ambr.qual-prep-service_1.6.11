package com.ambr.gtm.fta.qts.config;

import java.util.ArrayList;
import java.util.List;

import com.ambr.gtm.fta.qts.util.BomAnalysisConfigData;
import com.ambr.gtm.fta.qts.util.CompAnalysisConfData;
import com.ambr.gtm.fta.qts.util.CumulationConfigContainer;
import com.ambr.gtm.fta.qts.util.SubPullConfigContainer;
import com.ambr.gtm.fta.qts.util.TradeLane;
import com.ambr.gtm.fta.qts.util.TradeLaneContainer;
import com.ambr.gtm.fta.trade.FTACountryContainer;

public class QEConfig
{

	TradeLaneContainer			tradeLaneContainer			= new TradeLaneContainer();
	BomAnalysisConfigData		bomAnalysisConfigData		= new BomAnalysisConfigData();
	SubPullConfigContainer		subPullConfigContainer		= new SubPullConfigContainer();
	CumulationConfigContainer	cumulationConfigContainer	= new CumulationConfigContainer();
	List<CompAnalysisConfData>	compAnalysisConfDataList	= new ArrayList<CompAnalysisConfData>();
	FTACountryContainer			ftaCountryContainer			= new FTACountryContainer();

	public QEConfig()
	{

	}

	public TradeLaneContainer getTradeLaneContainer()
	{
		return tradeLaneContainer;
	}

	public void setTradeLaneContainer(TradeLaneContainer tradeLaneContainer)
	{
		this.tradeLaneContainer = tradeLaneContainer;
	}

	public BomAnalysisConfigData getBomAnalysisConfigData()
	{
		return bomAnalysisConfigData;
	}

	public void setBomAnalysisConfigData(BomAnalysisConfigData bomAnalysisConfigData)
	{
		this.bomAnalysisConfigData = bomAnalysisConfigData;
	}

	public SubPullConfigContainer getSubPullConfigContainer()
	{
		return subPullConfigContainer;
	}

	public void setSubPullConfigContainer(SubPullConfigContainer subPullConfigContainer)
	{
		this.subPullConfigContainer = subPullConfigContainer;
	}

	public CumulationConfigContainer getCumulationConfigContainer()
	{
		return cumulationConfigContainer;
	}

	public void setCumulationConfigContainer(CumulationConfigContainer cumulationConfigContainer)
	{
		this.cumulationConfigContainer = cumulationConfigContainer;
	}

	public List<CompAnalysisConfData> getCompAnalysisConfDataList()
	{
		return compAnalysisConfDataList;
	}

	public void setCompAnalysisConfDataList(List<CompAnalysisConfData> compAnalysisConfDataList)
	{
		this.compAnalysisConfDataList = compAnalysisConfDataList;
	}

	public FTACountryContainer getFtaCountryContainer()
	{
		return ftaCountryContainer;
	}

	public void setFtaCountryContainer(FTACountryContainer ftaCountryContainer)
	{
		this.ftaCountryContainer = ftaCountryContainer;
	}
	
	
	public synchronized List<TradeLane> getTradeLaneList() throws Exception
	{
		return this.tradeLaneContainer != null?this.tradeLaneContainer.getAllTradeLaneList() : null;
	}


	public synchronized List<TradeLane> getSubpullConfigList() throws Exception
	{
		return this.subPullConfigContainer != null ? this.subPullConfigContainer.getAllTradeLaneList(): null;
	}

	public CumulationConfigContainer getCumulationConfig()
	{
		return this.cumulationConfigContainer;
	}
	
	public String getAnalysisMethod() throws Exception
	{
		String analysisMethod =  this.bomAnalysisConfigData != null ? this.bomAnalysisConfigData.getAnalysisMethod() : null;
		if (analysisMethod == null) analysisMethod= TrackerCodes.AnalysisMethodFromConfig.TOP_DOWN.name();
		return analysisMethod;
	}
	
	
	public BomAnalysisConfigData getAnalysisConfig() throws Exception
	{
		return this.bomAnalysisConfigData;
	}


}
