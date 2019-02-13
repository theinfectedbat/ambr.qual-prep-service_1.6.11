package com.ambr.gtm.fta.qts.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SubPullConfigContainer {
	
	private String orgCode;
	public String getOrgCode()
	{
		return orgCode;
	}

	public void setOrgCode(String orgCode)
	{
		this.orgCode = orgCode;
	}

	Map<TradeLane, SubPullConfigData> subpullMap = new HashMap<>();
	
	public SubPullConfigData getTradeLaneData(TradeLane tradeLane) throws Exception {
		return subpullMap.get(tradeLane);
	}
	
	public List<TradeLane> getAllTradeLaneList() throws Exception
	{
		List<TradeLane> tradeLaneList = new ArrayList<TradeLane>();
		tradeLaneList.addAll(subpullMap.keySet());
		return tradeLaneList;
	}
	
	public void addTradelane(TradeLane tradeLane, SubPullConfigData subPullConfigData)
	{
		subpullMap.put(tradeLane, subPullConfigData);
	}
}
