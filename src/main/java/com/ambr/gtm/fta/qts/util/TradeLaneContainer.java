package com.ambr.gtm.fta.qts.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TradeLaneContainer
{
	private String orgCode;
	public String getOrgCode()
	{
		return orgCode;
	}

	public void setOrgCode(String orgCode)
	{
		this.orgCode = orgCode;
	}

	Map<TradeLane, TradeLaneData> tradeLaneMap = new HashMap<>();
	
	public TradeLaneData getTradeLaneData(TradeLane tradeLane) throws Exception {
		return tradeLaneMap.get(tradeLane);
	}
	
	public List<TradeLane> getAllTradeLaneList() throws Exception
	{
		List<TradeLane> tradeLaneList = new ArrayList<TradeLane>();
		tradeLaneList.addAll(tradeLaneMap.keySet());
		return tradeLaneList;
	}
	
	public void addTradelane(TradeLane tradeLane, TradeLaneData tradeLaneData)
	{
		tradeLaneMap.put(tradeLane, tradeLaneData);
	}
}
