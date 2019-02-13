package com.ambr.gtm.fta.qps.qualtx.engine.result;

public class BOMTradeLaneStatusTracker 
{
	public long							bomKey;
	public long							qualTXKey;
	public String						ftaCode;
	public String						coi;
	public boolean						existsFlag;
	public boolean						deletedFlag;
	public boolean						upToDateFlag;
	public BOMTradeLaneStatusEnum		status;
	public boolean						topDownCompleteFlag;
	
	public BOMTradeLaneStatusTracker(long theBOMKey, String theFTACode, String theCOI)
		throws Exception
	{
		this.bomKey = theBOMKey;
		this.ftaCode = theFTACode;
		this.coi = theCOI;
	}

}
