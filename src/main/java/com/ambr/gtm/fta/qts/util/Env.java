package com.ambr.gtm.fta.qts.util;

import com.ambr.gtm.fta.qts.api.TrackerClientAPI;
import com.ambr.gtm.fta.trade.client.TradeQualtxClient;

public class Env
{
    private static Env _env;

	private TrackerClientAPI trackerAPI;
	private TradeQualtxClient aTradeQualtxClient;
	
	public Env(String trackerURL, String taServiceURL) throws Exception
	{
		this.trackerAPI = new TrackerClientAPI(trackerURL);
		this.aTradeQualtxClient = new TradeQualtxClient(taServiceURL);
	}
	
	public void setSingleton(Env env)
	{
		Env._env = env;
	}
	
	public static Env getSingleton()
	{
		return Env._env;
	}
	
	public TrackerClientAPI getTrackerAPI() throws Exception
	{
		return this.trackerAPI;
	}
	
	public TradeQualtxClient getTradeQualtxClient() throws Exception
	{
		return this.aTradeQualtxClient;
	}
	
}