package com.ambr.gtm.fta.qts.util;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.ambr.gtm.fta.qts.api.TrackerClientAPI;
import com.ambr.gtm.fta.trade.client.TradeQualtxClient;

public class Env
{
    private static Env _env;

	private DataSource dataSource;

	private TrackerClientAPI trackerAPI;
	private TradeQualtxClient aTradeQualtxClient;
	
	public Env(DataSource dataSource, String trackerURL, String taServiceURL) throws Exception
	{
		this.dataSource = dataSource;

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
	
	public Connection getPoolConnection() throws SQLException
	{
		Connection connection = this.dataSource.getConnection();
		
		connection.setAutoCommit(false);
		
		return connection;
	}
	
	public void releasePoolConnection(Connection connection) throws SQLException
	{
		if (connection != null)
			connection.close();
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