package com.ambr.gtm.fta.qps.qualtx.engine.api;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class CacheRefreshInformation 
{
	public long		cacheLoadStart;
	public long		cacheLoadComplete;
	public String	cacheLoadStartAsText;
	public String	cacheLoadCompleteAsText;
	public String	timezone;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public CacheRefreshInformation()
		throws Exception
	{
		this.timezone = TimeZone.getDefault().getDisplayName();
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theTimestamp
     * @param	theTimestamp
     *************************************************************************************
     */
	public CacheRefreshInformation(long theStartTime, long theCompleteTime)
			throws Exception
	{
		this.cacheLoadStart = theStartTime;
		this.cacheLoadComplete = theCompleteTime;
		this.timezone = TimeZone.getDefault().getDisplayName();
		
		this.cacheLoadStartAsText = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS").format(new Timestamp(this.cacheLoadStart));
		this.cacheLoadCompleteAsText = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS").format(new Timestamp(this.cacheLoadComplete));
	}
}
