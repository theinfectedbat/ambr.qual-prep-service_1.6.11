package com.ambr.gtm.fta.qts;

public class QTSProperties
{
	public static final String	MAX_FETCH_SIZE						= "com.ambr.gtm.fta.qts.datasource.fetch_size";
	public static final String	TRACKER_SERVICE_START				= "com.ambr.gtm.fta.qts.tracker_service_start";
	public static final String	TRACKER_SERVICE_URL					= "com.ambr.gtm.fta.qts.api.TrackerClientAPI.serviceURL";
	public static final String	TA_SERVICE_URL						= "com.ambr.gtm.fta.qts.api.ta.serviceURL";
	public static final String	PREP_SERVICE_URL					= "com.ambr.gtm.fta.qts.api.qtxprep.serviceURL";
	public static final String	QTX_OBSERVER_THREAD_INTERVAL		= "com.ambr.gtm.fta.qts.observer.QtxStatusObserver.thread_interval";
	public static final String	BOM_OBSERVER_THREAD_INTERVAL		= "com.ambr.gtm.fta.qts.observer.BOMTrackerStatusObserver.thread_interval";
	public static final String	TRACKER_GARBAGE_THREAD_INTERVAL		= "com.ambr.gtm.fta.qts.observer.TrackerGarbageCollector.thread_interval";
	public static final String	QTX_RELOAD_THREAD_INTERVAL			= "com.ambr.gtm.fta.qts.observer.QtxStatusObserver.reload_thread_interval";
	public static final String	REQUAL_SERVICE_START				= "com.ambr.gtm.fta.qts.requal_service_start";
	public static final String	FTA_CTRY_DG_CACHE					= "com.ambr.gtm.fta.qts.FTACtryConfigCache.load_required";
	public static final String	LOAD_FTA_CTRY_DG_USING_TA_SERVICE	= "com.ambr.gtm.fta.qts.FTACtryConfigCache.load_using_service";
	public static final String	LOAD_QE_CONFIG_CACHE				= "com.ambr.gtm.fta.qts.config.QEConfigCache.load_required";
	public static final String	TRADE_SERVICE_CLIENT_KEY			= "com.ambr.gtm.fta.trade.client.TradeQualtxClient.service_client_key";
	public static final String	TRACKER_RELOAD_SIZE_LIMIT			= "com.ambr.gtm.fta.qts.trackerLoader.reloadSize";
}

