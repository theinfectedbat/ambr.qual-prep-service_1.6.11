package com.ambr.gtm.fta.qps.qualtx.engine.result;

import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;

import javax.sql.DataSource;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.gtm.fta.qps.qualtx.engine.PreparationEngineQueueUniverse;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTX;
import com.ambr.gtm.fta.qps.qualtx.engine.TypedPersistenceQueue;
import com.ambr.gtm.fta.qps.qualtx.exception.BOMNotCurrentlyTrackedException;
import com.ambr.gtm.fta.qps.qualtx.exception.TradeLaneNotCurrentlyTrackedException;
import com.ambr.platform.uoid.UniversalObjectIDGenerator;
import com.ambr.platform.utils.cache.CacheManagerService;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.platform.utils.propertyresolver.ConfigurationPropertyResolver;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class BOMStatusManager 
{
	public static Logger					logger = LogManager.getLogger(BOMStatusManager.class);

	private String		name;

	public static final String				BOM_STATUS_CACHE_SIZE_HEAP 	= "com.ambr.gtm.fta.qps.status_cache.bom.heap";
	public static final String				BOM_STATUS_CACHE_SIZE_DISK 	= "com.ambr.gtm.fta.qps.status_cache.bom.disk";
	
	private Cache<Long, BOMStatusTracker>	bomInProgressCache;
	private Cache<Long, BOMStatusTracker>	bomCompletedCache;

	private Cache<Long, TradeLaneStatusTracker>	tradeLaneInProgressCache;
	private Cache<Long, TradeLaneStatusTracker>	tradeLaneCompletedCache;
	
	private CacheManagerService								cacheMgrService;
	private ConfigurationPropertyResolver					propertyResolver;
	private PreparationEngineQueueUniverse					queueUniverse;
	private Date 											endTime;
	private Date 											startTime;
	private long											trackedBOMCount;
	private long											trackedTradeLaneCount;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theName
     * @param	theCacheMgrService
     * @param	thePropertyResolver
     * @param	theIDGenerator
     * @param	theDataSrc
     * @param	theLogEntryQueue
     *************************************************************************************
     */
	public BOMStatusManager(
		String 											theName,
		CacheManagerService								theCacheMgrService, 
		ConfigurationPropertyResolver 					thePropertyResolver,
		PreparationEngineQueueUniverse					theQueueUniverse)
		throws Exception
	{
		this.name = theName;

		this.cacheMgrService = theCacheMgrService;
		this.propertyResolver = thePropertyResolver;
		this.queueUniverse = theQueueUniverse;
		
		int aBOMCountInHeap = this.propertyResolver.getPropertyValueAsInteger(BOM_STATUS_CACHE_SIZE_HEAP, Integer.MAX_VALUE);
		int aBOMSizeOnDiskInGB = this.propertyResolver.getPropertyValueAsInteger(BOM_STATUS_CACHE_SIZE_DISK, 100);
		
		this.bomInProgressCache = this.cacheMgrService.getCacheManager().createCache(
			MessageFormat.format("BOM-{0}-in_progress", this.name), 
			CacheConfigurationBuilder.newCacheConfigurationBuilder(
				Long.class, 
				BOMStatusTracker.class, 
				ResourcePoolsBuilder.newResourcePoolsBuilder()
					.heap(aBOMCountInHeap, EntryUnit.ENTRIES)
					.build()
			)
		);

		this.initializeBOMCompletedCache(aBOMSizeOnDiskInGB);

		this.tradeLaneInProgressCache = this.cacheMgrService.getCacheManager().createCache(
			MessageFormat.format("TRADE_LANE-{0}-in_progress", this.name), 
			CacheConfigurationBuilder.newCacheConfigurationBuilder(
				Long.class, 
				TradeLaneStatusTracker.class, 
				ResourcePoolsBuilder.newResourcePoolsBuilder()
					.heap(aBOMCountInHeap, EntryUnit.ENTRIES)
					.build()
			)
		);

		this.initializeTradeLaneCompletedCache(aBOMSizeOnDiskInGB);
		
		MessageFormatter.info(logger, "constructor", "Cache [{0}]: heap object count [{1}] disk size [{2}]", 
			this.name, 
			aBOMCountInHeap, 
			aBOMSizeOnDiskInGB
		);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theBOMTracker
	 *************************************************************************************
	 */
	public void bomCompleted(BOMStatusTracker theBOMTracker)
		throws Exception
	{
		this.bomInProgressCache.remove(theBOMTracker.alt_key_bom);
		this.bomCompletedCache.put(theBOMTracker.alt_key_bom, theBOMTracker);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theBOM
	 *************************************************************************************
	 */
	public BOMStatusTracker bomStarted(BOM theBOM)
		throws Exception
	{
		BOMStatusTracker		aTracker;
		
		aTracker = new BOMStatusTracker(theBOM);
		aTracker.setManager(this);
		
		this.bomInProgressCache.put(aTracker.alt_key_bom, aTracker);
		this.trackedBOMCount++;
		return aTracker;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public void clear()
		throws Exception
	{
		this.trackedBOMCount = 0;
		this.trackedTradeLaneCount = 0;
		this.bomInProgressCache.clear();
		this.bomCompletedCache.clear();
		this.tradeLaneInProgressCache.clear();
		this.tradeLaneCompletedCache.clear();
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public ArrayList<BOMStatusTracker> getAllBOMStatusTrackers()
		throws Exception
	{
		ArrayList<BOMStatusTracker>		aList;
		
		aList = new ArrayList<BOMStatusTracker>();
		
		this.bomInProgressCache.forEach(aEntry->{aList.add(aEntry.getValue());});
		this.bomCompletedCache.forEach(aEntry->{aList.add(aEntry.getValue());});
	
		return aList;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theInProgressFlag	Indicates whether to ONLY return in progress results
	 *************************************************************************************
	 */
	public ArrayList<RecordOperationStatus> getAllRecordOperationStatuses(boolean theInProgressFlag)
		throws Exception
	{
		ArrayList<RecordOperationStatus>		aList = new ArrayList<>();
		
		this.tradeLaneInProgressCache.forEach(aEntry->{aList.addAll(aEntry.getValue().recOperationList);});
		
		if (!theInProgressFlag) {
			this.tradeLaneCompletedCache.forEach(aEntry->{aList.addAll(aEntry.getValue().recOperationList);});
		}
	
		return aList;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theBOMKey
	 *************************************************************************************
	 */
	public BOMStatusTracker getBOMTracker(long theBOMKey)
		throws Exception
	{
		BOMStatusTracker aTracker;
		
		aTracker = this.bomInProgressCache.get(theBOMKey);
		if (aTracker == null) {
			aTracker = this.bomCompletedCache.get(theBOMKey);
		}
		
		if (aTracker == null) {
			throw new BOMNotCurrentlyTrackedException(theBOMKey);
		}
		
		return aTracker;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public long getDuration()
		throws Exception
	{
		if (this.startTime == null) {
			throw new IllegalStateException("Processing has not started");
		}
		
		if (this.endTime == null) {
			return System.currentTimeMillis() - this.startTime.getTime();
		}
		else {
			return this.endTime.getTime() - this.startTime.getTime();
		}
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public String getDurationText()
		throws Exception
	{
		return DurationFormatUtils.formatDurationHMS(this.getDuration());
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public synchronized Date getEndTime()
		throws Exception
	{
		return this.endTime;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public synchronized Date getStartTime()
		throws Exception
	{
		return this.startTime;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public long getTrackedBOMCount()
		throws Exception
	{
		return this.trackedBOMCount;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQualTXKey
	 *************************************************************************************
	 */
	public TradeLaneStatusTracker getTradeLaneTracker(Long theQualTXKey)
		throws Exception
	{
		TradeLaneStatusTracker aTracker;
		
		aTracker = this.tradeLaneInProgressCache.get(theQualTXKey);
		if (aTracker == null) {
			aTracker = this.tradeLaneCompletedCache.get(theQualTXKey);
		}
		
		if (aTracker == null) {
			throw new TradeLaneNotCurrentlyTrackedException(theQualTXKey);
		}
		
		return aTracker;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theSizeInGB
	 *************************************************************************************
	 */
	private void initializeBOMCompletedCache(int theSizeInGB)
		throws Exception
	{
		int aMaxAttempts = 3;
		int aSleepIntervalInSecs = 1;
		
		for (int aAttemptCount = 1; aAttemptCount <= aMaxAttempts; aAttemptCount++) {
			try {
				this.bomCompletedCache = this.cacheMgrService.getCacheManager().createCache(
					MessageFormat.format("BOM-{0}-completed", this.name), 
					CacheConfigurationBuilder.newCacheConfigurationBuilder(
						Long.class, 
						BOMStatusTracker.class, 
						ResourcePoolsBuilder.newResourcePoolsBuilder()
							.disk(theSizeInGB, MemoryUnit.GB, false)
							.build()
					)
				);
				
				return;
			}
			catch (Exception e) {
				MessageFormatter.error(logger, "initializedBOMCompleted", e, "Attempt [{0}] of [{1}]: failed to initialize cache. Sleeping for [{2}] secs", aAttemptCount, aMaxAttempts, aSleepIntervalInSecs);
				Thread.sleep(aSleepIntervalInSecs * 1000);
			}
		}
		
		// We weren't able to successfully use the current cache manager.  Let's try to create a new cache 
		// manager.  If it fails this time, we will fail the process.
		
		MessageFormatter.info(logger, "initializeBOMCompletedCache", "Failed to initialize cache. Creating new cache manager instance");
		
		CacheManager	aCacheMgr = this.cacheMgrService.getCacheManager(true);

		this.bomCompletedCache = this.cacheMgrService.getCacheManager().createCache(
			MessageFormat.format("BOM-{0}-completed", this.name), 
			CacheConfigurationBuilder.newCacheConfigurationBuilder(
				Long.class, 
				BOMStatusTracker.class, 
				ResourcePoolsBuilder.newResourcePoolsBuilder()
					.disk(theSizeInGB, MemoryUnit.GB, false)
					.build()
			)
		);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theSizeInGB
	 *************************************************************************************
	 */
	private void initializeTradeLaneCompletedCache(int theSizeInGB)
		throws Exception
	{
		int aMaxAttempts = 3;
		int aSleepIntervalInSecs = 1;

		for (int aAttemptCount = 1; aAttemptCount <= aMaxAttempts; aAttemptCount++) {
			try {
				this.tradeLaneCompletedCache = this.cacheMgrService.getCacheManager().createCache(
					MessageFormat.format("TRADE_LANE-{0}-completed", this.name), 
					CacheConfigurationBuilder.newCacheConfigurationBuilder(
						Long.class, 
						TradeLaneStatusTracker.class, 
						ResourcePoolsBuilder.newResourcePoolsBuilder()
							.disk(theSizeInGB, MemoryUnit.GB, false)
							.build()
					)
				);
				
				return;
			}
			catch (Exception e) {
				MessageFormatter.error(logger, "initializeTradeLaneCompletedCache", e, "Attempt [{0}] of [{1}]: failed to initialize cache. Sleeping for [{2}] secs", aAttemptCount, aMaxAttempts, aSleepIntervalInSecs);
				Thread.sleep(aSleepIntervalInSecs * 1000);
			}
		}
		
		// We weren't able to successfully use the current cache manager.  Let's try to create a new cache 
		// manager.  If it fails this time, we will fail the process.
		
		MessageFormatter.info(logger, "initializeTradeLaneCompletedCache", "Failed to initialize cache. Creating new cache manager instance");
		CacheManager	aCacheMgr = this.cacheMgrService.getCacheManager(true);
	
		this.tradeLaneCompletedCache = this.cacheMgrService.getCacheManager().createCache(
			MessageFormat.format("TRADE_LANE-{0}-completed", this.name), 
			CacheConfigurationBuilder.newCacheConfigurationBuilder(
				Long.class, 
				TradeLaneStatusTracker.class, 
				ResourcePoolsBuilder.newResourcePoolsBuilder()
					.disk(theSizeInGB, MemoryUnit.GB, false)
					.build()
			)
		);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public synchronized boolean isInProgress()
		throws Exception
	{
		return (this.startTime != null) && (this.endTime == null);
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public void persistFailures()
		throws Exception
	{
		String			aSqlText;
		String			aPrepInstanceID;
		JdbcTemplate	aTemplate = new JdbcTemplate(this.queueUniverse.dataSrc);

		aPrepInstanceID = this.queueUniverse.idGenerator.generate().getEncodedID();
		
		aSqlText = "insert into ar_qtxprep_log (prep_instance_id, start_time, end_time, description) values (?,?,?,?)";
		aTemplate.update(aSqlText, aPrepInstanceID, this.startTime, this.endTime, this.name);
		
		this.tradeLaneCompletedCache.forEach(
			aTradeLaneEntry->
			{
				TradeLaneStatusTracker aTracker = aTradeLaneEntry.getValue();
				
				aTracker.recOperationList.forEach(
					aOperationEntry->
					{
						QualTXPrepLogDtlEntry	aLogEntry;
						
						try {
							if (!aOperationEntry.isSuccess()) {
								aLogEntry = new QualTXPrepLogDtlEntry();
								aLogEntry.prep_instance_id = aPrepInstanceID;
								aLogEntry.detail_instance_id = this.queueUniverse.idGenerator.generate().getEncodedID();
								aLogEntry.alt_key_bom = aTracker.alt_key_bom;
								aLogEntry.alt_key_qualtx = aTracker.alt_key_qualtx;
								aLogEntry.created_date = aTracker.getEndTime();
								aLogEntry.operation = aOperationEntry.operation.ordinal();
								aLogEntry.record_id = aOperationEntry.recordID;
								aLogEntry.record_type = aOperationEntry.recType.ordinal();
								aLogEntry.message = aOperationEntry.failureExceptionText;
								this.queueUniverse.qualTXPrepLogQueue.put(aLogEntry);
							}
						}
						catch (Exception e) {
							MessageFormatter.error(logger, "persisteFailures", e, "BOM [{0}]: error persisting failure detail log entry", aTracker.alt_key_bom);
						}
					}
				);
			}
		);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	@JsonIgnore
	public synchronized void setEndTime()
		throws Exception
	{
		this.endTime = new Timestamp(System.currentTimeMillis());
		this.persistFailures();
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public void setStartTime()
		throws Exception
	{
		this.startTime = new Timestamp(System.currentTimeMillis());
		this.endTime = null;
		this.clear();
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theTracker
	 *************************************************************************************
	 */
	public void tradeLaneCompleted(TradeLaneStatusTracker theTracker)
		throws Exception
	{
		this.tradeLaneInProgressCache.remove(theTracker.alt_key_qualtx);
		this.tradeLaneCompletedCache.put(theTracker.alt_key_qualtx, theTracker);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public TradeLaneStatusTracker tradeLaneStarted(QualTX theQualTX)
		throws Exception
	{
		TradeLaneStatusTracker	aTracker;
		
		aTracker = new TradeLaneStatusTracker(theQualTX);
		aTracker.setManager(this);
		
		this.tradeLaneInProgressCache.put(aTracker.alt_key_qualtx, aTracker);
		this.trackedTradeLaneCount++;
		return aTracker;
	}
}
