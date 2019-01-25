package com.ambr.gtm.fta.qps.qualtx.engine.result;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTX;
import com.ambr.platform.utils.log.MessageFormatter;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class BOMStatusTracker
	implements Serializable
{
	private static Logger							logger = LogManager.getLogger(BOMStatusTracker.class);
	private static final long 						serialVersionUID = 1L;

	public long 							alt_key_bom;
	public String							bom_id;
	public String							org_code;
	public BOMStatusEnum					status;
	public ArrayList<Long>					tradeLaneKeyList;
	public ArrayList<String>				failedTradeLaneList; 
	private Date							startTime;
	private Date							endTime;
	public long								componentCount;
	private transient BOMStatusManager		mgr;
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public BOMStatusTracker()
		throws Exception
	{
		this.tradeLaneKeyList = new ArrayList<>();
	}

    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBOM
     *************************************************************************************
     */
	public BOMStatusTracker(BOM theBOM)
		throws Exception
	{
		this();
		
		this.alt_key_bom = theBOM.alt_key_bom;
		this.bom_id = theBOM.bom_id;
		this.org_code = theBOM.org_code;
		this.componentCount = theBOM.componentCount;
		this.status = BOMStatusEnum.INIT;
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public synchronized ArrayList<Long> getAllTradeLaneKeys()
		throws Exception
	{
		return new ArrayList<Long>(this.tradeLaneKeyList);
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * Returns all of the Trade Lane status trackers for this BOM
	 * </P>
	 *************************************************************************************
	 */
	public ArrayList<TradeLaneStatusTracker> getAllTradeLaneStatusTrackers()
		throws Exception
	{
		ArrayList<TradeLaneStatusTracker>	aList = new ArrayList<>();
		
		this.tradeLaneKeyList.forEach(
			aQualTXKey->
			{
				try {
					aList.add(this.mgr.getTradeLaneTracker(aQualTXKey));
				}
				catch (Exception e) {
					MessageFormatter.error(logger, "getAllTradeLaneStatusTrackers", e, "BOM [{0}]: failed to get status tracker for [{1}]", this.alt_key_bom, aQualTXKey);
				}
			}
		);
		
		return aList;
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
			throw new IllegalStateException(MessageFormat.format("BOM [{0}]: processing has not started", this.alt_key_bom));
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
	public Date getEndTime()
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
	public Date getStartTime()
		throws Exception
	{
		return this.startTime;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	thePrevQualTXKey
	 * @param	theNewQualTXKey
	 *************************************************************************************
	 */
	public synchronized void replaceTradeLaneID(long thePrevQualTXKey, long theNewQualTXKey)
		throws Exception
	{
		this.tradeLaneKeyList.remove(thePrevQualTXKey);
		this.tradeLaneKeyList.add(theNewQualTXKey);
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public void setEndTime()
		throws Exception
	{
		this.endTime = new Timestamp(System.currentTimeMillis());
		this.status = BOMStatusEnum.PROCESSING_COMPLETE;
		if (this.mgr != null) {
			this.mgr.bomCompleted(this);
		}
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theMgr
     *************************************************************************************
     */
	void setManager(BOMStatusManager theMgr)
		throws Exception
	{
		this.mgr = theMgr;
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
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theFTACode
	 * @param	theCtryOfImport
	 *************************************************************************************
	 */
	public void setTradeLaneFailed(String theFTACode, String theCtryOfImport)
		throws Exception
	{
		String	aTradeLaneID = MessageFormat.format("{0}.{1}", theFTACode, theCtryOfImport);
		
		this.failedTradeLaneList.add(aTradeLaneID);
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theQualTX
     *************************************************************************************
     */
	public TradeLaneStatusTracker trackTradeLane(QualTX theQualTX)
		throws Exception
	{
		TradeLaneStatusTracker	aTracker;
		
		aTracker = this.mgr.tradeLaneStarted(theQualTX);
		
		synchronized (this) {
			this.tradeLaneKeyList.add(aTracker.alt_key_qualtx);
		}
		
		return aTracker;
	}
}
