package com.ambr.gtm.fta.qps.qualtx.engine.result;

import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;

import com.ambr.gtm.fta.qps.qualtx.engine.PreparationEngineQueueUniverse;
import com.ambr.gtm.fta.qps.qualtx.engine.request.QualTXUniverseGenerationRequest;
import com.ambr.platform.utils.cache.CacheManagerService;
import com.ambr.platform.utils.misc.ParameterizedMessageUtility;
import com.ambr.platform.utils.propertyresolver.ConfigurationPropertyResolver;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class QualTXUniversePreparationProgressManager 
{
	PreparationEngineQueueUniverse				queueUniverse;
	private ConfigurationPropertyResolver		propertyResolver;
	private CacheManagerService					cacheMgrService;
	private QualTXUniverseGenerationRequest		request;
	private BOMStatusManager					bomTopDownStatusMgr;
	private BOMStatusManager					bomRawMaterialAndOrIntermediateStatusMgr;
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQueueUniverse
	 * @param	theCacheMgr
	 * @param	thePropertyResolver
	 *************************************************************************************
	 */
	public QualTXUniversePreparationProgressManager(
		PreparationEngineQueueUniverse 	theQueueUniverse, 
		CacheManagerService				theCacheMgrService, 
		ConfigurationPropertyResolver 	thePropertyResolver)
		throws Exception
	{
		this.cacheMgrService = theCacheMgrService;
		this.queueUniverse = theQueueUniverse;
		this.propertyResolver = thePropertyResolver;
		this.bomTopDownStatusMgr = new BOMStatusManager("TopDownMethod", this.cacheMgrService, this.propertyResolver, this.queueUniverse);
		this.bomRawMaterialAndOrIntermediateStatusMgr = new BOMStatusManager("RawMaterialAndOrIntermediateMethod", this.cacheMgrService, this.propertyResolver, this.queueUniverse);
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public synchronized void clearResults()
		throws Exception
	{
		this.request = null;
		this.bomTopDownStatusMgr.clear();
		this.bomRawMaterialAndOrIntermediateStatusMgr.clear();
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theMsgUtil
	 *************************************************************************************
	 */
	public String getBOMStatus(long theBOMKey)
		throws Exception
	{
		BOMStatusGenerator	aStatusGenerator;
		
		aStatusGenerator = new BOMStatusGenerator(this, theBOMKey);
		return aStatusGenerator.generate();
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public synchronized BOMStatusManager getStatusManager()
		throws Exception
	{
		BOMStatusManager aStatusMgr;
		
		aStatusMgr = this.getStatusManager(true);
		if (aStatusMgr == null) {
			throw new IllegalStateException("No BOM Status manager is currently in progress");
		}
		
		return aStatusMgr;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	BOMStatusManager getStatusManager(boolean theInProgressFlag)
		throws Exception
	{
		if (this.bomTopDownStatusMgr.isInProgress()) {
			return this.bomTopDownStatusMgr;
		}
		else if (this.bomRawMaterialAndOrIntermediateStatusMgr.isInProgress()) {
			return this.bomRawMaterialAndOrIntermediateStatusMgr;
		}
		else if (!theInProgressFlag) {
			return this.bomTopDownStatusMgr;
		}
		else {
			return null;
		}
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theMsgUtil
	 *************************************************************************************
	 */
	void getTrackerStatusSummary(ParameterizedMessageUtility theMsgUtil)
		throws Exception
	{
		BOMStatusManager		aStatusMgr = this.getStatusManager(false);
		
		Date aTime = new Timestamp(System.currentTimeMillis());
				
		theMsgUtil.format("Start Time    [{0,date} {0,time}]", false, true, aStatusMgr.getStartTime());
		if (aStatusMgr.getEndTime() == null) {
			theMsgUtil.format("End Time      [IN PROGRESS]", false, true);
		}
		else {
			theMsgUtil.format("End Time      [{0,date} {0,time}]", false, true, aStatusMgr.getEndTime());
		}
		
		theMsgUtil.format("Current Time  [{0,date} {0,time}]", false, true, aTime);
		theMsgUtil.format("Duration      [{0}]", false, true, aStatusMgr.getDurationText());
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public synchronized String getTrackerStatusStatistics()
		throws Exception
	{
		Integer										aValue;
		ParameterizedMessageUtility					aUtil = new ParameterizedMessageUtility();
		HashMap<String, HashMap<String, Integer>>	aOperationMap = new HashMap<>();
		HashMap<String, Integer>					aStatisticMap;
		String										aStatName;
		HashMap<String, Integer>					aEntityCountMap = new HashMap<>();
		String										aMaxValueText;
		BOMStatusManager							aStatusMgr = this.getStatusManager(true);
		
		if (aStatusMgr == null) {
			return "Tracker Status Unavailable";
		}
		
		this.getTrackerStatusSummary(aUtil);
		
		for (RecordOperationStatus aRecOpStatus : aStatusMgr.getAllRecordOperationStatuses(true)) {
			
			aStatisticMap = aOperationMap.get(aRecOpStatus.operation.name());
			if (aStatisticMap == null) {
				aStatisticMap = new HashMap<>();
				aOperationMap.put(aRecOpStatus.operation.name(),  aStatisticMap);
			}

			aStatName = aRecOpStatus.isSuccess()? "SUCCESS" : "FAILURE";
			aValue = aStatisticMap.get(aStatName);
			if (aValue == null) {
				aValue = 0;
			}
			
			aValue++;
			aStatisticMap.put(aStatName, aValue);
			
			aValue = aEntityCountMap.get(aRecOpStatus.recType.name());
			if (aValue == null) {
				aValue = 0;
			}
			aValue++;
			aEntityCountMap.put(aRecOpStatus.recType.name(), aValue);
		}

		int aMaxEntityName = aEntityCountMap.keySet().stream().mapToInt(name->name.length()).max().getAsInt();

		aMaxValueText = MessageFormat.format(
			"{0}", 
			aEntityCountMap.values()
				.stream()
				.mapToInt(aIntObj->aIntObj.intValue())
				.max()
				.getAsInt()
		);
		
		for (String aEntityName: aEntityCountMap.keySet()) {
			String aValueText = MessageFormat.format("{0}", aEntityCountMap.get(aEntityName));

			aUtil.format("Entity    [{0}{1}]: Operation Count [{2}{3}]", false, true, 
				aEntityName, 
				aUtil.createPaddingString(aEntityName, ' ', aMaxEntityName),
				aUtil.createPaddingString(aValueText, ' ', aMaxValueText.length()),
				aValueText);
		}
		
		int aMaxOperationName = aOperationMap.keySet().stream().mapToInt(name->name.length()).max().getAsInt();
		
		aMaxValueText = MessageFormat.format(
			"{0}", 
			aOperationMap.values()
				.stream()
				.mapToInt(
					(HashMap<String, Integer> aMap)->
						aMap.values()
						.stream()
						.mapToInt(aIntObj->aIntObj.intValue())
						.max()
						.getAsInt()
				)
				.max()
				.getAsInt()
			);
		
		for (String aOperationName: aOperationMap.keySet()) {
			aStatisticMap = aOperationMap.get(aOperationName);
			
			String aSuccessCount;
			String aFailureCount;
			
			aSuccessCount = MessageFormat.format("{0}", aStatisticMap.get("SUCCESS"));
			aFailureCount = MessageFormat.format("{0}", aStatisticMap.get("FAILURE"));
			
			aUtil.format("Operation [{0}{1}]: SUCCESS [{2}{3}] FAILURE [{4}{5}]", false, true, 
				aOperationName, 
				aUtil.createPaddingString(aOperationName, ' ', aMaxOperationName),
				aUtil.createPaddingString(aSuccessCount, ' ', aMaxValueText.length()),
				aSuccessCount, 
				aUtil.createPaddingString(aFailureCount, ' ', aMaxValueText.length()),
				aFailureCount
			);
		}
		
		return aUtil.getMessage();
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public synchronized String getTrackerUniverseSummary(TrackerUniverseSummaryOptions theOptions)
		throws Exception
	{
		ArrayList<BOMStatusTracker>			aList;
		ArrayList<TradeLaneStatusTracker>	aTradeLaneList;
		ParameterizedMessageUtility			aUtil = new ParameterizedMessageUtility();
		BOMStatusManager					aStatusMgr = this.getStatusManager(true);
	
		if (aStatusMgr == null) {
			return "Tracker Status Unavailable";
		}
	
		this.getTrackerStatusSummary(aUtil);
		
		aList = aStatusMgr.getAllBOMStatusTrackers();
		aList.sort(
			new Comparator<BOMStatusTracker>()
			{
				@Override
				public int compare(BOMStatusTracker theBOM1, BOMStatusTracker theBOM2) 
				{
					if (theBOM1.alt_key_bom < theBOM2.alt_key_bom) {
						return -1;
					}
					else if (theBOM1.alt_key_bom > theBOM2.alt_key_bom) {
						return 1;
					}
					else {
						return 0;
					}
				}
			}
		);
		
		int aMaxBOMIDTextLength = aList.stream().mapToInt(bt->MessageFormat.format("{0,number,#}/{1}", bt.alt_key_bom, bt.bom_id).length()).max().getAsInt();
		int aMaxORGLength = aList.stream().mapToInt(bt->bt.org_code.length()).max().getAsInt();
		for (BOMStatusTracker aBOMTracker : aList) {
			String aBOMIDText = MessageFormat.format("{0,number,#}/{1}", aBOMTracker.alt_key_bom, aBOMTracker.bom_id);
			
			aTradeLaneList = aBOMTracker.getAllTradeLaneStatusTrackers();
			
			aUtil.format("BOM [{0}{1}] ORG [{2}{3}] Components [{4}] QTX Count [{5}] [{6,time}] [{7,time}] [{8}]", false, true,
				aBOMIDText,
				aUtil.createPaddingString(aBOMIDText, ' ', aMaxBOMIDTextLength),
				aBOMTracker.org_code,
				aUtil.createPaddingString(aBOMTracker.org_code, ' ', aMaxORGLength),
				aBOMTracker.componentCount,
				aTradeLaneList.size(),
				aBOMTracker.getStartTime(),
				aBOMTracker.getEndTime(),
				aBOMTracker.getDurationText()
			);
			
			if (aBOMTracker.getEndTime() != null) {
				continue;
			}
			
			if (aTradeLaneList.size() == 0) {
				continue;
			}
			
			int aMaxQtxKeyLength = aTradeLaneList.stream().mapToInt(qtxt->MessageFormat.format("{0,number,#}", qtxt.alt_key_qualtx).length()).max().getAsInt();
			int aMaxFTACodeLength = aTradeLaneList.stream().mapToInt(qtxt->qtxt.getFTACode().length()).max().getAsInt();
			for (TradeLaneStatusTracker aTracker : aTradeLaneList) {
				String	aQtxKeyText = MessageFormat.format("{0,number,#}",aTracker.alt_key_qualtx);
				
				aUtil.format("  QTX [{0}{1}] FTA [{2}{3}] COI [{4}] [{5,time}] [{6,time}] [{7}]", false, true,
					aQtxKeyText,
					aUtil.createPaddingString(aQtxKeyText, ' ', aMaxQtxKeyLength),
					aTracker.fta_code,
					aUtil.createPaddingString(aTracker.fta_code, ' ', aMaxFTACodeLength),
					aTracker.getCtryOfImport(),
					aBOMTracker.getStartTime(),
					aBOMTracker.getEndTime(),
					aBOMTracker.getDurationText()
				);
			}
		}
		
		return aUtil.getMessage();
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
		return 
			this.bomTopDownStatusMgr.isInProgress() ||
			this.bomRawMaterialAndOrIntermediateStatusMgr.isInProgress()
		;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public synchronized void prepareToStart()
		throws Exception
	{
		this.prepareToStart(new QualTXUniverseGenerationRequest());
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theRequest
	 *************************************************************************************
	 */
	public synchronized void prepareToStart(QualTXUniverseGenerationRequest theRequest)
		throws Exception
	{
		this.request = theRequest;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public synchronized void rawMaterialAndOrIntermediateAnalysisStart()
		throws Exception
	{
		this.bomRawMaterialAndOrIntermediateStatusMgr.setStartTime();
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public synchronized void rawMaterialAndOrIntermediateAnalysisComplete()
		throws Exception
	{
		this.bomRawMaterialAndOrIntermediateStatusMgr.setEndTime();
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public synchronized void topDownAnalysisStart()
		throws Exception
	{
		this.bomTopDownStatusMgr.setStartTime();
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public synchronized void topDownAnalysisComplete()
		throws Exception
	{
		this.bomTopDownStatusMgr.setEndTime();
	}
}
