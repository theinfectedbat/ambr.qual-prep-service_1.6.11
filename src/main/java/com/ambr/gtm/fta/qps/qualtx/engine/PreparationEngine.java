package com.ambr.gtm.fta.qps.qualtx.engine;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.gtm.fta.qps.bom.BOMMetricSetUniverseContainer;
import com.ambr.gtm.fta.qps.bom.BOMUniverse;
import com.ambr.gtm.fta.qps.bom.PrioritizedBOMMetricSet;
import com.ambr.gtm.fta.qps.qualtx.engine.request.QualTXUniverseGenerationRequest;
import com.ambr.gtm.fta.qps.qualtx.engine.result.QualTXUniversePreparationProgressManager;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.platform.utils.log.PerformanceTracker;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class PreparationEngine 
{
	static Logger						logger = LogManager.getLogger(PreparationEngine.class);
	
	@Autowired	private PreparationEngineQueueUniverse 					queueUniverse;
	@Autowired	private QualTXUniversePreparationProgressManager		qtxPrepProgressMgr;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public PreparationEngine()
		throws Exception
	{
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public void generateUniverse()
		throws Exception
	{
		this.generateUniverse(null);
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theRequest
     *************************************************************************************
     */
	public void generateUniverse(QualTXUniverseGenerationRequest theRequest)
		throws Exception
	{
		PerformanceTracker					aPerfTracker = new PerformanceTracker(logger, Level.INFO, "generateUniverse");
		
		if (this.qtxPrepProgressMgr.isInProgress()) {
			MessageFormatter.info(logger, "generateUniverse", "Processing currently in progress.");
			throw new IllegalStateException("Universe Preparation is currently in progress");
		}
		
		try {
			aPerfTracker.start();
			
			this.qtxPrepProgressMgr.prepareToStart(theRequest);
			this.queueUniverse.initialize(theRequest);
			
			this.topDownAnalysis();
			
			// TODO:  verify need to do this
			this.queueUniverse.qtxDetailUniverse.refresh();
			
			this.rawMaterialAndOrIntermediateAnalysis();
			
		}
		finally {

			aPerfTracker.stop("BOMs [{0}]; BOM Components [{1}]; QTXs [{2}]; QTX Components [{3}]; Classifications [{4}]; IVA Pulls [{5}]", 
				new Object[] {
					this.queueUniverse.getBOMCount(), 
					this.queueUniverse.getBOMComponent(),
					this.queueUniverse.tradeLaneQueue.getCompletedWorkCount(),
					this.queueUniverse.compQueue.getCompletedWorkCount(),
					this.queueUniverse.classificationQueue.getCompletedWorkCount(),
					this.queueUniverse.compIVAPullQueue.getCompletedWorkCount()
				}
			);
		}
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	private void topDownAnalysis()
		throws Exception
	{
		PerformanceTracker				aPerfTracker = new PerformanceTracker(logger, Level.INFO, "topDownAnalysis");
		BOM								aBOM;
		BOMMetricSetUniverseContainer	aBOMSet;
		BOMUniverse						aBOMUniverse = this.queueUniverse.getBOMUniverse();
	
		this.qtxPrepProgressMgr.topDownAnalysisStart();
		try {
			aPerfTracker.start();
			aBOMSet = aBOMUniverse.getPrioritizedBOMSet();
			for (PrioritizedBOMMetricSet aBOMMetricSet : aBOMSet.metricSetList) {
				try {
					if (!this.queueUniverse.request.isBOMEnabled(aBOMMetricSet.metricSet.bomKey)) {
						MessageFormatter.trace(logger, "topDownAnalysis", "BOM [{0,number,#}]: not enabled for processing.", aBOMMetricSet.metricSet.bomKey);
						continue;
					}
					aBOM = aBOMUniverse.getBOM(aBOMMetricSet.metricSet.bomKey);
					this.queueUniverse.processBOM(aBOM);
				}
				catch (Exception e) {
					MessageFormatter.error(logger, "topDownAnalysis", e, "BOM [{0,number,#}]: failed to process.", aBOMMetricSet.metricSet.bomKey);
				}
			}
			
			this.queueUniverse.waitForCompletion();
		}
		catch (Exception e)
		{
			MessageFormatter.error(logger, "topDownAnalysis", e, "unexpected error");
			throw e;
		}
		finally {
			this.qtxPrepProgressMgr.topDownAnalysisComplete();
	
			aPerfTracker.stop("BOMs [{0}]; BOM Components [{1}]; QTXs [{2}]; QTX Components [{3}]; Classifications [{4}]; IVA Pulls [{5}]", 
				new Object[] {
					this.queueUniverse.getBOMCount(), 
					this.queueUniverse.getBOMComponent(),
					this.queueUniverse.tradeLaneQueue.getCompletedWorkCount(),
					this.queueUniverse.compQueue.getCompletedWorkCount(),
					this.queueUniverse.classificationQueue.getCompletedWorkCount(),
					this.queueUniverse.compIVAPullQueue.getCompletedWorkCount()
				}
			);
		}
	
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	private void rawMaterialAndOrIntermediateAnalysis()
		throws Exception
	{
		PerformanceTracker				aPerfTracker = new PerformanceTracker(logger, Level.INFO, "rawMaterialAndOrIntermediateAnalysis");
		BOM								aBOM;
		BOMMetricSetUniverseContainer	aBOMSet;
		BOMUniverse						aBOMUniverse = this.queueUniverse.getBOMUniverse();
	
		this.qtxPrepProgressMgr.rawMaterialAndOrIntermediateAnalysisStart();
		try {
			aPerfTracker.start();
			aBOMSet = aBOMUniverse.getPrioritizedBOMSet();
			for (PrioritizedBOMMetricSet aBOMMetricSet : aBOMSet.metricSetList) {
				try {
					if (!this.queueUniverse.request.isBOMEnabled(aBOMMetricSet.metricSet.bomKey)) {
						MessageFormatter.trace(logger, "rawMaterialAndOrIntermediateAnalysis", "BOM [{0,number,#}]: not enabled for processing.", aBOMMetricSet.metricSet.bomKey);
						continue;
					}
					aBOM = aBOMUniverse.getBOM(aBOMMetricSet.metricSet.bomKey);
					this.queueUniverse.processBOMExpansion(aBOM);
				}
				catch (Exception e) {
					MessageFormatter.error(logger, "rawMaterialAndOrIntermediateAnalysis", e, "BOM [{0,number,#}]: failed to process.", aBOMMetricSet.metricSet.bomKey);
				}
			}
			
			this.queueUniverse.waitForCompletion();
		}
		catch (Exception e)
		{
			MessageFormatter.error(logger, "rawMaterialAndOrIntermediateAnalysis", e, "unexpected error");
			throw e;
		}
		finally {
			this.qtxPrepProgressMgr.rawMaterialAndOrIntermediateAnalysisComplete();
	
			aPerfTracker.stop("BOMs [{0}]; BOM Components [{1}]; QTXs [{2}]; QTX Components [{3}]; Classifications [{4}]; IVA Pulls [{5}]", 
				new Object[] {
					this.queueUniverse.getBOMCount(), 
					this.queueUniverse.getBOMComponent(),
					this.queueUniverse.tradeLaneQueue.getCompletedWorkCount(),
					this.queueUniverse.compQueue.getCompletedWorkCount(),
					this.queueUniverse.classificationQueue.getCompletedWorkCount(),
					this.queueUniverse.compIVAPullQueue.getCompletedWorkCount()
				}
			);
		}
	
	}
}
