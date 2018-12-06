package com.ambr.gtm.fta.qts.api;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.springframework.http.client.ClientHttpRequestInterceptor;

import com.ambr.gtm.fta.qts.QTXWork;
import com.ambr.gtm.fta.qts.QtxStatusUpdateRequest;
import com.ambr.gtm.fta.qts.TrackerCodes;
import com.ambr.gtm.fta.trade.client.TradeQualtxClient;
import com.ambr.gtm.fta.trade.model.BOMQualAuditEntity;

public class TestTrackerClientAPI
{
	private static Logger				logger				= LogManager.getLogger(TestTrackerClientAPI.class);

	public static Long					BOM_KEY				= 1L;
	public static Long					QTX_KEY				= 1L;
	public static Long					QTX_WID				= 1L;
	public static Integer				ANALYSIS_METHOD		= TrackerCodes.AnalysisMethod.TOP_DOWN_ANALYSIS.ordinal();
	public static Long					TOTAL_COMPONENTS	= 1L;

	private static String			trackerURL				= "http://localhost:8080";
	private static String			tradeURL				= "http://18.206.147.107:7566/TA/service";
	private static TrackerClientAPI		trackerAPI			= null;
	private static TradeQualtxClient	tradeAPI			= null;

	@BeforeClass
	public static void beforeClass() throws Exception
	{
		List<ClientHttpRequestInterceptor> interceptors = new ArrayList<ClientHttpRequestInterceptor>();
		interceptors.add(new LoggingRequestInterceptor());

		TestTrackerClientAPI.trackerAPI = new TrackerClientAPI(trackerURL);
//		TestTrackerClientAPI.trackerAPI.template.setInterceptors(interceptors);
		
		TestTrackerClientAPI.tradeAPI = new TradeQualtxClient(tradeURL);
//		TestTrackerClientAPI.tradeAPI.getTemplate().setInterceptors(interceptors);
		
		// trackerAPI.loadTracker(10000);
	}

//	@Test
	public void testStoreQTXWork() throws Exception
	{
		ArrayList<QTXWork> workList = new ArrayList<QTXWork>();
		QTXWork work = new QTXWork();
		
		work.qtx_wid = -1;
		work.setWorkStatus(TrackerCodes.QualtxStatus.INIT);
		work.time_stamp = new java.sql.Timestamp(System.currentTimeMillis());
		
		workList.add(work);
		
//		TestTrackerClientAPI.trackerAPI.storeQTXWork(workList);
	}
	
	//@Test
	public void testFullAudit() throws Exception
	{
		BOMQualAuditEntity auditEntity = new BOMQualAuditEntity("mdi_qualtx", 123);
		
		TestTrackerClientAPI.tradeAPI.doFullEntityAudit(auditEntity);
	}
	
	//@Test
	public void testLock() throws Exception
	{
		long time = System.currentTimeMillis();

		Long lockId = TestTrackerClientAPI.tradeAPI.acquireLock("AMBER_ROAD","AMBER_ROAD",System.currentTimeMillis());
		
		logger.debug("Lock id = " + lockId);
		
		boolean result = TestTrackerClientAPI.tradeAPI.releaseLock(lockId);
		
		logger.debug("Lock release = " + result);
		
		Assert.assertTrue(result);
		
		long altKeys[] = new long[3];
		altKeys[0] = time+1;
		altKeys[1] = time+2;
		altKeys[2] = time+3;

		long locks[] = TestTrackerClientAPI.tradeAPI.acquireLocks("AMBER_ROAD", "AMBER_ROAD", altKeys);
		
		for (int i=0; i<locks.length; i++)
			logger.debug("Locks [" + i + "] = " + locks[i]);
		
		int results = TestTrackerClientAPI.tradeAPI.releaseLocks(locks);
		
		logger.debug("Locks Released " + locks.length + " = " + results);
		
		Assert.assertEquals(results, locks.length);
	}

//	@Test
	public void testPostQualtxwork() throws Exception
	{
//		trackerAPI.addQualtxWork(BOM_KEY, QTX_WID, QTX_KEY, ANALYSIS_METHOD, TOTAL_COMPONENTS, TrackerCodes.QualtxStatus.PENDING.ordinal());
//		trackerAPI.updateQualtxCompWork(QTX_KEY, QTX_WID, QTX_COMP_KEY, QTX_COMP_WID, TrackerCodes.QualtxCompStatus.PENDING.ordinal());
//		trackerAPI.updateHeaderHSPullWork(QTX_KEY, QTX_WID, QTX_HS_WID, TrackerCodes.QualtxHSPullStatus.COMPLETED.ordinal());
//		trackerAPI.updateCompHSPullWork(QTX_KEY, QTX_WID, COMP_HS_WID, QTX_COMP_WID, TrackerCodes.QualtxCompHSPullStatus.COMPLETED.ordinal());
//		trackerAPI.updateCompIVAPullWorkStatus(QTX_KEY, QTX_WID, COMP_IVA_WID, QTX_COMP_WID, TrackerCodes.QualtxCompIVAPullStatus.COMPLETED.ordinal());

//		Thread.sleep(30 * 1000);

//		Assert.assertEquals(Integer.valueOf(TrackerCodes.QualtxCompStatus.COMPLETED.ordinal()), trackerAPI.getQualtxCompWorkStatus(QTX_WID, QTX_COMP_WID));
//		Thread.sleep(30 * 1000);
//		Assert.assertEquals(Integer.valueOf(TrackerCodes.QualtxStatus.READY_FOR_AUDIT.ordinal()), trackerAPI.getQualtxWorkStatus(QTX_WID));
//		Thread.sleep(30 * 1000);
//		Assert.assertFalse(trackerAPI.isBOMEligibleForPostPolicy(BOM_KEY));
//		trackerAPI.addQualtxWork(BOM_KEY, QTX_WID, QTX_KEY, ANALYSIS_METHOD, TOTAL_COMPONENTS, TrackerCodes.QualtxStatus.QUALIFICATION_COMPLETE.ordinal());
//		Assert.assertTrue(trackerAPI.isBOMEligibleForPostPolicy(BOM_KEY));
	}

	/*
	 * @Test public void postQualtxCompWork() throws Exception {
	 * trackerAPI.updateQualtxCompWork(QTX_KEY, QTX_WID, QTX_COMP_KEY,
	 * QTX_COMP_WID, TrackerCodes.QualtxCompStatus.PENDING.ordinal()); }
	 * @Test public void postQualtxHSWork() throws Exception {
	 * trackerAPI.updateHeaderHSPullWork(QTX_KEY, QTX_WID, QTX_HS_WID,
	 * TrackerCodes.HeaderHSPullStatus.COMPLETED.ordinal()); }
	 * @Test public void postCompHSWork() throws Exception {
	 * trackerAPI.updateCompHSPullWork(QTX_KEY, QTX_WID, COMP_HS_WID,
	 * QTX_COMP_WID, TrackerCodes.HSPullStatus.COMPLETED.ordinal()); }
	 * @Test public void postCompIVAWork() throws Exception {
	 * trackerAPI.updateCompIVAPullWorkStatus(QTX_KEY, QTX_WID, COMP_IVA_WID,
	 * QTX_COMP_WID, TrackerCodes.IVAPullStatus.COMPLETED.ordinal()); }
	 */
//	@Test
	public void testWorkProcessing() throws Exception
	{
		trackerAPI.processReQualWorks();
	}
	//@Test
	public void testTrackerUpdateStatusQualComplete() throws Exception
	{
		BOM_KEY = BOM_KEY + 1L;
		QTX_KEY = QTX_KEY + 1L;
		QTX_WID = QTX_WID + 1L;
		Long QTX_WID1 = QTX_WID + 1L;
		QTX_WID = QTX_WID + 1L;
		Long QTX_WID2 = QTX_WID + 1L;
		QTX_WID = QTX_WID + 1L;
		
		QtxStatusUpdateRequest theRequest1 = new QtxStatusUpdateRequest();
		theRequest1.setBOMKey(BOM_KEY);
		theRequest1.setQualtxKey(QTX_KEY);
		theRequest1.setQualtxWorkId(QTX_WID1);
		theRequest1.setAnalysisMethod(ANALYSIS_METHOD);
		theRequest1.setTotalComponents(TOTAL_COMPONENTS);
		theRequest1.setStatus(TrackerCodes.QualtxStatus.PENDING.ordinal());
		theRequest1.setWaitForNextAnalysisMethodFlg(false);
		trackerAPI.deleteBomTracker(BOM_KEY);
		trackerAPI.updateQualtxStatus(theRequest1);
		
		QtxStatusUpdateRequest theRequest2 = new QtxStatusUpdateRequest();
		theRequest2.setBOMKey(BOM_KEY);
		theRequest2.setQualtxKey(QTX_KEY);
		theRequest2.setQualtxWorkId(QTX_WID2);
		theRequest2.setAnalysisMethod(ANALYSIS_METHOD);
		theRequest2.setTotalComponents(TOTAL_COMPONENTS);
		theRequest2.setStatus(TrackerCodes.QualtxStatus.PENDING.ordinal());
		theRequest2.setWaitForNextAnalysisMethodFlg(false);
		trackerAPI.updateQualtxStatus(theRequest2);

		Thread.sleep(60 * 1000);

		Assert.assertEquals(Integer.valueOf(TrackerCodes.QualTrackerStatus.INIT.ordinal()), trackerAPI.getQualtxStatus(QTX_KEY));
		Assert.assertFalse(trackerAPI.isBOMEligibleForPostPolicy(BOM_KEY));
		
		QtxStatusUpdateRequest aTrackerModelData3 = new QtxStatusUpdateRequest();
		aTrackerModelData3.setBOMKey(BOM_KEY);
		aTrackerModelData3.setQualtxKey(QTX_KEY);
		aTrackerModelData3.setQualtxWorkId(QTX_WID1);
		aTrackerModelData3.setAnalysisMethod(ANALYSIS_METHOD);
		aTrackerModelData3.setTotalComponents(TOTAL_COMPONENTS);
		aTrackerModelData3.setStatus(TrackerCodes.QualtxStatus.QUALIFICATION_COMPLETE.ordinal());
		aTrackerModelData3.setWaitForNextAnalysisMethodFlg(false);
		trackerAPI.updateQualtxStatus(aTrackerModelData3);
		QtxStatusUpdateRequest aTrackerModelData4 = new QtxStatusUpdateRequest();
		aTrackerModelData4.setBOMKey(BOM_KEY);
		aTrackerModelData4.setQualtxKey(QTX_KEY);
		aTrackerModelData4.setQualtxWorkId(QTX_WID2);
		aTrackerModelData4.setAnalysisMethod(ANALYSIS_METHOD);
		aTrackerModelData4.setTotalComponents(TOTAL_COMPONENTS);
		aTrackerModelData4.setStatus(TrackerCodes.QualtxStatus.QUALIFICATION_COMPLETE.ordinal());
		aTrackerModelData4.setWaitForNextAnalysisMethodFlg(false);
		trackerAPI.updateQualtxStatus(aTrackerModelData4);
		
		Thread.sleep(60 * 1000);
		Assert.assertEquals(Integer.valueOf(TrackerCodes.QualTrackerStatus.QUALIFICATION_COMPLETE.ordinal()), trackerAPI.getQualtxStatus(QTX_KEY));
		Assert.assertTrue(trackerAPI.isBOMEligibleForPostPolicy(BOM_KEY));
	}
	//@Test
	public void testTrackerUpdateStatusQualFailed() throws Exception
	{
		BOM_KEY = BOM_KEY + 1L;
		QTX_KEY = QTX_KEY + 1L;
		QTX_WID = QTX_WID + 1L;
		Long QTX_WID1 = QTX_WID + 1L;
		QTX_WID = QTX_WID + 1L;
		Long QTX_WID2 = QTX_WID + 1L;
		QTX_WID = QTX_WID + 1L;
		trackerAPI.deleteBomTracker(BOM_KEY);
		QtxStatusUpdateRequest aTrackerModelData3 = new QtxStatusUpdateRequest();
		aTrackerModelData3.setBOMKey(BOM_KEY);
		aTrackerModelData3.setQualtxKey(QTX_KEY);
		aTrackerModelData3.setQualtxWorkId(QTX_WID1);
		aTrackerModelData3.setAnalysisMethod(ANALYSIS_METHOD);
		aTrackerModelData3.setTotalComponents(TOTAL_COMPONENTS);
		aTrackerModelData3.setStatus(TrackerCodes.QualtxStatus.QUALIFICATION_FAILED.ordinal());
		aTrackerModelData3.setWaitForNextAnalysisMethodFlg(false);
		trackerAPI.updateQualtxStatus(aTrackerModelData3);
		QtxStatusUpdateRequest aTrackerModelData4 = new QtxStatusUpdateRequest();
		aTrackerModelData4.setBOMKey(BOM_KEY);
		aTrackerModelData4.setQualtxKey(QTX_KEY);
		aTrackerModelData4.setQualtxWorkId(QTX_WID2);
		aTrackerModelData4.setAnalysisMethod(ANALYSIS_METHOD);
		aTrackerModelData4.setTotalComponents(TOTAL_COMPONENTS);
		aTrackerModelData4.setStatus(TrackerCodes.QualtxStatus.QUALIFICATION_FAILED.ordinal());
		aTrackerModelData4.setWaitForNextAnalysisMethodFlg(false);
		trackerAPI.updateQualtxStatus(aTrackerModelData4);
		
		Thread.sleep(60 * 1000);
		Assert.assertEquals(Integer.valueOf(TrackerCodes.QualTrackerStatus.QUALIFICATION_FAILED.ordinal()), trackerAPI.getQualtxStatus(QTX_KEY));
		Assert.assertTrue(trackerAPI.isBOMEligibleForPostPolicy(BOM_KEY));
	}
	//@Test
	public void testTrackerUpdateStatusAllProcessed() throws Exception
	{
		BOM_KEY = BOM_KEY + 1L;
		QTX_KEY = QTX_KEY + 1L;
		QTX_WID = QTX_WID + 1L;
		Long QTX_WID1 = QTX_WID + 1L;
		QTX_WID = QTX_WID + 1L;
		Long QTX_WID2 = QTX_WID + 1L;
		QTX_WID = QTX_WID + 1L;
		trackerAPI.deleteBomTracker(BOM_KEY);
		QtxStatusUpdateRequest aTrackerModelData3 = new QtxStatusUpdateRequest();
		aTrackerModelData3.setBOMKey(BOM_KEY);
		aTrackerModelData3.setQualtxKey(QTX_KEY);
		aTrackerModelData3.setQualtxWorkId(QTX_WID1);
		aTrackerModelData3.setAnalysisMethod(ANALYSIS_METHOD);
		aTrackerModelData3.setTotalComponents(TOTAL_COMPONENTS);
		aTrackerModelData3.setStatus(TrackerCodes.QualtxStatus.QUALIFICATION_FAILED.ordinal());
		aTrackerModelData3.setWaitForNextAnalysisMethodFlg(false);
		trackerAPI.updateQualtxStatus(aTrackerModelData3);
		QtxStatusUpdateRequest aTrackerModelData4 = new QtxStatusUpdateRequest();
		aTrackerModelData4.setBOMKey(BOM_KEY);
		aTrackerModelData4.setQualtxKey(QTX_KEY);
		aTrackerModelData4.setQualtxWorkId(QTX_WID2);
		aTrackerModelData4.setAnalysisMethod(ANALYSIS_METHOD);
		aTrackerModelData4.setTotalComponents(TOTAL_COMPONENTS);
		aTrackerModelData4.setStatus(TrackerCodes.QualtxStatus.QUALIFICATION_COMPLETE.ordinal());
		aTrackerModelData4.setWaitForNextAnalysisMethodFlg(false);
		trackerAPI.updateQualtxStatus(aTrackerModelData4);
		
		Thread.sleep(60 * 1000);
		Assert.assertEquals(Integer.valueOf(TrackerCodes.QualTrackerStatus.QUALIFICATION_COMPLETE.ordinal()), trackerAPI.getQualtxStatus(QTX_KEY));
		Assert.assertTrue(trackerAPI.isBOMEligibleForPostPolicy(BOM_KEY));
	}
	
	//@Test
	public void testTrackerUpdateStatusAllNotProcessed() throws Exception
	{
		BOM_KEY = BOM_KEY + 1L;
		QTX_KEY = QTX_KEY + 1L;
		QTX_WID = QTX_WID + 1L;
		Long QTX_WID1 = QTX_WID + 1L;
		QTX_WID = QTX_WID + 1L;
		Long QTX_WID2 = QTX_WID + 1L;
		QTX_WID = QTX_WID + 1L;
		trackerAPI.deleteBomTracker(BOM_KEY);
		QtxStatusUpdateRequest aTrackerModelData3 = new QtxStatusUpdateRequest();
		aTrackerModelData3.setBOMKey(BOM_KEY);
		aTrackerModelData3.setQualtxKey(QTX_KEY);
		aTrackerModelData3.setQualtxWorkId(QTX_WID1);
		aTrackerModelData3.setAnalysisMethod(ANALYSIS_METHOD);
		aTrackerModelData3.setTotalComponents(TOTAL_COMPONENTS);
		aTrackerModelData3.setStatus(TrackerCodes.QualtxStatus.READY_FOR_QUALIFICATION.ordinal());
		aTrackerModelData3.setWaitForNextAnalysisMethodFlg(false);
		trackerAPI.updateQualtxStatus(aTrackerModelData3);
		QtxStatusUpdateRequest aTrackerModelData4 = new QtxStatusUpdateRequest();
		aTrackerModelData4.setBOMKey(BOM_KEY);
		aTrackerModelData4.setQualtxKey(QTX_KEY);
		aTrackerModelData4.setQualtxWorkId(QTX_WID2);
		aTrackerModelData4.setAnalysisMethod(ANALYSIS_METHOD);
		aTrackerModelData4.setTotalComponents(TOTAL_COMPONENTS);
		aTrackerModelData4.setStatus(TrackerCodes.QualtxStatus.QUALIFICATION_COMPLETE.ordinal());
		aTrackerModelData4.setWaitForNextAnalysisMethodFlg(false);
		trackerAPI.updateQualtxStatus(aTrackerModelData4);
		
		Thread.sleep(60 * 1000);
		Assert.assertEquals(Integer.valueOf(TrackerCodes.QualTrackerStatus.INIT.ordinal()), trackerAPI.getQualtxStatus(QTX_KEY));
		Assert.assertFalse(trackerAPI.isBOMEligibleForPostPolicy(BOM_KEY));
	}
	

	//@Test
	public void testTrackerUpdateStatuProcessed() throws Exception
	{
		BOM_KEY = BOM_KEY + 1L;
		QTX_KEY = QTX_KEY + 1L;
		QTX_WID = QTX_WID + 1L;
		Long QTX_WID1 = QTX_WID + 1L;
		
		trackerAPI.deleteBomTracker(BOM_KEY);
		QtxStatusUpdateRequest aTrackerModelData3 = new QtxStatusUpdateRequest();
		aTrackerModelData3.setBOMKey(BOM_KEY);
		aTrackerModelData3.setQualtxKey(QTX_KEY);
		aTrackerModelData3.setQualtxWorkId(QTX_WID1);
		aTrackerModelData3.setAnalysisMethod(ANALYSIS_METHOD);
		aTrackerModelData3.setTotalComponents(TOTAL_COMPONENTS);
		aTrackerModelData3.setStatus(TrackerCodes.QualtxStatus.READY_FOR_QUALIFICATION.ordinal());
		aTrackerModelData3.setWaitForNextAnalysisMethodFlg(false);
		trackerAPI.updateQualtxStatus(aTrackerModelData3);
		
		Thread.sleep(60 * 1000);
		Assert.assertEquals(Integer.valueOf(TrackerCodes.QualTrackerStatus.INIT.ordinal()), trackerAPI.getQualtxStatus(QTX_KEY));
		Assert.assertFalse(trackerAPI.isBOMEligibleForPostPolicy(BOM_KEY));
	}
	
	//@Test
	public void testTrackerWaitForNextAnalysisMethod() throws Exception
	{
		BOM_KEY = BOM_KEY + 1L;
		QTX_KEY = QTX_KEY + 1L;
		QTX_WID = QTX_WID + 1L;
		Long QTX_WID1 = QTX_WID + 1L;
		QTX_WID = QTX_WID + 1L;
		Long QTX_WID2 = QTX_WID + 1L;
		QTX_WID = QTX_WID + 1L;
		trackerAPI.deleteBomTracker(BOM_KEY);
		QtxStatusUpdateRequest aTrackerModelData3 = new QtxStatusUpdateRequest();
		aTrackerModelData3.setBOMKey(BOM_KEY);
		aTrackerModelData3.setQualtxKey(QTX_KEY);
		aTrackerModelData3.setQualtxWorkId(QTX_WID1);
		aTrackerModelData3.setAnalysisMethod(ANALYSIS_METHOD);
		aTrackerModelData3.setTotalComponents(TOTAL_COMPONENTS);
		aTrackerModelData3.setStatus(TrackerCodes.QualtxStatus.QUALIFICATION_COMPLETE.ordinal());
		aTrackerModelData3.setWaitForNextAnalysisMethodFlg(true);
		trackerAPI.updateQualtxStatus(aTrackerModelData3);
		
		Thread.sleep(60 * 1000);
		Assert.assertEquals(Integer.valueOf(TrackerCodes.QualTrackerStatus.INIT.ordinal()), trackerAPI.getQualtxStatus(QTX_KEY));
		Assert.assertFalse(trackerAPI.isBOMEligibleForPostPolicy(BOM_KEY));
		
		QtxStatusUpdateRequest aTrackerModelData4 = new QtxStatusUpdateRequest();
		aTrackerModelData4.setBOMKey(BOM_KEY);
		aTrackerModelData4.setQualtxKey(QTX_KEY);
		aTrackerModelData4.setQualtxWorkId(QTX_WID2);
		aTrackerModelData4.setAnalysisMethod(TrackerCodes.AnalysisMethod.RAW_MATERIAL_ANALYSIS.ordinal());
		aTrackerModelData4.setTotalComponents(TOTAL_COMPONENTS);
		aTrackerModelData4.setStatus(TrackerCodes.QualtxStatus.QUALIFICATION_COMPLETE.ordinal());
		aTrackerModelData4.setWaitForNextAnalysisMethodFlg(false);
		trackerAPI.updateQualtxStatus(aTrackerModelData4);
		
		Thread.sleep(60 * 1000);
		Assert.assertEquals(Integer.valueOf(TrackerCodes.QualTrackerStatus.QUALIFICATION_COMPLETE.ordinal()), trackerAPI.getQualtxStatus(QTX_KEY));
		Assert.assertTrue(trackerAPI.isBOMEligibleForPostPolicy(BOM_KEY));
	}
	//@Test
	public void testTrackerWaitForNextAnalysisMethodForThree() throws Exception
	{
		BOM_KEY = BOM_KEY + 1L;
		QTX_KEY = QTX_KEY + 1L;
		QTX_WID = QTX_WID + 1L;
		Long QTX_WID1 = QTX_WID + 1L;
		QTX_WID = QTX_WID + 1L;
		Long QTX_WID2 = QTX_WID + 1L;
		QTX_WID = QTX_WID + 1L;
		Long QTX_WID3 = QTX_WID + 1L;
		trackerAPI.deleteBomTracker(BOM_KEY);
		QtxStatusUpdateRequest aTrackerModelData3 = new QtxStatusUpdateRequest();
		aTrackerModelData3.setBOMKey(BOM_KEY);
		aTrackerModelData3.setQualtxKey(QTX_KEY);
		aTrackerModelData3.setQualtxWorkId(QTX_WID1);
		aTrackerModelData3.setAnalysisMethod(ANALYSIS_METHOD);
		aTrackerModelData3.setTotalComponents(TOTAL_COMPONENTS);
		aTrackerModelData3.setStatus(TrackerCodes.QualtxStatus.QUALIFICATION_COMPLETE.ordinal());
		aTrackerModelData3.setWaitForNextAnalysisMethodFlg(true);
		trackerAPI.updateQualtxStatus(aTrackerModelData3);
		
		Thread.sleep(60 * 1000);
		Assert.assertEquals(Integer.valueOf(TrackerCodes.QualTrackerStatus.INIT.ordinal()), trackerAPI.getQualtxStatus(QTX_KEY));
		Assert.assertFalse(trackerAPI.isBOMEligibleForPostPolicy(BOM_KEY));
		
		QtxStatusUpdateRequest aTrackerModelData4 = new QtxStatusUpdateRequest();
		aTrackerModelData4.setBOMKey(BOM_KEY);
		aTrackerModelData4.setQualtxKey(QTX_KEY);
		aTrackerModelData4.setQualtxWorkId(QTX_WID2);
		aTrackerModelData4.setAnalysisMethod(TrackerCodes.AnalysisMethod.RAW_MATERIAL_ANALYSIS.ordinal());
		aTrackerModelData4.setTotalComponents(TOTAL_COMPONENTS);
		aTrackerModelData4.setStatus(TrackerCodes.QualtxStatus.QUALIFICATION_COMPLETE.ordinal());
		aTrackerModelData4.setWaitForNextAnalysisMethodFlg(true);
		trackerAPI.updateQualtxStatus(aTrackerModelData4);
		
		Thread.sleep(60 * 1000);
		
		
		Assert.assertEquals(Integer.valueOf(TrackerCodes.QualTrackerStatus.INIT.ordinal()), trackerAPI.getQualtxStatus(QTX_KEY));
		Assert.assertFalse(trackerAPI.isBOMEligibleForPostPolicy(BOM_KEY));
		
		
		QtxStatusUpdateRequest aTrackerModelData5 = new QtxStatusUpdateRequest();
		aTrackerModelData5.setBOMKey(BOM_KEY);
		aTrackerModelData5.setQualtxKey(QTX_KEY);
		aTrackerModelData5.setQualtxWorkId(QTX_WID3);
		aTrackerModelData5.setAnalysisMethod(TrackerCodes.AnalysisMethod.INTERMEDIATE_ANALYSIS.ordinal());
		aTrackerModelData5.setTotalComponents(TOTAL_COMPONENTS);
		aTrackerModelData5.setStatus(TrackerCodes.QualtxStatus.QUALIFICATION_COMPLETE.ordinal());
		aTrackerModelData5.setWaitForNextAnalysisMethodFlg(false);
		trackerAPI.updateQualtxStatus(aTrackerModelData5);
		
		Thread.sleep(60 * 1000);
		Assert.assertEquals(Integer.valueOf(TrackerCodes.QualTrackerStatus.QUALIFICATION_COMPLETE.ordinal()), trackerAPI.getQualtxStatus(QTX_KEY));
		Assert.assertTrue(trackerAPI.isBOMEligibleForPostPolicy(BOM_KEY));
	}
}
