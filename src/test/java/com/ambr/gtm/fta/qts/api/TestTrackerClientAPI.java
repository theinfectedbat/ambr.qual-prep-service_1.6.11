package com.ambr.gtm.fta.qts.api;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
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

	private static String			trackerURL				= "http://paa20481f:8380";
	private static String			tradeURL				= "http://paa20481f.amberroad.com:18300/gtm/service";
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
		TestTrackerClientAPI.tradeAPI.setTradekey("@MBERr0@D");
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
	
	@Test
	public void testLock() throws Exception
	{
		long time = System.currentTimeMillis();

//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-5446850115907833085L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-6844208730638327871L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-6383442079756980897L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-5449282853572251061L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-4886724277077986756L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-7077751031062415745L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-8462549902658588941L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-8881742555428293318L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-7744821164925035625L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-5443255914845557379L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-6743280403583650595L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-8498009882072781757L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-8913810529894261422L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-7678474229423220282L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-5759292617519527396L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-5589316585022760048L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-9097180848879751962L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-4653678903295887472L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-5093122480765659980L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-5804807513191767188L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-8650148629559870412L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-5199525799655232009L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-6545507031039293411L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-7864178616482490857L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-8993027249502539969L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-5229575922862886985L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-8694454651826444206L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-5992852653936424584L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-5959436262816277514L);
//		TestTrackerClientAPI.tradeAPI.acquireLock("RENAULT","REN_ADMIN_TEST",-7187781223524449966L);
		
//		boolean result = TestTrackerClientAPI.tradeAPI.releaseLock(lockId);
//		
//		logger.debug("Lock release = " + result);
//		
//		Assert.assertTrue(result);
//		
//		long altKeys[] = new long[3];
//		altKeys[0] = time+1;
//		altKeys[1] = time+2;
//		altKeys[2] = time+3;
//
//		long locks[] = TestTrackerClientAPI.tradeAPI.acquireLocks("AMBER_ROAD", "AMBER_ROAD", altKeys);
//		
//		for (int i=0; i<locks.length; i++)
//			logger.debug("Locks [" + i + "] = " + locks[i]);
//		
//		int results = TestTrackerClientAPI.tradeAPI.releaseLocks(locks);
//		
//		logger.debug("Locks Released " + locks.length + " = " + results);
//		
//		Assert.assertEquals(results, locks.length);
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
