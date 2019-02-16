package com.ambr.gtm.fta.qts.api;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qts.BOMTracker;
import com.ambr.gtm.fta.qts.QTXCompWork;
import com.ambr.gtm.fta.qts.QTXCompWorkHS;
import com.ambr.gtm.fta.qts.QTXCompWorkIVA;
import com.ambr.gtm.fta.qts.QTXWork;
import com.ambr.gtm.fta.qts.QTXWorkHS;
import com.ambr.gtm.fta.qts.QTXWorkRepository;
import com.ambr.gtm.fta.qts.QtsUtility;
import com.ambr.gtm.fta.qts.QtxStatusUpdateRequest;
import com.ambr.gtm.fta.qts.QtxTracker;
import com.ambr.gtm.fta.qts.QtxWorkBean;
import com.ambr.gtm.fta.qts.QtxWorkTracker;
import com.ambr.gtm.fta.qts.TrackerCodes;
import com.ambr.gtm.fta.qts.WorkManagementException;
import com.ambr.gtm.fta.qts.config.FTACtryConfigCache;
import com.ambr.gtm.fta.qts.config.QEConfigCache;
import com.ambr.gtm.fta.qts.container.TrackerContainer;
import com.ambr.gtm.fta.qts.util.TradeLane;
import com.ambr.gtm.fta.qts.work.QtxWorkInfo;
import com.ambr.gtm.fta.qts.workmgmt.QTXStageProducer;
import com.ambr.gtm.fta.qts.workmgmt.QTXWorkInfo;
import com.ambr.gtm.fta.qts.workmgmt.QTXWorkProducer;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.platform.utils.propertyresolver.ConfigurationPropertyResolver;

@RestController
public class TrackerServiceAPI
{
	static Logger logger = LogManager.getLogger(TrackerServiceAPI.class);
	
	@Autowired
	private QEConfigCache qeConfigCache;
	
	@Autowired
	private FTACtryConfigCache ftaCtryConfigCache;
	
    @Autowired
    private ConfigurationPropertyResolver propertyResolver;

    @Autowired
	QTXWorkProducer qtxWorkProducer;
    
    @Autowired
	QTXStageProducer qtxStageProducer;
    
    @Autowired
    QTXWorkRepository workRepository;
    
    @Autowired
    TrackerContainer trackerContainer;

    public TrackerServiceAPI() throws Exception
	{
      
	}
	
	/*@RequestMapping(value=TrackerClientAPI.QTX_WORKMGMT_WORK_STORE,  method = RequestMethod.POST)
	public boolean QTXWorkStore(
			@RequestBody ArrayList<QTXWork> workList
		) throws Exception
	{
		Connection connection = null;

		if (workList == null || workList.size() == 0) return true;
		
		try
		{
			connection = Env.getSingleton().getPoolConnection();
			
			QTXWorkRepository.storeWork(workList, connection);
			
			connection.commit();
		}
		catch (Exception e)
		{
			connection.rollback();
			
			throw e;
		}
		finally
		{
			Env.getSingleton().releasePoolConnection(connection);
		}
		
		return true;
	}*/
	
	@RequestMapping(value = TrackerClientAPI.POST_QUALIFICATION_WORK, method = RequestMethod.POST)
	public ResponseEntity<String> executeQualification(@RequestBody(required = true) QtxStatusUpdateRequest theRequest) throws Exception
	{
		
		if (theRequest.getBOMKey() != null && theRequest.getQualtxKey() != null
				&& theRequest.getOrgCode() != null &&  theRequest.getUserId() != null && theRequest.getIvaKey() != null)
		{
			QTXWorkInfo qtxWorkInfo = new QTXWorkInfo();
			qtxWorkInfo.bomKey = theRequest.getBOMKey();
			qtxWorkInfo.qualtx_key = theRequest.getQualtxKey();
			qtxWorkInfo.companyCode = theRequest.getOrgCode();
			qtxWorkInfo.userId = theRequest.getUserId();
			qtxWorkInfo.ivaKey = theRequest.getIvaKey();
			qtxWorkInfo.analysisMathod = theRequest.getAnalysisMethod();
			qtxWorkInfo.priority = theRequest.getPriority();
			new QtsUtility().executeQtxQualificationWork(qtxWorkInfo, workRepository, this.trackerContainer);
		}
		else
		{
			MessageFormatter.info(logger,"executeQualification","One or more Mandatory Fields are missing  BOMKey, QualtxKey, Orgcode, userId, and IVAKey " + TrackerClientAPI.POST_QUALIFICATION_WORK);
			return new ResponseEntity<String>("One or more Mandatory Fields are missing  BOMKey, QualtxKey, Orgcode, userId, and IVAKey ", HttpStatus.BAD_REQUEST);
		}

		return new ResponseEntity<String>(HttpStatus.OK);
	
	}
	
	@RequestMapping(value=TrackerClientAPI.QTX_WORK_STATUS,  method = RequestMethod.GET)
	public Integer getQualtxWorkStatus(
			@RequestParam(name="qtx_key", 		required=true)	Long theQtxKey,
			@RequestParam(name="qtx_wid", 		required=true)	Long theQualtxWorkId
		) throws Exception
	{
		//TODO check for invalid wid
		return this.trackerContainer.getWorkQtxTracker(theQtxKey,theQualtxWorkId).getQtxStatus().ordinal();
	}
	
	@RequestMapping(value=TrackerClientAPI.QTX_STATUS,  method = RequestMethod.GET)
	public ResponseEntity<Integer> getQualtxStatus(
			@RequestParam(name="qtx_key", 		required=true)	Long theQualtxKey
		) throws Exception
	{
		//TODO check for invalid wid
		return new ResponseEntity<Integer>(this.trackerContainer.getQtxTracker(theQualtxKey).getQtxStatus().ordinal(),HttpStatus.OK);
	}
	
	@RequestMapping(value=TrackerClientAPI.BOM_ELIGIBLE,  method = RequestMethod.GET)
	public Boolean getBOMStatus(
			@RequestParam(name="bom_key", 		required=true)	Long theBomKey
		) throws Exception
	{
		//TODO check for invalid BOM Key
		return this.trackerContainer.getBomTracker(theBomKey).isEligibleToTriggerPostBOMValidationPolicy().isEligibleforPostPolicyTrigger;
	}
	
	@RequestMapping(value=TrackerClientAPI.BOM_DELETE,  method = RequestMethod.GET)
	public void deleteBOMTracker(
			@RequestParam(name="bom_key", 		required=true)	Long theBomKey
		) throws Exception
	{
		//TODO check for invalid BOM Key
		 BOMTracker aBOMTracker=this.trackerContainer.getBomTracker(theBomKey);
		 this.trackerContainer.clearBOMTracker(aBOMTracker);
	}
	
    @RequestMapping(value = TrackerClientAPI.QTX_WORKMGMT_START, method = RequestMethod.GET)
    public void qtxStart(@PathVariable String id) throws WorkManagementException
    {
    	if (id == null || id.length() == 0) throw new WorkManagementException("Invalid id of [" + id + "]");
    	
    	if (id.equalsIgnoreCase("qtx_work_producer"))
    	{
    		this.qtxWorkProducer.startup();
    		return;
    	}

    	new IllegalArgumentException("Invalid id of [" + id + "]");
    }

    @RequestMapping(value = TrackerClientAPI.QTX_WORKMGMT_STOP, method = RequestMethod.GET)
    public void qtxSttop(@PathVariable String id) throws WorkManagementException
    {
    	if (id == null || id.length() == 0) throw new WorkManagementException("Invalid id of [" + id + "]");
    	
    	if (id.equalsIgnoreCase("qtx_work_producer"))
    	{
    		this.qtxWorkProducer.shutdown();
    		return;
    	}

    }

    @RequestMapping(value = TrackerClientAPI.QTX_WORKMGMT_STAGE_PRODUCER, method = RequestMethod.GET)
    public void qtxExecuteStageProducer() throws Exception
    {
    	try
    	{
     			this.qtxStageProducer.executeFindWork();
    	}
    	catch (Exception e)
    	{
    		MessageFormatter.error(logger, "qtxExecuteStageProducer",e, "Exception while executeFindWork ");
    	}
    }

    @RequestMapping(value = TrackerClientAPI.QTX_WORKMGMT_REQUAL_PRODUCER, method = RequestMethod.GET)
    public void qtxExecuteRequalProducer() throws Exception
    {
    	try
    	{
    		if(this.qtxWorkProducer.getStatus() == QTXWorkProducer.REQUAL_SERVICE_AVAILABLE)
    			this.qtxWorkProducer.executeFindWork();
    		else
    			throw new Exception("Requal service is in progress");
    	}
    	catch (Exception e)
    	{
    		MessageFormatter.error(logger, "qtxExecuteRequalProducer",e, "Exception while executeFindWork ");
    	}
    }

    @RequestMapping(value = TrackerClientAPI.QTX_WORKMGMT_WP_SET_LIMITS, method = RequestMethod.GET)
    public void qtxWorkProducerSetLimits(@RequestParam(name="maxObjects", required=true) long maxObjects) throws Exception
    {
		this.qtxWorkProducer.setMaxObjects(maxObjects);
    }

    @RequestMapping(value = TrackerClientAPI.QTX_WORKMGMT_SP_SET_LIMITS, method = RequestMethod.GET)
    public void qtxStageProducerSetLimits(
    		@RequestParam(name="maxWork", required=true) long maxWork,
    		@RequestParam(name="maxStage", required=true) long maxStage) throws Exception
    {
		this.qtxStageProducer.setLimits(maxWork, maxStage);
    }

    @RequestMapping(value = TrackerClientAPI.API_CREATE_UNIVERSE, method = RequestMethod.GET)
    public void createUniverse(@PathVariable String org_code) throws Exception
    {
    	List<TradeLane> tradeLanes = this.qeConfigCache.getQEConfig(org_code).getTradeLaneList();
    	
    	if (tradeLanes != null)
    	{
	    	for (TradeLane tradeLane : tradeLanes)
	    	{
	    		MessageFormatter.debug(logger, "createUniverse", "Create Universe called on Org [{0}]: trade lane [{1}] : [{2}]", org_code, tradeLane.getCtryOfImport(), tradeLane.getFtaCode());
	    	}
    	}
    	else
    	MessageFormatter.debug(logger, "createUniverse", "Create Universe called on org : [{0}] no tradelanes defined ", org_code);
    }

    @RequestMapping(value = "/qts/api/workmgmt/test_work_create", method = RequestMethod.GET)
    public void qtxTestWorkCreate(
    		@RequestParam(name="work_count") int workCount,
    		@RequestParam(name="include_work_hs") boolean includeWorkHS,
    		@RequestParam(name="comp_count") long compCount,
    		@RequestParam(name="include_comp_iva") boolean includeCompIVA,
    		@RequestParam(name="include_comp_hs") boolean includeCompHS) throws Exception
    {
    	for (int i=0; i<workCount; i++)
    	{
    		QTXWork work = workRepository.createWork();
    		
    		work.bom_key = 101L;
    		work.company_code = "AMBER_ROAD";
    		work.entity_key = 122L;
    		work.entity_type = QTXWork.ENTITY_TYPE_PRODUCT;
    		work.iva_key = 555L;
    		work.priority = 0;
    		work.userId = "SYSTEM";
    		
    		work.details = workRepository.createWorkDetails(work.qtx_wid);
    		work.details.components = compCount;
    		work.details.ctry_of_import = "US";
    		work.details.qualtx_key = work.qtx_wid;
    		work.details.reason_code = 111L;
    		
    		if (includeWorkHS)
    		{
    			QTXWorkHS workHS = workRepository.createWorkHS(work.qtx_wid);
    			
    			workHS.ctry_cmpl_key=456L;
    			workHS.hs_number = "10000001";
    			workHS.reason_code = 7L;
    			workHS.target_hs_ctry = "CA";
    			
    			work.workHSList.add(workHS);
    		}
    		
    		for (int j=0; j<compCount; j++)
    		{
    			QTXCompWork compWork = workRepository.createCompWork(work.qtx_wid);
    			
    			compWork.bom_comp_key = 111L;
    			compWork.bom_key = 222L;
    			compWork.entity_key = 333L;
    			compWork.entity_src_key = 444L;
    			compWork.priority = 0;
    			compWork.qualifier = TrackerCodes.QualtxCompQualifier.CREATE_COMP;
    			compWork.qualtx_comp_key = compWork.qtx_comp_wid;
    			compWork.qualtx_key = compWork.qtx_wid;
    			
    			work.compWorkList.add(compWork);
    			
    			if (includeCompIVA)
    			{
    				QTXCompWorkIVA compIVA = workRepository.createCompWorkIVA(compWork.qtx_comp_wid, work.qtx_wid);
    				
    				compIVA.reason_code = 9L;
    				compIVA.iva_key = 909L;
    				
    				compWork.compWorkIVAList.add(compIVA);
    			}
    			
    			if (includeCompHS)
    			{
    				QTXCompWorkHS compHS = workRepository.createCompWorkHS(compWork.qtx_comp_wid, work.qtx_wid);
    				
    				compHS.ctry_cmpl_key = 667L;
    				compHS.hs_number = "12121212";
    				compHS.reason_code = 3L;
    				compHS.target_hs_ctry = "CN";
    				
    				compWork.compWorkHSList.add(compHS);
    			}
    		}
    		
			workRepository.storeWork(work);
    	}
    }

    @RequestMapping(value = TrackerClientAPI.QTX_STATUS_UPDATE, method = RequestMethod.POST)
	public ResponseEntity<String> updateQtxworkStatus(@RequestBody(required = true) QtxStatusUpdateRequest theRequest) throws Exception
	{
		Long aBOMKey = theRequest.getBOMKey();
		Long aQualtxWorkId = theRequest.getQualtxWorkId();
		Long aQualtxKey = theRequest.getQualtxKey();
		Integer aAnalysisMethod = theRequest.getAnalysisMethod();
		Long aTotalComponents = theRequest.getTotalComponents();
		Integer aStatusOrdinal = theRequest.getStatus();
		MessageFormatter.info(logger, "updateQtxworkStatus","Received status code [{0}]  For the BOM key : [{1}] qtxkey : [{2}] qtxWorkId: [{3}] "+ aStatusOrdinal, aBOMKey, aQualtxKey, aQualtxWorkId);
		if (aBOMKey != null && aQualtxKey != null && aQualtxWorkId != null && aStatusOrdinal != null)
		{
			QtxWorkInfo qtxWorkInfo = new QtxWorkInfo();
			qtxWorkInfo.setBomKey(aBOMKey);
			qtxWorkInfo.setQtxWorkId(aQualtxWorkId);
			qtxWorkInfo.setQtxKey(aQualtxKey);
			if (aAnalysisMethod != null) qtxWorkInfo.setAnalysisMethod(TrackerCodes.AnalysisMethod.values()[aAnalysisMethod]);
			if (aTotalComponents != null) qtxWorkInfo.setTotalComponents(aTotalComponents);
			qtxWorkInfo.setWaitForNextAnalysisMethodFlg(theRequest.isWaitForNextAnalysisMethodFlg());
			TrackerCodes.QualtxStatus aStatus = null;
			aStatus = TrackerCodes.QualtxStatus.values()[aStatusOrdinal];
			qtxWorkInfo.setQtxStatus(aStatus);

			BOMTracker aBOMTracker = this.trackerContainer.getBomTracker(aBOMKey);
			int newPriority = theRequest.getPriority();
			int existingPriority = aBOMTracker.getPriority();
			if (newPriority >= 0 && newPriority > existingPriority) aBOMTracker.setPriority(newPriority);

			QtxTracker aQtxTracker = aBOMTracker.addQualtx(qtxWorkInfo);
			QtxWorkTracker aQtxWorkTracker = aQtxTracker.addQualtxWork(qtxWorkInfo);
			aQtxWorkTracker.setQtxLastModifiedTime(System.currentTimeMillis());

		}
		else
		{
			MessageFormatter.info(logger,"updateQtxworkStatus","One or more Mandatory Fields are missing  BOMKey, QualtxKey, QualtxWorkId and Status in the request" + TrackerClientAPI.QTX_STATUS_UPDATE);
			return new ResponseEntity<String>("One or more Mandatory Fields are missing  BOMKey, QualtxKey, QualtxWorkId and Status in the request", HttpStatus.BAD_REQUEST);
		}

		return new ResponseEntity<String>(HttpStatus.OK);
	}
    
    @RequestMapping(value=TrackerClientAPI.QTX_WORKMGMT_WORK_STORE,  method = RequestMethod.POST)
	public boolean QTXWorkStore(@RequestBody QtxWorkBean theRequestQtxWork) throws Exception
	{
		ArrayList<QTXWork> workList = theRequestQtxWork.qtxWorkList;
		if (workList == null || workList.size() == 0) return true;
	
		workRepository.storeWork(workList);
	
		return true;
	} 
    
	@RequestMapping(value = QEConfClientAPI.FLUSH_FTA_CTRY_DATA_GROUP, method = RequestMethod.POST)
	public boolean flushFTACtryDataGroupCache(@RequestBody String theOrgCode) throws Exception
	{

		if (theOrgCode != null) ftaCtryConfigCache.flushCache(theOrgCode);

		return true;

	}
}
