package com.ambr.gtm.fta.qts.workmgmt;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.transaction.PlatformTransactionManager;

import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.gtm.fta.qps.bom.api.BOMUniverseBOMClientAPI;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationProductContainer;
import com.ambr.gtm.fta.qps.gpmclass.api.GetGPMClassificationsByProductFromUniverseClientAPI;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAProductContainer;
import com.ambr.gtm.fta.qps.gpmsrciva.api.GetGPMSourceIVAByProductFromUniverseClientAPI;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTX;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponent;
import com.ambr.gtm.fta.qts.QTXCompWork;
import com.ambr.gtm.fta.qts.QTXCompWorkHS;
import com.ambr.gtm.fta.qts.QTXCompWorkIVA;
import com.ambr.gtm.fta.qts.QTXWork;
import com.ambr.gtm.fta.qts.QTXWorkHS;
import com.ambr.gtm.fta.qts.RequalificationWorkCodes;
import com.ambr.gtm.fta.qts.TrackerCodes;
import com.ambr.platform.rdbms.orm.EntityManager;
import com.ambr.platform.rdbms.schema.SchemaDescriptor;
import com.ambr.platform.uoid.UniversalObjectIDGenerator;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.platform.utils.log.PerformanceTracker;

//TODO metrics for each producer/consumer (throughput, remaining items)
//TODO estimate on BOM and when all related work items will be complete (estimate based on some form of throughput on each producer and last position of bom in each queue)

//TODO this class needs to only load the necessary columns based on the reason codes.  currently doing a select *.
//TODO limit select down to only X records processed at a time - OOM
//TODO feed work packages into consumers as soon as ready (can this be done before all data is staged)
public class QTXWorkUniverse
{
	private static final Logger logger = LogManager.getLogger(QTXWorkUniverse.class);
	
	private JdbcTemplate template;
	private PlatformTransactionManager txMgr;
	private SchemaDescriptor schemaDesc;
	
	private int batchSize;

	private HashMap<Long, WorkPackage> workByIDMap = new HashMap<Long, WorkPackage>();
	private HashMap<Long, WorkPackage> workByQualtxMap = new HashMap<Long, WorkPackage>();
	private HashMap<Long, QualTX> qualTXMap = new HashMap<Long, QualTX>();
	private HashMap<Long, ArrayList<CompWorkPackage>> compWorkByQualtxCompMap = new HashMap<Long, ArrayList<CompWorkPackage>>();
	private HashMap<Long, HashMap<Long, WorkPackage>> workByBOMMap = new HashMap <Long, HashMap<Long, WorkPackage>>();
	
	private long workLoaded = 0;
	private long workHSLoaded = 0;
	private long compWorkLoaded = 0;
	private long compWorkHSLoaded = 0;
	private long compWorkIVALoaded = 0;
	
	private HashMap<Long, ArrayList<WorkPackage>> gpmClassificationResourceRegistration = new HashMap <Long, ArrayList<WorkPackage>>();
	private HashMap<Long, ArrayList<CompWorkPackage>> gpmClassificationResourceCompRegistration = new HashMap <Long, ArrayList<CompWorkPackage>>();
	private HashMap<Long, GPMClassificationProductContainer> gpmClassificationCache = new HashMap<Long, GPMClassificationProductContainer>();

	private HashMap<Long, ArrayList<CompWorkPackage>> gpmSourceIVACompRegistration = new HashMap <Long, ArrayList<CompWorkPackage>>();
	private HashMap<Long, GPMSourceIVAProductContainer> gpmSourceIVAProductCache = new HashMap<Long, GPMSourceIVAProductContainer>();

	private BOMUniverseBOMClientAPI bomUniverseBOMClientAPI;
	private GetGPMClassificationsByProductFromUniverseClientAPI gpmClassificationsByProductAPI;
	private GetGPMSourceIVAByProductFromUniverseClientAPI gpmSourceIVAByProductFromUniverseClientAPI;
	
	UniversalObjectIDGenerator idGenerator;
	
	public QTXWorkUniverse(UniversalObjectIDGenerator idGenerator, PlatformTransactionManager txMgr, SchemaDescriptor schemaDesc, JdbcTemplate template, BOMUniverseBOMClientAPI bomUniverseBOMClientAPI, GetGPMClassificationsByProductFromUniverseClientAPI gpmClassificationsByProductAPI, GetGPMSourceIVAByProductFromUniverseClientAPI gpmSourceIVAByProductFromUniverseClientAPI, int batchSize)
	{
		this.txMgr = txMgr;
		this.schemaDesc = schemaDesc;
		this.template = template;
		
		this.idGenerator = idGenerator;
		
		this.batchSize = batchSize;
		
		this.bomUniverseBOMClientAPI = bomUniverseBOMClientAPI;
		this.gpmClassificationsByProductAPI = gpmClassificationsByProductAPI;
		this.gpmSourceIVAByProductFromUniverseClientAPI = gpmSourceIVAByProductFromUniverseClientAPI;
	}
	
	public HashMap <Long, WorkPackage> stageWork(long bestTime, JdbcTemplate template) throws Exception
	{
		//Order of following steps are important
		
		//Step 1
		this.updateInitWorkToPending(bestTime);
		this.workLoaded = this.loadPendingWork();
		
		//Step 2
		//TODO review - this could be handled with one update to each child table (after ar_qtx_work_status is set to pending)
		this.updateWorkRecordsToPending("update ar_qtx_work_status set status=? where qtx_wid=?");
		this.updateWorkRecordsToPending("update ar_qtx_work_hs set status=? where qtx_wid=?");
		this.updateWorkRecordsToPending("update ar_qtx_comp_work_status set status=? where qtx_wid=?");
		this.updateWorkRecordsToPending("update ar_qtx_comp_work_hs set status=? where qtx_wid=?");
		this.updateWorkRecordsToPending("update ar_qtx_comp_work_iva set status=? where qtx_wid=?");
		
		//Step 3
		this.loadPendingQualtx();
		this.workHSLoaded = this.loadPendingWorkHS();

		//Step 4
		this.compWorkLoaded = this.loadPendingCompWork();
		this.loadPendingQualtxComp();
		this.compWorkIVALoaded = this.loadPendingCompWorkIVA();
		this.compWorkHSLoaded = this.loadPendingCompWorkHS();
		
		//Loading of BOM resource data must take place prior to GPMClassification loading
		this.loadRequiredBOMResources();
		this.loadRequiredGPMClassificationResources();
		this.loadRequiredGPMIVAResources();
		this.setupEntityManagers();
		logger.debug("Staged work items " + this.workByIDMap.size() + " root work items " + this.workByQualtxMap.size());
		
		return this.workByQualtxMap;
	}
	
	private void setupEntityManagers() throws Exception
	{
		for (WorkPackage workPackage : this.workByQualtxMap.values())
		{
			EntityManager<QualTX> qualtxMgr = new EntityManager<QualTX>(QualTX.class, this.txMgr, this.schemaDesc, template);

			try
			{
				qualtxMgr.setExistingEntity(workPackage.qualtx);
				workPackage.setEntityManager(qualtxMgr);

			}
			catch (Exception e)
			{
				MessageFormatter.error(logger, "setupEntityManagers", e, "Work package does not have qualtx assigned for the Ar qtx work Id : [{0}] ", workPackage.work.qtx_wid);

			}
		}
	}

	private int updateWorkRecordsToPending(String sql) throws SQLException
	{
		PerformanceTracker tracker = new PerformanceTracker(logger, Level.INFO, "updateWorkRecordsToPending");
		int rowsAffected = 0;
		
		tracker.start();
		try
		{
			int updateMatrix[][] = template.batchUpdate(sql, this.workByIDMap.values(), this.batchSize, new ParameterizedPreparedStatementSetter<WorkPackage>() {
					@Override
					public void setValues(PreparedStatement ps, WorkPackage argument) throws SQLException {
						ps.setInt(1, TrackerCodes.QualtxStatus.PENDING.ordinal());
						ps.setLong(2, argument.work.qtx_wid);
					}
				  });
			
			for (int updateList[] : updateMatrix)
			{
				for (int updateCount : updateList)
					rowsAffected = rowsAffected + updateCount;
			}
		}
		finally
		{
			tracker.stop("ar_qtx_work_status records affected = {0}" , new Object[]{rowsAffected});
		}
		
		return rowsAffected;
	}
	
	private long loadPendingWork() throws Exception
	{
		logger.debug("Loading init work records");
		
		PerformanceTracker tracker = new PerformanceTracker(logger, Level.INFO, "loadPendingWork");
		
		tracker.start();
		
		try
		{
			List<QTXWork> workList = this.template.query(this.getProducerSQL(), new Object[] {TrackerCodes.QualtxStatus.PENDING.ordinal()},  new QTXWorkResultSetExtractor());
			
			for (QTXWork work : workList)
			{
				this.addWorkPackage(work);
			}
		}
		finally
		{
			tracker.stop("ar_qtx_work records loaded = {0}" , new Object[]{this.workByIDMap.size()});
		}
		
		logger.debug("Loaded work items = " + this.workByIDMap.size());
		
		return this.workByIDMap.size();
	}
	
	private int loadPendingQualtx() throws Exception
	{
		PerformanceTracker tracker = new PerformanceTracker(logger, Level.INFO, "loadPendingQualtx");
		logger.debug("Loading related qualtx records");
		
		String sql = "select MDI_QUALTX.* "
				+ "from MDI_QUALTX "
				+ "join ("
				+ "   select distinct qualtx_key from AR_QTX_WORK_DETAILS "
				+ "   join AR_QTX_WORK_STATUS ON AR_QTX_WORK_DETAILS.QTX_WID = AR_QTX_WORK_STATUS.QTX_WID "
				+ "   WHERE AR_QTX_WORK_STATUS.STATUS=?"
				+ ") QTX on MDI_QUALTX.ALT_KEY_QUALTX = QTX.QUALTX_KEY";
		tracker.start();

		List<QualTX> qualtxList = null;
		try
		{
			SimpleDataLoaderResultSetExtractor<QualTX> extractor = new SimpleDataLoaderResultSetExtractor<QualTX>(QualTX.class);
			
			qualtxList = this.template.query(sql, new Object[]{TrackerCodes.QualtxStatus.PENDING.ordinal()}, extractor);
			
			for (QualTX qualtx : qualtxList)
			{
				qualtx.idGenerator = this.idGenerator;
				this.setQualtxOnWorkPackage(qualtx);
				this.qualTXMap.put(qualtx.alt_key_qualtx, qualtx);
			}
		}
		finally
		{
			tracker.stop("mdi_qualtx records loaded = {0}" , new Object[]{(qualtxList != null) ? qualtxList.size() : "ERROR"});
		}
		
		logger.debug("Load qualtx records = " + qualtxList.size());
		
		return qualtxList.size();
	}
	
	private long loadPendingWorkHS() throws Exception
	{
		PerformanceTracker tracker = new PerformanceTracker(logger, Level.INFO, "loadPendingWorkHS");
		logger.debug("Loading pending work hs records ...");

		tracker.start();
		
		List<QTXWorkHS> list = null;
		try
		{
			String sql = "select * "
					+ "from AR_QTX_WORK_HS "
					+ "WHERE AR_QTX_WORK_HS.STATUS=?";
			
			SimpleDataLoaderResultSetExtractor<QTXWorkHS> extractor = new SimpleDataLoaderResultSetExtractor<QTXWorkHS>(QTXWorkHS.class);

			list = this.template.query(sql, new Object[] {TrackerCodes.QualtxHSPullStatus.PENDING.ordinal()}, extractor);
			
			for (QTXWorkHS workHS : list)
			{
				this.setWorkHSOnWorkPackage(workHS);
			}
		}
		finally
		{
			tracker.stop("ar_qtx_work_hs records loaded = {0}" , new Object[]{(list != null) ? list.size() : "ERROR"});
		}
		
		logger.debug("Loaded pending work hs records = " + list.size());
		
		return list.size();
	}
	
	private long loadPendingCompWork() throws Exception
	{
		PerformanceTracker tracker = new PerformanceTracker(logger, Level.INFO, "loadPendingCompWork");
		logger.debug("Loading pending compwork records");

		tracker.start();
		
		List<QTXCompWork> list = null;
		try
		{
			String sql = "select AR_QTX_COMP_WORK.*,AR_QTX_COMP_WORK_STATUS.status "
					+ "from AR_QTX_COMP_WORK_STATUS "
					+ "join AR_QTX_COMP_WORK on AR_QTX_COMP_WORK.qtx_comp_wid = AR_QTX_COMP_WORK_STATUS.qtx_comp_wid "
					+ "WHERE AR_QTX_COMP_WORK_STATUS.STATUS=?";
			
			SimpleDataLoaderResultSetExtractor<QTXCompWork> extractor = new SimpleDataLoaderResultSetExtractor<QTXCompWork>(QTXCompWork.class);
			
			list = this.template.query(sql, new Object[]{TrackerCodes.QualtxCompStatus.PENDING.ordinal()}, extractor);
			
			for (QTXCompWork compWork : list)
			{
				this.setCompWorkOnWorkPackage(compWork);
			}
		}
		finally
		{
			tracker.stop("ar_qtx_comp_work records loaded = {0}" , new Object[]{(list != null) ? list.size() : "ERROR"});
		}
		
		logger.debug("Loaded compwork records = " + list.size());
		
		return list.size();
	}

	private long loadPendingCompWorkHS() throws Exception
	{
		PerformanceTracker tracker = new PerformanceTracker(logger, Level.INFO, "loadPendingCompWorkHS");
		logger.debug("Loading pending compwork hs records");

		tracker.start();
		
		List<QTXCompWorkHS> list = null;
		try
		{
			String sql = "select * "
					+ "from AR_QTX_COMP_WORK_HS "
					+ "WHERE AR_QTX_COMP_WORK_HS.STATUS=?";
			
			SimpleDataLoaderResultSetExtractor<QTXCompWorkHS> extractor = new SimpleDataLoaderResultSetExtractor<QTXCompWorkHS>(QTXCompWorkHS.class);
			
			list = this.template.query(sql, new Object[]{TrackerCodes.QualtxCompHSPullStatus.PENDING.ordinal()}, extractor);
			
			for (QTXCompWorkHS compWorkHS : list)
			{
				this.setCompWorkHSOnWorkPackage(compWorkHS);
			}
		}
		finally
		{
			tracker.stop("ar_qtx_comp_work_hs records loaded = {0}" , new Object[]{(list != null) ? list.size() : "ERROR"});
		}
		
		logger.debug("Load compwork hs records = " + list.size());
		
		return list.size();
	}

	private long loadPendingCompWorkIVA() throws Exception
	{
		PerformanceTracker tracker = new PerformanceTracker(logger, Level.INFO, "loadPendingCompWorkIVA");
		logger.debug("Loading pending compwork iva records");

		tracker.start();
		
		List<QTXCompWorkIVA> list = null;
		try
		{
			String sql = "select * "
					+ "from AR_QTX_COMP_WORK_IVA "
					+ "WHERE AR_QTX_COMP_WORK_IVA.STATUS=?";
			
			SimpleDataLoaderResultSetExtractor<QTXCompWorkIVA> extractor = new SimpleDataLoaderResultSetExtractor<QTXCompWorkIVA>(QTXCompWorkIVA.class);
			
			list = this.template.query(sql, new Object[]{TrackerCodes.QualtxCompIVAPullStatus.PENDING.ordinal()}, extractor);
			
			for (QTXCompWorkIVA compWorkIVA : list)
			{
				this.setCompWorkIVAOnWorkPackage(compWorkIVA);
			}
		}
		finally
		{
			tracker.stop("ar_qtx_work_status records affected = {0}" , new Object[]{(list != null) ? list.size() : "ERROR"});
		}
		
		logger.debug("Load compwork iva records = " + list.size());
		
		return list.size();
	}

	private void loadPendingQualtxComp() throws Exception
	{
		PerformanceTracker tracker = new PerformanceTracker(logger, Level.INFO, "loadPendingQualtxComp");
		logger.debug("Loading related qualtx comp records");

		tracker.start();
		
		List<QualTXComponent> qualtxCompList = null;
		try
		{
			String sql = "select MDI_QUALTX_COMP.* "
					+ "from MDI_QUALTX_COMP "
					+ "join ("
					+ "   select distinct qualtx_comp_key from AR_QTX_COMP_WORK"
					+ "   join AR_QTX_COMP_WORK_STATUS ON AR_QTX_COMP_WORK.QTX_COMP_WID = AR_QTX_COMP_WORK_STATUS.QTX_COMP_WID "
					+ "   WHERE AR_QTX_COMP_WORK_STATUS.STATUS=?"
					+ ") QTX_COMP on MDI_QUALTX_COMP.ALT_KEY_COMP = QTX_COMP.QUALTX_COMP_KEY";
			
			SimpleDataLoaderResultSetExtractor<QualTXComponent> extractor = new SimpleDataLoaderResultSetExtractor<QualTXComponent>(QualTXComponent.class);

			qualtxCompList = this.template.query(sql, new Object[] {TrackerCodes.QualtxCompStatus.PENDING.ordinal()}, extractor);
				
			for (QualTXComponent qualtxComp : qualtxCompList)
			{
				this.setQualtxCompOnWorkPackage(qualtxComp);
			}
		}
		finally
		{
			tracker.stop("mdi_qualtx_comp records loaded = {0}" , new Object[]{(qualtxCompList != null) ? qualtxCompList.size() : "ERROR"});
		}
		
		logger.debug("Loaded qualtx comp records = " + qualtxCompList.size());
	}
	
	private void setWorkHSOnWorkPackage(QTXWorkHS workHS)
	{
		WorkPackage workPackage = this.getWorkPackage(workHS.qtx_wid);
		
		workPackage.work.workHSList.add(workHS);
	}
	
	private void setCompWorkOnWorkPackage(QTXCompWork compWork)
	{
		WorkPackage workPackage = this.getWorkPackage(compWork.qtx_wid);
		
		if(null == workPackage) return;
		
		CompWorkPackage compWorkPackage = new CompWorkPackage(workPackage);
		
		workPackage.work.addCompWork(compWork);
		
		compWorkPackage.compWork = compWork;

//		if(compWork.qualtx_comp_key == null) 
//			compWorkPackage.qualtxCompAudit = new BOMQualAuditEntity(MDIQualTxComp.TABLE_NAME); //When new component added to a bom, there will be no qualtx_comp_key
//		else 
//			compWorkPackage.qualtxCompAudit = new BOMQualAuditEntity(MDIQualTxComp.TABLE_NAME, compWork.qualtx_comp_key); 
//		
//		workPackage.qualtxAudit.addChildTable(compWorkPackage.qualtxCompAudit);
		
		workPackage.addCompWorkPackage(compWorkPackage);
		
		if (compWork.qualtx_comp_key != null)
		{
			ArrayList<CompWorkPackage> compWorkList = this.compWorkByQualtxCompMap.get(compWork.qualtx_comp_key);
			
			if (compWorkList == null)
			{
				compWorkList = new ArrayList<CompWorkPackage>();
				
				this.compWorkByQualtxCompMap.put(compWork.qualtx_comp_key, compWorkList);
			}
			compWorkList.add(compWorkPackage);
		}
		
		this.checkForBOMResourceRequirements(compWorkPackage);

	}
	
	private void setCompWorkHSOnWorkPackage(QTXCompWorkHS compWorkHS)
	{
		WorkPackage workPackage = this.getWorkPackage(compWorkHS.qtx_wid);
		
		CompWorkPackage compWorkPackage = workPackage.getCompWorkPackage(compWorkHS.qtx_comp_wid);
		
		compWorkPackage.compWork.compWorkHSList.add(compWorkHS);
		
		this.checkForGPMClassificationResourceCompRequirements(compWorkPackage, compWorkHS);
	}
	
	private void setCompWorkIVAOnWorkPackage(QTXCompWorkIVA compWorkIVA)
	{
		WorkPackage workPackage = this.getWorkPackage(compWorkIVA.qtx_wid);
		
		CompWorkPackage compWorkPackage = workPackage.getCompWorkPackage(compWorkIVA.qtx_comp_wid);
		
		compWorkPackage.compWork.compWorkIVAList.add(compWorkIVA);
		
		this.checkForIVARequirements(compWorkPackage, compWorkIVA);
	}
	
	private void setQualtxOnWorkPackage(QualTX qualtx) throws Exception
	{
		WorkPackage workPackage = this.workByQualtxMap.get(qualtx.alt_key_qualtx);
		this.qualTXMap.put(qualtx.alt_key_qualtx, qualtx);
		//EntityManager<QualTX> entityMgr = new EntityManager<QualTX>(QualTX.class, this.txMgr, this.schemaDesc, template);
		//entityMgr.loadExistingEntity(theKey)
		//entityMgr.setExistingEntity(qualtx);
		while (workPackage != null)
		{
			workPackage.qualtx = qualtx;
			this.checkForGPMClassificationResourceRequirements(workPackage);
			//workPackage.entityMgr = entityMgr;
			
			workPackage = workPackage.getLinkedPackage();
		}
	}
	
	private void setQualtxCompOnWorkPackage(QualTXComponent qualtxComp) throws Exception
	{
		WorkPackage workPackage = this.workByQualtxMap.get(qualtxComp.alt_key_qualtx);
		QualTX qualtx = this.qualTXMap.get(qualtxComp.alt_key_qualtx);
		
		if(qualtx == null)
		{
			logger.error("Qualtx is not loaded for the component ID :" + qualtxComp.comp_id + " and qualtxComp.tx_id=" + qualtxComp.tx_id +" and qualtxComp.src_key=" + qualtxComp.src_key + " AND QTX_WID=" + workPackage.work.qtx_wid);
			return ; //Invalid Data, Added for debuggging
		}
		
		// qualtx will not have list of comps - managed separately

		qualtx.addComponent(qualtxComp);

		while (workPackage != null)
		{
			workPackage.qualtx = qualtx;
			
			workPackage = workPackage.getLinkedPackage();
		}

			ArrayList<CompWorkPackage> compWorkList = this.compWorkByQualtxCompMap.get(qualtxComp.alt_key_comp);
			
			if(compWorkList == null || compWorkList.size() == 0)
			{
				logger.error("compWorkByQualtxCompMap returns empty for the component ID :" + qualtxComp.comp_id + " and qualtxComp.tx_id" + qualtxComp.tx_id +" and qualtxComp.src_key" + qualtxComp.src_key + "AND QTX_WID=" + workPackage.work.qtx_wid);
				return ;  //Invalid, Added for debuggging
			}
			
			// EntityManager<QualTXComponent> entityMgr = new
			// EntityManager<QualTXComponent>(QualTXComponent.class, this.txMgr,
			// this.schemaDesc, template);
			// entityMgr.setExistingEntity(qualtxComp);
			for (CompWorkPackage compWorkPackage : compWorkList)
			{
				compWorkPackage.qualtxComp = qualtxComp;

				// compWorkPackage.entityMgr = entityMgr;
			}

	}
	
	private WorkPackage addWorkPackage(QTXWork work)
	{
		WorkPackage workPackage = new WorkPackage();
		
		workPackage.work = work;
		
		this.workByIDMap.put(workPackage.work.qtx_wid, workPackage);
		
		//This will serialize all work items targeting the same qualtx into a single linked list which will be processed in order by QTXWorkConsumer
		WorkPackage headPackage = this.workByQualtxMap.get(work.details.qualtx_key);
		if (headPackage == null)
		{
			this.workByQualtxMap.put(work.details.qualtx_key, workPackage);
		}
		else
		{
			headPackage.appendLinkedPackage(workPackage);
		}
		
		this.checkForBOMResourceRequirements(workPackage);

		return workPackage;
	}
	
	private void checkForIVARequirements(CompWorkPackage compWorkPackage, QTXCompWorkIVA compWorkIVA)
	{
		if (compWorkIVA.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_COMP_FINAL_DECISION_CHANGE) || compWorkIVA.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_IVA_CHANGE_M_I))
		{
			this.addSourceIVACompRequirement(compWorkPackage);
		}
	}
	
	private void addSourceIVACompRequirement(CompWorkPackage compWorkPackage)
	{
		Long prodKey = null;
		
		if (compWorkPackage.qualtxComp != null)
			prodKey = compWorkPackage.qualtxComp.prod_key;
		else
		{
			prodKey = compWorkPackage.compWork.entity_key; //For comp work, we are inserting the prod_key of the bom component for the entity key column while posting the work item
		}
		
		if (prodKey == null)
		{
			logger.error("Attempt to register for source iva comp requirement with null prod_key.  Work item " + compWorkPackage.compWork.qtx_wid + ":" + compWorkPackage.compWork.qtx_comp_wid);
			return;
		}

		ArrayList<CompWorkPackage> registeredWork = this.gpmSourceIVACompRegistration.get(prodKey);
		
		if (registeredWork == null)
		{
			registeredWork = new ArrayList<CompWorkPackage>();
			
			this.gpmSourceIVACompRegistration.put(prodKey, registeredWork);
		}
		
		logger.debug("Registering " + compWorkPackage.compWork.qtx_wid + ":" + compWorkPackage.compWork.qtx_comp_wid + " for GPMSourceIVAProduct " + prodKey);

		registeredWork.add(compWorkPackage);
	}
	
	private void checkForGPMClassificationResourceCompRequirements(CompWorkPackage compWorkPackage, QTXCompWorkHS compWorkHS)
	{
		if (compWorkHS.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_CTRY_CMPL_ADDED) ||
				compWorkHS.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_CTRY_CMPL_CHANGE) ||
				compWorkPackage.compWork.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_COMP_COO_CHG) ||
				compWorkPackage.compWork.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_COMP_COM_COO_CHG) ||
				compWorkPackage.compWork.isReasonCodeFlagSet(RequalificationWorkCodes.COMP_STP_COO_CHG) ||
				compWorkPackage.compWork.isReasonCodeFlagSet(RequalificationWorkCodes.COMP_GPM_COO_CHG))
		{
			this.addGPMClassificationResourceCompRequirement(compWorkPackage, compWorkPackage.compWork.entity_key);
		}
	}
	
	private void addGPMClassificationResourceCompRequirement(CompWorkPackage compWorkPackage, Long prodKey)
	{
		if (prodKey == null)
		{
			logger.error("Attempt to register for gpm classification resources with null prod_key.  Work item " + compWorkPackage.compWork.qtx_wid + ":" + compWorkPackage.compWork.qtx_comp_wid);
			return;
		}

		ArrayList<CompWorkPackage> registeredWork = this.gpmClassificationResourceCompRegistration.get(prodKey);
		
		if (registeredWork == null)
		{
			registeredWork = new ArrayList<CompWorkPackage>();
			
			this.gpmClassificationResourceCompRegistration.put(prodKey, registeredWork);
		}
		
		logger.debug("Registering " + compWorkPackage.compWork.qtx_wid + ":" + compWorkPackage.compWork.qtx_comp_wid + " for GPMClassification " + prodKey);

		registeredWork.add(compWorkPackage);
	}

	private void checkForGPMClassificationResourceRequirements(WorkPackage workPackage)
	{
			this.addGPMClassificationResourceRequirement(workPackage, workPackage.qualtx.prod_key);
	}
	
	private void addGPMClassificationResourceRequirement(WorkPackage workPackage, Long prodKey)
	{
		if (prodKey == null)
		{
			logger.error("Attempt to register for gpm classification resources with null prod_key.  Work item " + workPackage.work.qtx_wid);
			return;
		}

		ArrayList<WorkPackage> registeredWork = this.gpmClassificationResourceRegistration.get(prodKey);
		
		if (registeredWork == null)
		{
			registeredWork = new ArrayList<WorkPackage>();
			
			this.gpmClassificationResourceRegistration.put(prodKey, registeredWork);
		}
		
		logger.debug("Registering " + workPackage.work.qtx_wid + " for GPMClassification " + prodKey);
		
		registeredWork.add(workPackage);
	}

	private void checkForBOMResourceRequirements(WorkPackage workPackage)
	{
//		QTXWorkDetails workDetails = workPackage.work.details;
		
		/*if (workDetails.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_HDR_CHG) || 
				workDetails.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_PRC_CHG) ||
				workDetails.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_PROD_TXT_DE) ||
				workDetails.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_PROD_AUTO_DE) || workPackage.work.workHSList.size() > 0 )
		{
			this.addBOMResourceRequirement(workPackage);
		}*/
		this.addBOMResourceRequirement(workPackage);
	}
	
	private void checkForBOMResourceRequirements(CompWorkPackage compWorkPackage)
	{
		/*if (compWorkPackage.compWork.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_COMP_ADDED) ||
				compWorkPackage.compWork.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_COMP_MODIFIED) ||
				compWorkPackage.compWork.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_COMP_YARN_DTLS_CHG) ||
				compWorkPackage.compWork.isReasonCodeFlagSet(RequalificationWorkCodes.COMP_PRC_CHG) || compWorkPackage.compWork.compWorkHSList.size() > 0 || compWorkPackage.compWork.compWorkIVAList.size() > 0)
		{
			this.addBOMResourceRequirement(compWorkPackage.parent);
		}*/
		
		this.addBOMResourceRequirement(compWorkPackage.getParentWorkPackage());
	}
	
	private void addBOMResourceRequirement(WorkPackage workPackage)
	{
		HashMap<Long, WorkPackage> registeredPackages = this.workByBOMMap.get(workPackage.work.bom_key);
		
		if (registeredPackages == null)
		{
			registeredPackages = new HashMap<Long, WorkPackage>();
			this.workByBOMMap.put(workPackage.work.bom_key, registeredPackages);
		}
		
		logger.debug("Registering " + workPackage.work.qtx_wid + " for BOM " + workPackage.work.bom_key);

		registeredPackages.put(workPackage.work.qtx_wid, workPackage);
	}
	
	private WorkPackage getWorkPackage(Long workId)
	{
		if (workId != null)
			return this.workByIDMap.get(workId);
		
		return null;
	}
	
	private void loadRequiredGPMIVAResources() throws Exception
	{
		PerformanceTracker tracker = new PerformanceTracker(logger, Level.INFO, "loadRequiredGPMIVAResources");

		tracker.start();
		
		try
		{
			for (Long prodKey : this.gpmSourceIVACompRegistration.keySet())
			{
				for (CompWorkPackage relatedPackage : this.gpmSourceIVACompRegistration.get(prodKey))
				{
					try 
					{
						relatedPackage.gpmSourceIVAProductContainer = this.getGPMSourceIVAProductContainer(prodKey);
					}
					catch (Exception e)
					{
						relatedPackage.failure = e;
					}
				}
			}
		}
		finally
		{
			tracker.stop("GPM IVA resources loaded = {0}" , new Object[]{gpmSourceIVAProductCache.size()});
		}
	}
	
	private GPMSourceIVAProductContainer getGPMSourceIVAProductContainer(Long prodKey) throws Exception
	{
		GPMSourceIVAProductContainer container = this.gpmSourceIVAProductCache.get(prodKey);
		
		if (container == null)
		{
			container = this.gpmSourceIVAByProductFromUniverseClientAPI.execute(prodKey);
			
			if (container != null && container.prodKey == prodKey)
			{
				this.gpmSourceIVAProductCache.put(prodKey, container);
				
				container.indexByProdSourceKey();
			}
			else
			{
				logger.error("Failed to find GPM source IVA resource for product " + prodKey);
			}
		}
		
		return container;
	}
	
	//TODO can the Product container be fetched with only needed child records
	private void loadRequiredGPMClassificationResources() throws Exception
	{
		PerformanceTracker tracker = new PerformanceTracker(logger, Level.INFO, "loadRequiredGPMClassificationResources");

		tracker.start();
		
		try
		{
			for (Long prodKey : this.gpmClassificationResourceRegistration.keySet())
			{
				for (WorkPackage relatedPackage : this.gpmClassificationResourceRegistration.get(prodKey))
				{
					try
					{
						relatedPackage.gpmClassificationProductContainer = this.getGPMClassificationProductContainer(prodKey);
					}
					catch (Exception e)
					{
						relatedPackage.failure = e;
					}
				}
			}
			
			for (Long prodKey : this.gpmClassificationResourceCompRegistration.keySet())
			{
				for (CompWorkPackage relatedPackage : this.gpmClassificationResourceCompRegistration.get(prodKey))
				{
					try
					{
						relatedPackage.gpmClassificationProductContainer = this.getGPMClassificationProductContainer(prodKey);
					}
					catch (Exception e)
					{
						relatedPackage.failure = e;
					}
				}
			}
		}
		finally
		{
			tracker.stop("GPM Classification resources loaded = {0}" , new Object[]{gpmClassificationCache.size()});
		}
	}
	
	//TODO can the Product container be fetched with only needed child records
	private GPMClassificationProductContainer getGPMClassificationProductContainer(Long prodKey) throws Exception
	{
		GPMClassificationProductContainer product = this.gpmClassificationCache.get(prodKey);
		
		if (product == null)
		{
			product = this.gpmClassificationsByProductAPI.execute(prodKey);
			
			if (product != null && product.prodKey == prodKey)
			{
				this.gpmClassificationCache.put(prodKey, product);
				
				product.indexByCtryCmplKey();
			}
			else
			{
				logger.error("Failed to find GPM classification resource data for prod " + prodKey);
			}
		}
		
		return product;
	}
	
	//TODO can the BOM be fetched with only the required BOMComp
	private void loadRequiredBOMResources() throws Exception
	{
		PerformanceTracker tracker = new PerformanceTracker(logger, Level.INFO, "loadRequiredBOMResources");
		
		tracker.start();
		
		try
		{
			for (Long bomKey : this.workByBOMMap.keySet())
			{
				try
				{
					BOM bom = this.bomUniverseBOMClientAPI.execute(bomKey);
					 
					if(bom == null) throw new Exception("Bom not found for the key : "+bomKey);
					bom.indexByAltKeyBOMComponent();
					
					for (WorkPackage workPackage : this.workByBOMMap.get(bomKey).values())
					{
						workPackage.setBOM(bom);
					}
				}
				catch (Exception e)
				{
					for (WorkPackage workPackage : this.workByBOMMap.get(bomKey).values())
					{
						workPackage.failure = e;
					}
				}
			}
		}
		finally
		{
			tracker.stop("BOM resources loaded = {0}" , new Object[]{this.workByBOMMap.size()});
		}
	}
	
	private int updateInitWorkToPending(long bestTime) throws SQLException
	{
		PerformanceTracker tracker = new PerformanceTracker(logger, Level.INFO, "updateInitWorkToPending");
		int rowsAffected = 0;
		
		tracker.start();
		
		try
		{
			rowsAffected = this.template.update("update ar_qtx_work_status set status=? where status=? ", TrackerCodes.QualtxStatus.PENDING.ordinal(), TrackerCodes.QualtxStatus.INIT.ordinal());
			
			logger.debug("QTXWorkStaging init records found = " + rowsAffected);
		}
		finally
		{
			tracker.stop("ar_qtx_work_status records affected = {0}" , new Object[]{rowsAffected});
		}
		
		return rowsAffected;
	}
	
	private String getProducerSQL()
	{
		return "select AR_QTX_WORK.QTX_WID,AR_QTX_WORK.COMPANY_CODE,AR_QTX_WORK.PRIORITY,AR_QTX_WORK.BOM_KEY,AR_QTX_WORK.IVA_KEY,AR_QTX_WORK.ENTITY_KEY, "
				+ "AR_QTX_WORK.ENTITY_TYPE,AR_QTX_WORK.USER_ID,AR_QTX_WORK.TIME_STAMP, "
				+ "AR_QTX_WORK_STATUS.STATUS, "
				+ "AR_QTX_WORK_DETAILS.QUALTX_KEY,AR_QTX_WORK_DETAILS.ANALYSIS_METHOD,AR_QTX_WORK_DETAILS.COMPONENTS, "
				+ "AR_QTX_WORK_DETAILS.REASON_CODE,AR_QTX_WORK_DETAILS.CTRY_OF_IMPORT "
				+ "from AR_QTX_WORK_STATUS "
				+ "join AR_QTX_WORK ON AR_QTX_WORK.QTX_WID = AR_QTX_WORK_STATUS.QTX_WID "
				+ "join AR_QTX_WORK_DETAILS ON AR_QTX_WORK.QTX_WID = AR_QTX_WORK_DETAILS.QTX_WID "
				+ "WHERE STATUS=? "
				+ "ORDER BY AR_QTX_WORK.PRIORITY, AR_QTX_WORK.TIME_STAMP desc";
	}
}
