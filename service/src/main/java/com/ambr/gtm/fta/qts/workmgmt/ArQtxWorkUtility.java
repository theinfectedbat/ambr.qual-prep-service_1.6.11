package com.ambr.gtm.fta.qts.workmgmt;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;

import com.ambr.gtm.fta.qps.bom.BOMMetricSetUniverseContainer;
import com.ambr.gtm.fta.qps.bom.BOMUniverse;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTX;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponent;
import com.ambr.gtm.fta.qts.QTXCompWork;
import com.ambr.gtm.fta.qts.QTXCompWorkHS;
import com.ambr.gtm.fta.qts.QTXCompWorkIVA;
import com.ambr.gtm.fta.qts.QTXConsolWork;
import com.ambr.gtm.fta.qts.QTXWork;
import com.ambr.gtm.fta.qts.QTXWorkHS;
import com.ambr.gtm.fta.qts.QTXWorkRepository;
import com.ambr.gtm.fta.qts.ReQualificationReasonCodes;
import com.ambr.gtm.fta.qts.TrackerCodes;
import com.ambr.gtm.fta.qts.config.QEConfig;
import com.ambr.gtm.fta.qts.config.QEConfigCache;
import com.ambr.gtm.fta.qts.RequalificationWorkCodes;

public class ArQtxWorkUtility
{
	private static final Logger logger = LogManager.getLogger(ArQtxWorkUtility.class);

	public static final int			WORK_PRIORITY			= 1;

	public static final int			INT_WORK_STATUS			= 1;

	public static final int			READY_TO_QUALIFY_STATUS	= 5;
	
	private QTXWorkRepository workRepository;
	private JdbcTemplate template;
	private BOMUniverse bomUniverse;
	private QEConfigCache qeConfigCache;
	
	public ArQtxWorkUtility(QTXWorkRepository workRepository, JdbcTemplate template, BOMUniverse bomUniverse, QEConfigCache qeConfigCache)
	{
		this.workRepository = workRepository;
		this.template = template;
		this.bomUniverse = bomUniverse;
		this.qeConfigCache = qeConfigCache;
	}

	public List<QualTX> getImpactedQtxKeys(Long altKeyBom) throws Exception
	{
		String sql = "SELECT ALT_KEY_QUALTX,PROD_SRC_IVA_KEY,PROD_KEY, USER_ID, ORG_CODE from MDI_QUALTX where SRC_KEY = ?";
		
		SimpleDataLoaderResultSetExtractor<QualTX> extractor = new SimpleDataLoaderResultSetExtractor<QualTX>(QualTX.class);
		List<QualTX> qualtxList = this.template.query(sql, new Object[] {altKeyBom}, extractor);

		return qualtxList;
	}

	public List<Long> getImpactedQtxKeysForMass(ArrayList<Long> altKeyList, long reasonCode, ArrayList<String> ftaList) throws Exception
	{
		StringBuilder sql = new StringBuilder("SELECT DISTINCT ALT_KEY_QUALTX from MDI_QUALTX WHERE ");

		ArrayList<Object> paramList = new ArrayList<>();
		if (reasonCode == ReQualificationReasonCodes.BOM_MASS_QUALIFICATION)
		{
			sql.append(this.getSimpleClause("SRC_KEY", "=", "OR", altKeyList.size()));
			if (null != ftaList && !ftaList.isEmpty()) sql.append(" AND " + this.getSimpleClause("FTA_CODE", "=", "OR", ftaList.size()));
		}
		paramList.addAll(altKeyList);
		paramList.addAll(ftaList);

		List<Long> data = this.template.query(sql.toString(),paramList.toArray(), new RowMapper<Long>(){
            public Long mapRow(ResultSet rs, int rowNum) 
                                         throws SQLException {
                    return rs.getLong(1);
            }
       });
		
		return data;
	}
	
	public List<QualTX> getImpactedQtxKeys(ArrayList<Long> altKeyList, long reasonCode) throws Exception
	{
		StringBuilder sql = new StringBuilder("SELECT DISTINCT ALT_KEY_QUALTX, PROD_SRC_IVA_KEY,PROD_KEY,PROD_SRC_KEY,PROD_CTRY_CMPL_KEY,SRC_KEY,SUB_PULL_CTRY,HS_NUM, ORG_CODE, IVA_CODE, CTRY_OF_IMPORT, FTA_CODE,CREATED_DATE from MDI_QUALTX WHERE ");

		if (reasonCode == ReQualificationReasonCodes.GPM_CTRY_CMPL_CHANGE) sql.append(this.getSimpleClause("PROD_CTRY_CMPL_KEY", "=", "OR", altKeyList.size()));
		if (reasonCode == ReQualificationReasonCodes.GPM_CTRY_CMPL_DELETED) sql.append(this.getSimpleClause("PROD_CTRY_CMPL_KEY", "=", "OR", altKeyList.size()));
		if (reasonCode == ReQualificationReasonCodes.GPM_IVA_AND_CLAIM_DTLS_CHANGE) sql.append(this.getSimpleClause("PROD_SRC_IVA_KEY", "=", "OR", altKeyList.size()));
		if (reasonCode == ReQualificationReasonCodes.GPM_SRC_IVA_DELETED) sql.append(this.getSimpleClause("PROD_SRC_IVA_KEY", "=", "OR", altKeyList.size()));
		if (reasonCode == ReQualificationReasonCodes.GPM_SRC_CHANGE) sql.append(this.getSimpleClause("PROD_SRC_KEY", "=", "OR", altKeyList.size()));
		if (reasonCode == ReQualificationReasonCodes.GPM_SRC_DELETED) sql.append(this.getSimpleClause("PROD_SRC_KEY", "=", "OR", altKeyList.size()));
		if (reasonCode == ReQualificationReasonCodes.GPM_IVA_CHANGE_M_I) sql.append(this.getSimpleClause("PROD_SRC_IVA_KEY", "=", "OR", altKeyList.size()));
		if (reasonCode == ReQualificationReasonCodes.BOM_QUAL_MPQ_CHG) sql.append(this.getSimpleClause("ALT_KEY_QUALTX", "=", "OR", altKeyList.size()));
		if (reasonCode == ReQualificationReasonCodes.GPM_CTRY_CMPL_ADDED) sql.append(this.getSimpleClause("PROD_SRC_IVA_KEY", "=", "OR", altKeyList.size()));

		SimpleDataLoaderResultSetExtractor<QualTX> extractor = new SimpleDataLoaderResultSetExtractor<QualTX>(QualTX.class);
		List<QualTX> qualtxList = this.template.query(sql.toString(), altKeyList.toArray(), extractor);

		return qualtxList;
	}

	public List<QualTX> getImpactedQtxCompKeys(ArrayList<Long> altKeyList, long reasonCode) throws Exception
	{
		StringBuilder sql = new StringBuilder("SELECT DISTINCT COMP.ALT_KEY_COMP AS COMP_QUALTX_KEY, COMP.ORG_CODE, COMP.HS_NUM AS COMP_HS_NUM, COMP.PROD_KEY AS COMP_PROD_KEY, "
				+ " COMP.PROD_SRC_KEY AS COMP_PROD_SRC_KEY, COMP.PROD_SRC_IVA_KEY AS COMP_PROD_SRC_IVA_KEY, COMP.SUB_PULL_CTRY AS COMP_SUB_PULL_CTRY,"
				+ " COMP.PROD_CTRY_CMPL_KEY AS COMP_PROD_CTRY_CMPL_KEY, COMP.SRC_KEY AS COMP_KEY, COMP.SRC_ID AS COMP_ID , QUALTX.ALT_KEY_QUALTX, QUALTX.PROD_SRC_IVA_KEY, QUALTX.PROD_KEY,"
				+ " QUALTX.PROD_SRC_KEY, QUALTX.PROD_CTRY_CMPL_KEY, QUALTX.SRC_KEY AS BOM_KEY, QUALTX.SUB_PULL_CTRY, QUALTX.IVA_CODE as HEADER_IVA_CODE, QUALTX.FTA_CODE as HEADER_FTA_CODE, QUALTX.CREATED_DATE AS QUALTX_CREATED_DATE, QUALTX.CTRY_OF_IMPORT as HEADER_CTRY_OF_IMPORT "
				+ " FROM MDI_QUALTX_COMP COMP JOIN MDI_QUALTX QUALTX "
				+ " ON (QUALTX.ALT_KEY_QUALTX = COMP.ALT_KEY_QUALTX) WHERE ");

		if (reasonCode == ReQualificationReasonCodes.GPM_CTRY_CMPL_CHANGE) sql.append(this.getSimpleClause("COMP.PROD_CTRY_CMPL_KEY", "=", "OR", altKeyList.size()));
		else if (reasonCode == ReQualificationReasonCodes.GPM_CTRY_CMPL_ADDED) sql.append(this.getSimpleClause("COMP.PROD_SRC_IVA_KEY", "=", "OR", altKeyList.size()));
		else if (reasonCode == ReQualificationReasonCodes.GPM_CTRY_CMPL_DELETED) sql.append(this.getSimpleClause("COMP.PROD_CTRY_CMPL_KEY", "=", "OR", altKeyList.size()));
		else if (reasonCode == ReQualificationReasonCodes.GPM_IVA_AND_CLAIM_DTLS_CHANGE) sql.append(this.getSimpleClause("COMP.PROD_SRC_IVA_KEY", "=", "OR", altKeyList.size()));
		else if (reasonCode == ReQualificationReasonCodes.GPM_SRC_IVA_DELETED) sql.append(this.getSimpleClause("COMP.PROD_SRC_IVA_KEY", "=", "OR", altKeyList.size()));
		else if (reasonCode == ReQualificationReasonCodes.GPM_SRC_CHANGE) sql.append(this.getSimpleClause("COMP.PROD_SRC_KEY", "=", "OR", altKeyList.size()));
		else if (reasonCode == ReQualificationReasonCodes.GPM_SRC_DELETED) sql.append(this.getSimpleClause("COMP.PROD_SRC_KEY", "=", "OR", altKeyList.size()));
		else if (reasonCode == ReQualificationReasonCodes.GPM_SRC_ADDED) sql.append(this.getSimpleClause("COMP.PROD_KEY", "=", "OR", altKeyList.size()));
		else if (reasonCode == ReQualificationReasonCodes.BOM_COMP_MODIFIED) sql.append(this.getSimpleClause("COMP.SRC_KEY", "=", "OR", altKeyList.size()));
		else if (reasonCode == ReQualificationReasonCodes.BOM_COMP_DELETED) sql.append(this.getSimpleClause("COMP.SRC_KEY", "=", "OR", altKeyList.size()));
		else if (reasonCode == ReQualificationReasonCodes.BOM_COMP_ADDED) sql.append(this.getSimpleClause("COMP.SRC_KEY", "=", "OR", altKeyList.size()));
		else if (reasonCode == ReQualificationReasonCodes.BOM_COMP_YARN_DTLS_CHG) sql.append(this.getSimpleClause("COMP.SRC_KEY", "=", "OR", altKeyList.size()));
		else if (reasonCode == ReQualificationReasonCodes.BOM_COMP_SRC_CHG) sql.append(this.getSimpleClause("COMP.SRC_KEY", "=", "OR", altKeyList.size()));
		else if (reasonCode == ReQualificationReasonCodes.GPM_COMP_FINAL_DECISION_CHANGE) sql.append(this.getSimpleClause("COMP.PROD_SRC_IVA_KEY", "=", "OR", altKeyList.size()));
		else if (reasonCode == ReQualificationReasonCodes.GPM_IVA_CHANGE_M_I) sql.append(this.getSimpleClause("COMP.PROD_SRC_IVA_KEY", "=", "OR", altKeyList.size()));
		else if (reasonCode == ReQualificationReasonCodes.GPM_COMP_CUMULATION_CHANGE) sql.append(this.getSimpleClause("COMP.PROD_SRC_IVA_KEY", "=", "OR", altKeyList.size()));
		else if (reasonCode == ReQualificationReasonCodes.GPM_COMP_TRACE_VALUE_CHANGE) sql.append(this.getSimpleClause("COMP.PROD_SRC_IVA_KEY", "=", "OR", altKeyList.size()));
		else if (reasonCode == ReQualificationReasonCodes.GPM_COMP_PREV_YEAR_QUAL_CHANGE) sql.append(this.getSimpleClause("COMP.SRC_KEY", "=", "OR", altKeyList.size()));
		else if (reasonCode == ReQualificationReasonCodes.BOM_COMP_PRC_CHG) sql.append(this.getSimpleClause("COMP.SRC_KEY", "=", "OR", altKeyList.size()));
		else if (reasonCode == ReQualificationReasonCodes.BOM_COMP_COO_CHG) sql.append(this.getSimpleClause("COMP.SRC_KEY", "=", "OR", altKeyList.size()));
		else if (reasonCode == ReQualificationReasonCodes.BOM_COMP_COM_COO_CHG) sql.append(this.getSimpleClause("COMP.SRC_KEY", "=", "OR", altKeyList.size()));
		else if (reasonCode == ReQualificationReasonCodes.STP_COO_CHG) sql.append(this.getSimpleClause("COMP.PROD_KEY", "=", "OR", altKeyList.size()));
		else if (reasonCode == ReQualificationReasonCodes.GPM_COO_CHG) sql.append(this.getSimpleClause("COMP.PROD_KEY", "=", "OR", altKeyList.size()));
		else if (reasonCode == ReQualificationReasonCodes.BOM_COMP_PREV_YEAR_QUAL_CHANGE) sql.append(this.getSimpleClause("COMP.SRC_KEY", "=", "OR", altKeyList.size()));
		if (reasonCode != ReQualificationReasonCodes.GPM_IVA_CHANGE_M_I) sql.append(" AND (COMP.IS_ACTIVE = 'Y' OR COMP.IS_ACTIVE is null)");

		List<QualTX> list = new ArrayList<QualTX>();
			
		logger.debug("getImpactedQtxCompKeys reason code " + reasonCode + " key size " + altKeyList.size());
		
		this.template.query(sql.toString(), altKeyList.toArray(), new RowCallbackHandler() {
	        @Override
	        public void processRow(ResultSet resultSet) throws SQLException {
	        	try
	        	{
		        	QualTX qualtx = new QualTX();
		        	QualTXComponent qualtxComp = new QualTXComponent();
		        	
		        	qualtx.org_code = resultSet.getString("ORG_CODE");
		        	qualtx.alt_key_qualtx =  DataLoader.getLong(resultSet, "ALT_KEY_QUALTX");
	
		        	qualtx.prod_src_iva_key = DataLoader.getLong(resultSet, "PROD_SRC_IVA_KEY");
		    		qualtx.prod_key =  DataLoader.getLong(resultSet, "PROD_KEY");
		    		qualtx.prod_src_key =  DataLoader.getLong(resultSet, "PROD_SRC_KEY");
		    		qualtx.prod_ctry_cmpl_key =  DataLoader.getLong(resultSet, "PROD_CTRY_CMPL_KEY");
		    		qualtx.src_key =  DataLoader.getLong(resultSet, "BOM_KEY");
		    		qualtx.sub_pull_ctry = resultSet.getString("SUB_PULL_CTRY");
		    		qualtx.iva_code = resultSet.getString("HEADER_IVA_CODE");
		    		qualtx.fta_code = resultSet.getString("HEADER_FTA_CODE");
		    		qualtx.ctry_of_import = resultSet.getString("HEADER_CTRY_OF_IMPORT");
		    		qualtx.created_date = resultSet.getTimestamp("QUALTX_CREATED_DATE");	

		        	qualtxComp.alt_key_qualtx = qualtx.alt_key_qualtx;
		        	qualtxComp.org_code = qualtx.org_code;
		    		qualtxComp.alt_key_comp = DataLoader.getLong(resultSet, "COMP_QUALTX_KEY");
		    		
		    		qualtxComp.hs_num = resultSet.getString("COMP_HS_NUM");
		    		qualtxComp.prod_key =  DataLoader.getLong(resultSet, "COMP_PROD_KEY");
		    		qualtxComp.prod_src_key =  DataLoader.getLong(resultSet, "COMP_PROD_SRC_KEY");
		    		qualtxComp.prod_src_iva_key =  DataLoader.getLong(resultSet, "COMP_PROD_SRC_IVA_KEY");
		    		qualtxComp.sub_pull_ctry = resultSet.getString("COMP_SUB_PULL_CTRY");
		    		qualtxComp.prod_ctry_cmpl_key =  DataLoader.getLong(resultSet, "COMP_PROD_CTRY_CMPL_KEY");
		    		qualtxComp.src_key =  DataLoader.getLong(resultSet, "COMP_KEY");
		    		qualtxComp.src_id = resultSet.getString("COMP_ID");
		    		qualtx.compList.add(qualtxComp);
		    		
		    		list.add(qualtx);
	        	}
	        	catch (Exception e)
	        	{
	        		throw new SQLException("Failed to load record", e);
	        	}
	        }
	    });
		
		return list;
	}

	public List<QualTX> getImpactedQtxCompKeysForNewComp(ArrayList<Long> altKeyList, long reasonCode) throws Exception
	{
		StringBuilder sql = new StringBuilder("SELECT QUALTX.ALT_KEY_QUALTX, QUALTX.ORG_CODE, QUALTX.PROD_SRC_KEY,  QUALTX.SRC_KEY AS BOM_KEY, QUALTX.PROD_SRC_IVA_KEY, QUALTX.PROD_KEY, BOM_COMP.ALT_KEY_COMP  AS COMP_KEY, " + " BOM_COMP.PROD_KEY AS COMP_PROD_KEY, BOM_COMP.PROD_SRC_KEY AS COMP_PROD_SRC_KEY, QUALTX.IVA_CODE AS HEADER_IVA_CODE, QUALTX.FTA_CODE AS HEADER_FTA_CODE, QUALTX.CREATED_DATE AS QUALTX_CREATED_DATE, QUALTX.CTRY_OF_IMPORT AS HEADER_CTRY_OF_IMPORT " + " FROM MDI_QUALTX QUALTX JOIN MDI_BOM_COMP BOM_COMP ON ( QUALTX.SRC_KEY = BOM_COMP.ALT_KEY_BOM ) WHERE ");

		if (reasonCode == ReQualificationReasonCodes.BOM_COMP_ADDED) sql.append(this.getSimpleClause("BOM_COMP.ALT_KEY_COMP", "=", "OR", altKeyList.size()));

		List<QualTX> list = new ArrayList<QualTX>();

		logger.debug("getImpactedQtxCompKeysForNewComp  key size " + altKeyList.size());

		this.template.query(sql.toString(), altKeyList.toArray(), new RowCallbackHandler()
		{
			@Override
			public void processRow(ResultSet resultSet) throws SQLException
			{
				try
				{
					QualTX qualtx = new QualTX();
					QualTXComponent qualtxComp = new QualTXComponent();

					qualtx.org_code = resultSet.getString("ORG_CODE");
					qualtx.alt_key_qualtx = DataLoader.getLong(resultSet, "ALT_KEY_QUALTX");

					qualtx.prod_src_iva_key = DataLoader.getLong(resultSet, "PROD_SRC_IVA_KEY");
					qualtx.prod_key = DataLoader.getLong(resultSet, "PROD_KEY");
					qualtx.prod_src_key = DataLoader.getLong(resultSet, "PROD_SRC_KEY");
					qualtx.src_key = DataLoader.getLong(resultSet, "BOM_KEY");
					qualtx.iva_code = resultSet.getString("HEADER_IVA_CODE");
					qualtx.fta_code = resultSet.getString("HEADER_FTA_CODE");
					qualtx.ctry_of_import = resultSet.getString("HEADER_CTRY_OF_IMPORT");
					qualtx.created_date = resultSet.getTimestamp("QUALTX_CREATED_DATE");	

					qualtxComp.alt_key_qualtx = qualtx.alt_key_qualtx;
					qualtxComp.org_code = qualtx.org_code;
					qualtxComp.prod_key = DataLoader.getLong(resultSet, "COMP_PROD_KEY");
					qualtxComp.prod_src_key = DataLoader.getLong(resultSet, "COMP_PROD_SRC_KEY");
					qualtxComp.src_key = DataLoader.getLong(resultSet, "COMP_KEY");

					qualtx.compList.add(qualtxComp);

					list.add(qualtx);
				}
				catch (Exception e)
				{
					throw new SQLException("Failed to load record", e);
				}
			}
		});

		return list;
	}

	public List<QualTXComponent> getImpactedQtxCompKeys(ArrayList<Long> altKeyCompList) throws Exception
	{
		String sql = "SELECT alt_key_qualtx, ALT_KEY_COMP, ORG_CODE, PROD_KEY, PROD_SRC_KEY, PROD_SRC_IVA_KEY,SUB_PULL_CTRY,PROD_KEY,PROD_CTRY_CMPL_KEY from MDI_QUALTX_COMP where " + this.getSimpleClause("SRC_KEY", "=", "OR", altKeyCompList.size()) + " AND (COMP.IS_ACTIVE = 'Y' OR COMP.IS_ACTIVE is null) ";
		SimpleDataLoaderResultSetExtractor<QualTXComponent> extractor = new SimpleDataLoaderResultSetExtractor<QualTXComponent>(QualTXComponent.class);
		List<QualTXComponent> qualtxComponentList = this.template.query(sql.toString(), altKeyCompList.toArray(), extractor);

		return qualtxComponentList;
	}
	
	public List<QualTX> getImpactedQtxKeys(ArrayList<Long> altKeyBoms) throws Exception
	{
		StringBuilder sql  = new StringBuilder("SELECT ALT_KEY_QUALTX, PROD_SRC_IVA_KEY, PROD_KEY, SRC_KEY, IVA_CODE, CTRY_OF_IMPORT, ORG_CODE, FTA_CODE,CREATED_DATE from MDI_QUALTX WHERE ");
		sql.append(this.getSimpleClause("SRC_KEY", "=", "OR", altKeyBoms.size()));

		SimpleDataLoaderResultSetExtractor<QualTX> extractor = new SimpleDataLoaderResultSetExtractor<QualTX>(QualTX.class);
		List<QualTX> qualtxList = this.template.query(sql.toString(), altKeyBoms.toArray(), extractor);

		return qualtxList;
	}
	
	private String getSimpleClause(String columnName, String conditionOperator, String booleanOperator, int conditionCount)
	{
		StringBuilder buffer = new StringBuilder("(");
		
		for (int i=0; i<conditionCount; i++)
		{
			if (i>0)
				buffer.append(" " + booleanOperator + " ");
			buffer.append(columnName).append(conditionOperator).append("?");
		}
		
		buffer.append(")");
		
		return buffer.toString();
	}

	public QTXWork createQtxWorkObj(QualTX qualtx, long reasonCode, Map<Long, QTXConsolWork> bomConsolMap, long key) throws Exception
	{
		QTXWork work =this.workRepository.createWork();
		QTXConsolWork qtxConsolWork = null;
		if(bomUniverse == null )
		{
			logger.error("ArQtxWorkUtility: bomUniverse is not initalized properly, qualtx "+qualtx + " and the key : "+key);
			work.priority = 1; //TODO Need to remove the default priority 
		}

		work.bom_key = qualtx.src_key;
		work.company_code = qualtx.org_code;
		work.entity_key  = qualtx.prod_key;
		work.entity_type = QTXWork.ENTITY_TYPE_PRODUCT;
		work.iva_key = qualtx.prod_src_iva_key;
		work.createDetails();
		work.details.qualtx_key = qualtx.alt_key_qualtx;
		work.details.setReasonCodeFlag(reasonCode);
		if(null != bomConsolMap)
		{
			qtxConsolWork = bomConsolMap.get(key);
			
			if(qtxConsolWork == null ) //TODO work arround
			{
				logger.error("@@@@@@ArQtxWorkUtility: qtxConsolWork, qualtx:BOM key= "+qualtx.src_key +": BOM ID="+ qualtx.src_id+ " and  key : "+key + "reasonCode:"+reasonCode + "bomConsolMap key set"+bomConsolMap.keySet());
				work.priority = 1; //TODO Need to remove the default priority 
				qtxConsolWork = new QTXConsolWork();
				qtxConsolWork.time_stamp =  new Timestamp(System.currentTimeMillis());
				qtxConsolWork.user_id = (qualtx.last_modified_by == null) ? "SYSTEM_ADMIN" : qualtx.last_modified_by;
			}	
			else work.priority = qtxConsolWork.priority; //Since we are setting the default priority as 70 in GPM, we are using the same priority.

			work.time_stamp = qtxConsolWork.time_stamp;
			work.status.time_stamp = qtxConsolWork.time_stamp;
			work.details.time_stamp = qtxConsolWork.time_stamp;
			work.userId = qtxConsolWork.user_id;
		}
		
		/*QEConfig  QEConfig = qeConfigCache.getQEConfig(qualtx.org_code);
		if(QEConfig != null && QEConfig.getAnalysisMethod() != null )
		{ 
			String analysisMethod =  QEConfig.getAnalysisMethod() ;
			work.details.analysis_method = TrackerCodes.AnalysisMethod.values()[TrackerCodes.AnalysisMethodFromConfig.valueOf(analysisMethod).ordinal()];
		}*/
		work.details.analysis_method = TrackerCodes.AnalysisMethod.TOP_DOWN_ANALYSIS; //Always Top Down approach
		
		return work;
	}

	public QTXCompWork createQtxCompWorkObj(QualTX qualtx, QualTXComponent qualtxComp, long reasonCode, long workId) throws Exception
	{
		QTXCompWork compWork =this.workRepository.createCompWork(workId);
		
		compWork.bom_comp_key = qualtxComp.src_key;
		compWork.bom_key = qualtx.src_key;
		compWork.entity_key = qualtxComp.prod_key;
		compWork.entity_src_key = qualtxComp.prod_src_key;
		compWork.qualtx_comp_key = qualtxComp.alt_key_comp;
		compWork.qualtx_key = qualtxComp.alt_key_qualtx;
		compWork.priority = 1;
		compWork.qualifier= TrackerCodes.QualtxCompQualifier.CREATE_COMP;
		compWork.setReasonCodeFlag(reasonCode);
	
		return compWork;
	}

	public QTXCompWorkIVA createQtxCompIVAWorkObj(QualTXComponent qualtxComp, long reasonCode, long compWorkId, long workId) throws Exception
	{
		QTXCompWorkIVA compWorkIVA = this.workRepository.createCompWorkIVA(compWorkId, workId);

		compWorkIVA.iva_key = qualtxComp.prod_src_iva_key;
		compWorkIVA.setReasonCodeFlag(reasonCode);
		
		return compWorkIVA;
	}

	public QTXCompWorkHS createQtxCompHSWorkObj(QualTXComponent qualtxComp, long reasonCode, long compWorkId, long workId) throws Exception
	{
		QTXCompWorkHS compWorkHS = this.workRepository.createCompWorkHS(compWorkId, workId);

		compWorkHS.ctry_cmpl_key = qualtxComp.prod_ctry_cmpl_key;
		compWorkHS.setReasonCodeFlag(reasonCode);

		return compWorkHS;
	}
	
	public QTXWorkHS createQtxHSWorkObj(QualTX qualtx, long reasonCode, long workId) throws Exception
	{
		QTXWorkHS hsWork = this.workRepository.createWorkHS(workId);
		
		hsWork.ctry_cmpl_key = qualtx.prod_ctry_cmpl_key;
		hsWork.setReasonCodeFlag(reasonCode);

		return hsWork;
	}

	public long getQtxWorkReasonCodes(long reasonCode)
	{
		long workCode = 0;
		
		if (reasonCode == ReQualificationReasonCodes.BOM_PRC_CHG) workCode = RequalificationWorkCodes.BOM_PRC_CHG;
		else if (reasonCode == ReQualificationReasonCodes.BOM_HDR_CHG) workCode = RequalificationWorkCodes.BOM_HDR_CHG;
		else if (reasonCode == ReQualificationReasonCodes.BOM_PROD_TXT_DE) workCode = RequalificationWorkCodes.BOM_PROD_TXT_DE;
		else if (reasonCode == ReQualificationReasonCodes.BOM_PROD_AUTO_DE) workCode = RequalificationWorkCodes.BOM_PROD_AUTO_DE;
		else if (reasonCode == ReQualificationReasonCodes.BOM_QUAL_MPQ_CHG) workCode = RequalificationWorkCodes.BOM_QUAL_MPQ_CHG;
		else if (reasonCode == ReQualificationReasonCodes.BOM_TXREF_CHG) workCode = RequalificationWorkCodes.BOM_TXREF_CHG;
		else if (reasonCode == ReQualificationReasonCodes.BOM_COMP_ADDED) workCode = RequalificationWorkCodes.BOM_COMP_ADDED;
		else if (reasonCode == ReQualificationReasonCodes.BOM_COMP_DELETED) workCode = RequalificationWorkCodes.BOM_COMP_DELETED;
		else if (reasonCode == ReQualificationReasonCodes.BOM_COMP_MODIFIED) workCode = RequalificationWorkCodes.BOM_COMP_MODIFIED;
		else if (reasonCode == ReQualificationReasonCodes.BOM_COMP_YARN_DTLS_CHG) workCode = RequalificationWorkCodes.BOM_COMP_YARN_DTLS_CHG;
		else if (reasonCode == ReQualificationReasonCodes.BOM_COMP_SRC_CHG) workCode = RequalificationWorkCodes.BOM_COMP_SRC_CHG;
		else if (reasonCode == ReQualificationReasonCodes.GPM_IVA_AND_CLAIM_DTLS_CHANGE) workCode = RequalificationWorkCodes.GPM_IVA_AND_CLAIM_DTLS_CHANGE;
		else if (reasonCode == ReQualificationReasonCodes.GPM_NEW_HEADER_IVA_IDENTIFED) workCode = RequalificationWorkCodes.GPM_NEW_HEADER_IVA_IDENTIFED;
		else if (reasonCode == ReQualificationReasonCodes.GPM_NEW_IVA_IDENTIFED) workCode = RequalificationWorkCodes.GPM_NEW_IVA_IDENTIFED;
		else if (reasonCode == ReQualificationReasonCodes.GPM_SRC_IVA_DELETED) workCode = RequalificationWorkCodes.GPM_SRC_IVA_DELETED;
		else if (reasonCode == ReQualificationReasonCodes.GPM_SRC_CHANGE) workCode = RequalificationWorkCodes.GPM_SRC_CHANGE;
		else if (reasonCode == ReQualificationReasonCodes.GPM_SRC_DELETED) workCode = RequalificationWorkCodes.GPM_SRC_DELETED;
		else if (reasonCode == ReQualificationReasonCodes.GPM_SRC_ADDED) workCode = RequalificationWorkCodes.GPM_SRC_ADDED;
		else if (reasonCode == ReQualificationReasonCodes.GPM_CTRY_CMPL_CHANGE) workCode = RequalificationWorkCodes.GPM_CTRY_CMPL_CHANGE;
		else if (reasonCode == ReQualificationReasonCodes.GPM_CTRY_CMPL_DELETED) workCode = RequalificationWorkCodes.GPM_CTRY_CMPL_DELETED;
		else if (reasonCode == ReQualificationReasonCodes.GPM_CTRY_CMPL_ADDED) workCode = RequalificationWorkCodes.GPM_CTRY_CMPL_ADDED;
		else if (reasonCode == ReQualificationReasonCodes.GPM_IVA_CHANGE_M_I) workCode = RequalificationWorkCodes.GPM_IVA_CHANGE_M_I;
		else if (reasonCode == ReQualificationReasonCodes.GPM_COMP_FINAL_DECISION_CHANGE) workCode = RequalificationWorkCodes.GPM_COMP_FINAL_DECISION_CHANGE;
		else if (reasonCode == ReQualificationReasonCodes.GPM_COMP_PREV_YEAR_QUAL_CHANGE) workCode = RequalificationWorkCodes.GPM_COMP_PREV_YEAR_QUAL_CHANGE;
		else if (reasonCode == ReQualificationReasonCodes.GPM_COMP_CUMULATION_CHANGE) workCode = RequalificationWorkCodes.GPM_COMP_CUMULATION_CHANGE;
		else if (reasonCode == ReQualificationReasonCodes.GPM_COMP_TRACE_VALUE_CHANGE) workCode = RequalificationWorkCodes.GPM_COMP_TRACE_VALUE_CHANGE;
		else if (reasonCode == ReQualificationReasonCodes.BOM_COMP_PRC_CHG) workCode = RequalificationWorkCodes.COMP_PRC_CHG;
		else if (reasonCode == ReQualificationReasonCodes.BOM_COMP_COO_CHG) workCode = RequalificationWorkCodes.BOM_COMP_COO_CHG;
		else if (reasonCode == ReQualificationReasonCodes.BOM_COMP_COM_COO_CHG) workCode = RequalificationWorkCodes.BOM_COMP_COM_COO_CHG;
		else if (reasonCode == ReQualificationReasonCodes.STP_COO_CHG) workCode = RequalificationWorkCodes.COMP_STP_COO_CHG;
		else if (reasonCode == ReQualificationReasonCodes.GPM_COO_CHG) workCode = RequalificationWorkCodes.COMP_GPM_COO_CHG;
		else if (reasonCode == ReQualificationReasonCodes.BOM_COMP_PREV_YEAR_QUAL_CHANGE) workCode = RequalificationWorkCodes.BOM_COMP_PREV_YEAR_QUAL_CHANGE;
		else if (reasonCode == ReQualificationReasonCodes.BOM_PRIORITIZE_QUALIFICATION) workCode = RequalificationWorkCodes.HEADER_CONFIG_CHANGE;

		return workCode;
	}
}
