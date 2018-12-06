package com.ambr.gtm.fta.qts.config;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowCallbackHandler;


import com.ambr.gtm.fta.qps.exception.MaxRowsReachedException;
import com.ambr.gtm.fta.qts.TrackerLoaderQueries;
import com.ambr.gtm.fta.qts.util.BomAnalysisConfigData;
import com.ambr.gtm.fta.qts.util.CompAnalysisConfData;
import com.ambr.gtm.fta.qts.util.CumulationConfigContainer;
import com.ambr.gtm.fta.qts.util.SubPullConfigContainer;
import com.ambr.gtm.fta.qts.util.SubPullConfigData;
import com.ambr.gtm.fta.qts.util.SubPullConfigData.BaseHSFallConfg;
import com.ambr.gtm.fta.qts.util.TradeLane;
import com.ambr.gtm.fta.qts.util.TradeLaneContainer;
import com.ambr.gtm.fta.qts.util.TradeLaneData;
import com.ambr.gtm.fta.trade.FTACountryContainer;
import com.ambr.gtm.fta.trade.FTAParticipatingCountrySet;
import com.ambr.gtm.utils.legacy.multiorg.Org;
import com.ambr.gtm.utils.legacy.multiorg.OrgCache;
import com.ambr.platform.utils.log.MessageFormatter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class QEConfigCache
{
	static Logger					logger				= LogManager.getLogger(QEConfigCache.class);

	private JdbcTemplate			theJdbcTemplate;
	private int						fetchSize;
	private FTACtryConfigCache		ftaCtryCache;
	private OrgCache				orgCache;
	private Map<String, QEConfig>	qeConfigMap			= new ConcurrentHashMap<String, QEConfig>();
	private Set<String>			reloadOrgList		= new HashSet<String>();
	
	public QEConfigCache(OrgCache orgCache, JdbcTemplate aJdbcTemplate, int fetchSize, FTACtryConfigCache ftaCtryCache)
	{
		this.theJdbcTemplate = aJdbcTemplate;
		this.fetchSize = fetchSize;
		this.orgCache = orgCache;
		this.ftaCtryCache = ftaCtryCache;
	}

	public QEConfigCache()
	{

	}

	public Set<String> getCountryListByFTA(String theOrgcode, String theFTACode) throws Exception
	{
		Set<String> ctryList = null;
		if (theFTACode != null && theOrgcode != null)
		{
			FTACountryContainer ftaCountryContainer = ftaCtryCache.getFTACountryContainer(theOrgcode);
			if (ftaCountryContainer != null)
			{
				Map<String, FTAParticipatingCountrySet> participatingCtryMap = ftaCountryContainer.getFtaParticipatingCtryMap();
				if (participatingCtryMap != null)
				{
					FTAParticipatingCountrySet ftaParticipatingCountrySet = participatingCtryMap.get(theFTACode);
					if (ftaParticipatingCountrySet != null) ctryList = ftaParticipatingCountrySet.countryList;
				}
			}
		}
		return ctryList;
	}

	public synchronized void flushQEConfigCache(String orgCode)
	{
		if (null != orgCode)
		{
			qeConfigMap.remove(orgCode);
			reloadOrgList.add(orgCode);
		}
	}

	public synchronized QEConfig getQEConfig(String orgCode) throws Exception
	{
		QEConfig qeConfig = this.qeConfigMap.get(orgCode);
		if (qeConfig == null)
		{
			if (reloadOrgList.contains(orgCode))
			{
				loadOrgQEConfigCache(orgCode);
				reloadOrgList.remove(orgCode);
				qeConfig = this.qeConfigMap.get(orgCode);
				if (qeConfig != null) return qeConfig;
			}
			
			Org org = this.orgCache.getOrg(orgCode);
			if (org == null || org.isCompany()) return getQEConfig("SYSTEM");
			else return getQEConfig(org.getParentOrgCode());
		}
		return qeConfig;
	}

	public boolean isTradeLaneEnabled(String orgCode, String ftaCode, String coi) throws Exception
	{
		List<TradeLane> tradeLaneList = getQEConfig(orgCode).getTradeLaneContainer().getAllTradeLaneList();
		if (tradeLaneList != null) { return tradeLaneList.stream().anyMatch(p -> p.getFtaCode().equalsIgnoreCase(ftaCode) && p.getCtryOfImport().equalsIgnoreCase(coi)); }
		return false;
	}

	public void loadOrgQEConfigCache(String orgCode) throws Exception
	{
		MessageFormatter.trace(logger, "loadOrgQEConfigCache", "Loading QE Config cache for all orgs - START");

		try
		{
			theJdbcTemplate.setFetchSize(fetchSize);

			theJdbcTemplate.query(TrackerLoaderQueries.orgQEConfigCache, new PreparedStatementSetter()
			{
				public void setValues(PreparedStatement ps) throws SQLException
				{
					ps.setString(1, orgCode);
				}
			}, new QEConfigCacheRowCallbackHandler());

		}
		catch (DataAccessException e)
		{
			if (!(e.getCause() instanceof MaxRowsReachedException))
			{
				MessageFormatter.error(logger, "loadOrgQEConfigCache", e, "Exception while loading QE Config cache for all orgs");
				throw e;
			}
		}

		MessageFormatter.trace(logger, "loadFullQEConfigCache", "Loading QE Config cache for all orgs - COMPLETE");
	}

	public void loadFullQEConfigCache() throws Exception
	{
		
		MessageFormatter.trace(logger, "loadFullQEConfigCache", "Loading QE Config cache for all orgs - START");
	
		if (!qeConfigMap.isEmpty()) return;

		try
		{
			theJdbcTemplate.setFetchSize(fetchSize);

			theJdbcTemplate.query(TrackerLoaderQueries.fullCacheLoadQuery, new QEConfigCacheRowCallbackHandler());
			MessageFormatter.info(logger, "loadFullQEConfigCache", "Load QE Config cache for all orgs - COMPLETE");
		}
		catch (DataAccessException e)
		{
			if (!(e.getCause() instanceof MaxRowsReachedException))
			{
				MessageFormatter.error(logger, "loadFullQEConfigCache", e, "Exception while loading QE Config cache for all orgs");
				throw e;
			}
		}

		MessageFormatter.trace(logger, "loadFullQEConfigCache", "Loading QE Config cache for all orgs - COMPLETE");
	}


	public void addTradeLaneContainer(String orgCode, TradeLaneContainer container)
	{
		qeConfigMap.get(orgCode).setTradeLaneContainer(container);
	}

	

	private BomAnalysisConfigData loadAnalysisConf(JsonNode aQEConfigJsonObject) throws Exception
	{

		MessageFormatter.trace(logger, "loadAnalysisConf", "Loading  analysis Config cache for FTA_ANALYSIS START");

		BomAnalysisConfigData bomConf = new BomAnalysisConfigData();
		JsonNode analysisConfJson = aQEConfigJsonObject.get("BOM");
		if (analysisConfJson == null) return bomConf;

		for (JsonNode eachNode : analysisConfJson)
		{
			if (eachNode.has("FTA_ANALYSIS") && eachNode.get("FTA_ANALYSIS").isArray())
			{
				for (JsonNode ftaAnalysisNode : eachNode.get("FTA_ANALYSIS"))
				{
					Map<String, String> ftaanalysis = new HashMap<>();
					ftaanalysis.put(ftaAnalysisNode.get("FTA_CODE").asText(), ftaAnalysisNode.get("DEMIN_SAFETY_FACTOR").asText());
					bomConf.setFtaanalysis(ftaanalysis);
				}
			}

			if (eachNode.has("COO_FROM_COM")) bomConf.setCooFromCom(eachNode.get("COO_FROM_COM").asText());
			if (eachNode.has("RVC_CALC_MAX_SALES_PRICE")) bomConf.setRvcMaxSalePrice(eachNode.get("RVC_CALC_MAX_SALES_PRICE").asDouble());
			if (eachNode.has("RVC_LIMIT_SAFETY_FACTOR")) bomConf.setRvcLimitSafFactor(eachNode.get("RVC_LIMIT_SAFETY_FACTOR").asDouble());
			if (eachNode.has("PARTIAL_PERIOD")) bomConf.setPartialPeriod(eachNode.get("PARTIAL_PERIOD").asText());
			if (eachNode.has("RVC_THRESHOLD_SAFETY_FACTOR")) bomConf.setRvcThreshHoldSaffactor(eachNode.get("RVC_THRESHOLD_SAFETY_FACTOR").asDouble());
			if (eachNode.has("ANALYSIS_METHOD")) bomConf.setAnalysisMethod(eachNode.get("ANALYSIS_METHOD").asText());
			if (eachNode.has("INTERMEDIATE_PARTS")) bomConf.setInterMediateParts(eachNode.get("INTERMEDIATE_PARTS").asText());
			if (eachNode.has("AGG_COO_BOM_HEADER")) bomConf.setAggCooFromCom(eachNode.get("AGG_COO_BOM_HEADER").asText());
			if (eachNode.has("AGG_COO_FROM_COM")) bomConf.setAggCooFromCom(eachNode.get("AGG_COO_FROM_COM").asText());
			if (eachNode.has("RVC_METHOD")) bomConf.setRvcmethod(eachNode.get("RVC_METHOD").asText());
			if (eachNode.has("RVC_CALC_MIN_SALES_PRICE")) bomConf.setRvcMinSalePrice(eachNode.get("RVC_CALC_MIN_SALES_PRICE").asText());
			if (eachNode.has("MANDATORY_COO")) bomConf.setCooFromCom(eachNode.get("MANDATORY_COO").asText());
		}
		MessageFormatter.trace(logger, "loadAnalysisConf", "Loading  analysis Config cache for FTA_ANALYSIS COMPLETE");
		return bomConf;
	}

	private List<CompAnalysisConfData> loadConfAnalysis(JsonNode aQEConfigJsonObject)
	{

		MessageFormatter.trace(logger, "loadConfAnalysis", "Loading component analysis Config cache for COMPONENT_ANALYSIS_CONFIG START");
		List<CompAnalysisConfData> compAnalysisList = new ArrayList<>();

		if (aQEConfigJsonObject.has("COMPONENT_ANALYSIS_CONFIG") && aQEConfigJsonObject.get("COMPONENT_ANALYSIS_CONFIG").isArray())
		{

			for (JsonNode eachNode : aQEConfigJsonObject.get("COMPONENT_ANALYSIS_CONFIG"))
			{

				CompAnalysisConfData companalysisconf = new CompAnalysisConfData();
				if (eachNode.has("COMP_HS_MATCH")) companalysisconf.setCompHsMatch(eachNode.get("COMP_HS_MATCH").asText());

				if (eachNode.has("EXCLUDE_FROM_TS")) companalysisconf.setExcludeTS(eachNode.get("EXCLUDE_FROM_TS").asText());

				if (eachNode.has("COMPONENT_TYPE")) companalysisconf.setCompType(eachNode.get("COMPONENT_TYPE").asText());

				if (eachNode.has("FTA_CODE")) companalysisconf.setFtaCode(eachNode.get("FTA_CODE").asText());

				if (eachNode.has("EXCLUDE_FROM_RVC")) companalysisconf.setExcludeRvc(eachNode.get("EXCLUDE_FROM_RVC").asText());

				if (eachNode.has("COMP_HS_LENGTH")) companalysisconf.setCompHsLength(eachNode.get("COMP_HS_LENGTH").asText());

				compAnalysisList.add(companalysisconf);
			}

		}
		MessageFormatter.trace(logger, "loadConfAnalysis", "Loading component analysis Config cache for COMPONENT_ANALYSIS_CONFIG  COMPLETE");
		return compAnalysisList;
	}

	private SubPullConfigContainer loadSubPullConf(JsonNode aQEConfigJsonObject, String aOrgCode) throws Exception
	{

		MessageFormatter.trace(logger, "loadSubPullConf", "Loading subpull Config cache for BASE_HS for the org code: [{0}] START", aOrgCode);
		SubPullConfigContainer container = new SubPullConfigContainer();

		JsonNode subPullCOnfJson = aQEConfigJsonObject.get("BASE_HS");
		if (subPullCOnfJson == null) return container;

		for (JsonNode eachNode : subPullCOnfJson)
		{
			String ftaCode = "";
			if (eachNode.has("FTA_CODE")) ftaCode = eachNode.get("FTA_CODE").asText();

			String ftaCoi = "";
			if (eachNode.has("FTA_COI")) ftaCoi = eachNode.get("FTA_COI").asText();

			if (ftaCoi.isEmpty() && ftaCode.isEmpty()) continue;

			if ("ANY".equals(ftaCoi))
			{

				SubPullConfigData commonSubPullConf = new SubPullConfigData();
				commonSubPullConf.setFtaCoi("ANY");
				commonSubPullConf.setFtaCode(ftaCode);

				setSubPullConfigData(commonSubPullConf, eachNode);

				Set<String> ctryList = getCountryListByFTA(aOrgCode, ftaCode);
			
				if (ctryList == null) continue;

				for (String ctryCode : ctryList)
				{
					TradeLane aTradeLane = new TradeLane(ftaCode, ctryCode);
					SubPullConfigData tldTest = container.getTradeLaneData(aTradeLane);
					if (tldTest == null)
					{
						container.addTradelane(aTradeLane, commonSubPullConf);
					}
				}
			}
			else
			{
				TradeLane aTradeLane = new TradeLane(ftaCode, ftaCoi);
				SubPullConfigData tldTest = container.getTradeLaneData(aTradeLane);
				if (tldTest == null)
				{
					tldTest = new SubPullConfigData();
					tldTest.setFtaCoi(ftaCoi);
					tldTest.setFtaCode(ftaCode);
					container.addTradelane(aTradeLane, tldTest);
					setSubPullConfigData(tldTest, eachNode);
				}
				else
				{
					setSubPullConfigData(tldTest, eachNode);
				}
			}
		}
		MessageFormatter.trace(logger, "loadSubPullConf", "Loading subpull Config cache for BASE_HS for the org code: [{0}] COMPLETE", aOrgCode);
		return container;
	}

	private void setSubPullConfigData(SubPullConfigData subPullConf, JsonNode eachNode)
	{
		List<BaseHSFallConfg> baseHSfallConfList = new ArrayList<>();

		if (eachNode.has("BASE_HS_FALLBACK") && eachNode.get("BASE_HS_FALLBACK").isArray())
		{
			for (JsonNode baseHsFallNode : eachNode.get("BASE_HS_FALLBACK"))
			{
				SubPullConfigData.BaseHSFallConfg baseConf = subPullConf.new BaseHSFallConfg();

				if (baseHsFallNode.has("HEADER_HS_LENGTH")) baseConf.setHeaderHsLength(baseHsFallNode.get("HEADER_HS_LENGTH").asText());

				if (baseHsFallNode.has("COMPONENT_CTRY")) baseConf.setCompCtry(baseHsFallNode.get("COMPONENT_CTRY").asText());

				if (baseHsFallNode.has("HEADER_CTRY")) baseConf.setHeaderCtry(baseHsFallNode.get("HEADER_CTRY").asText());

				if (baseHsFallNode.has("COMP_HS_LENGTH")) baseConf.setCompHslength(baseHsFallNode.get("COMP_HS_LENGTH").asText());
				baseHSfallConfList.add(baseConf);

			}
		}
		if (eachNode.has("HEADER_HS_LENGTH")) subPullConf.setHeaderHsLength(eachNode.get("HEADER_HS_LENGTH").asText());

		if (eachNode.has("COMPONENT_CTRY")) subPullConf.setCompCtry(eachNode.get("COMPONENT_CTRY").asText());

		if (eachNode.has("HEADER_CTRY")) subPullConf.setHeaderCtry(eachNode.get("HEADER_CTRY").asText());

		if (eachNode.has("COMP_HS_LENGTH")) subPullConf.setCompHsLength(eachNode.get("COMP_HS_LENGTH").asText());

		if (eachNode.has("USE_COM_HS")) subPullConf.setManufactureCountry(eachNode.get("USE_COM_HS").asText());

		subPullConf.setBaseHSfallConfList(baseHSfallConfList);

	}

	private TradeLaneContainer loadFTAActivation(JsonNode aQEConfigJsonObject, String aOrgCode) throws Exception
	{

		MessageFormatter.trace(logger, "loadFTAActivation", "Loading fta activation Config cache for the org code [{0}]  START", aOrgCode);

		TradeLaneContainer laneContainer = new TradeLaneContainer();
		JsonNode ftaActivationJson = null;
		if (aQEConfigJsonObject.has("FTA_ACTIVITY")) ftaActivationJson = aQEConfigJsonObject.get("FTA_ACTIVITY");
		if (ftaActivationJson != null)
		{
			for (JsonNode eachNode : ftaActivationJson)
			{
				String ftaCode = "";
				if (eachNode.has("FTA_CODE")) ftaCode = eachNode.get("FTA_CODE").asText();
				else continue;

				String coi = "";
				if (eachNode.has("COI")) coi = eachNode.get("COI").asText();

				if ("ANY".equals(coi))
				{

					TradeLaneData commonTradeLaneData = new TradeLaneData();
					commonTradeLaneData.setCtryOfImport("ANY");
					commonTradeLaneData.setFtaCode(ftaCode);

					setTradeLaneData(commonTradeLaneData, eachNode);

					Set<String> ctryList = getCountryListByFTA(aOrgCode, ftaCode);

					if (ctryList == null) continue;

					for (String ctryCode : ctryList)
					{
						TradeLane aTradeLane = new TradeLane(ftaCode, ctryCode);
						TradeLaneData tldTest = laneContainer.getTradeLaneData(aTradeLane);
						if (tldTest == null)
						{
							laneContainer.addTradelane(aTradeLane, commonTradeLaneData);
						}
					}
				}
				else
				{
					TradeLane aTradeLane = new TradeLane(ftaCode, coi);
					TradeLaneData tldTest = laneContainer.getTradeLaneData(aTradeLane);
					if (tldTest == null)
					{
						tldTest = new TradeLaneData();
						tldTest.setCtryOfImport(coi);
						tldTest.setFtaCode(ftaCode);
						laneContainer.addTradelane(aTradeLane, tldTest);
						setTradeLaneData(tldTest, eachNode);
					}
					else
					{
						setTradeLaneData(tldTest, eachNode);
					}
				}
			}
		}
		MessageFormatter.trace(logger, "loadFTAActivation", "Loading fta activation Config cache for the org code [{0}]  COMPLETE", aOrgCode);
		return laneContainer;
	}
	

	private void setTradeLaneData(TradeLaneData aTradeLane, JsonNode aTradeLaneJson) throws IOException
	{
		// Requalification
		String requalification = "";
		if (aTradeLaneJson.has("REQUALIFICATION")) requalification = aTradeLaneJson.get("REQUALIFICATION").asText("");
		aTradeLane.setRequalification("Y".equals(requalification));

		// Annual Requalification
		String annualQualification = "";
		if (aTradeLaneJson.has("ANNUAL_QUALIFICATION")) annualQualification = aTradeLaneJson.get("ANNUAL_QUALIFICATION").asText("");
		aTradeLane.setAnnulaqualification("Y".equals(annualQualification));

		// Annual Requalification date
		String annualQualificationDate = "";
		if (aTradeLaneJson.has("ANNUAL_REQUALIFICATION_DATE")) annualQualificationDate = aTradeLaneJson.get("ANNUAL_REQUALIFICATION_DATE").asText("");
		aTradeLane.setAnnualQualificationDate(annualQualificationDate);

		// Raw Material Analysis Method
		String rawmaterialanalysismethod = "";
		if (aTradeLaneJson.has("RAWMAT_ANALYSIS_METHOD")) rawmaterialanalysismethod = aTradeLaneJson.get("RAWMAT_ANALYSIS_METHOD").asText("");
		aTradeLane.setRawMaterialAnalysisMethod("Y".equals(rawmaterialanalysismethod));

		// Intermediate Analysis Method
		String intermediatealanalysismethod = "";
		if (aTradeLaneJson.has("INTERMEDIATE_ANALYSIS_METHOD")) intermediatealanalysismethod = aTradeLaneJson.get("INTERMEDIATE_ANALYSIS_METHOD").asText("");
		aTradeLane.setIntermediateAnalysisMethod("Y".equals(intermediatealanalysismethod));

		// Use Previous Year Qualification
		String userPriviousYearqualification = "";
		if (aTradeLaneJson.has("USE_PREV_YEAR_QUAL")) userPriviousYearqualification = aTradeLaneJson.get("USE_PREV_YEAR_QUAL").asText("");
		aTradeLane.setUserPriviousYearQualification("Y".equals(userPriviousYearqualification));

		// Use Previous Year Qualification. Up to
		String usepreviousqualificationupto = "";
		if (aTradeLaneJson.has("USE_PREV_YEAR_QUAL_UPTO")) usepreviousqualificationupto = aTradeLaneJson.get("USE_PREV_YEAR_QUAL_UPTO").asText("");
		aTradeLane.setUsePrevYearQualUpto(usepreviousqualificationupto);

		// Use Non-Originating Material
		String usenonoriginatingmaterial = "";
		if (aTradeLaneJson.has("USE_NON_ORIGINATING_MATERIALS")) usenonoriginatingmaterial = aTradeLaneJson.get("USE_NON_ORIGINATING_MATERIALS").asText("");
		aTradeLane.setUseNonOriginatingMaterials("Y".equals(usenonoriginatingmaterial));

		// Use populate Material attribute
		String populatenotoriginatingcompdtls = "";
		if (aTradeLaneJson.has("POPULATE_NOT_ORIG_COMP_DTLS")) populatenotoriginatingcompdtls = aTradeLaneJson.get("POPULATE_NOT_ORIG_COMP_DTLS").asText("");
		aTradeLane.setPopulateMaterialAttributes("Y".equals(populatenotoriginatingcompdtls));
		// Apply RVCc restriction
		String applyrvcrestriction = "";
		if (aTradeLaneJson.has("APPLY_RVC_RESTRICTION")) applyrvcrestriction = aTradeLaneJson.get("APPLY_RVC_RESTRICTION").asText("");
		aTradeLane.setApplyRvcRestriction("Y".equals(applyrvcrestriction));

		// Apply Roll up components
		String rollupcomponents = "";
		if (aTradeLaneJson.has("ROLL_UP_COMPONENTS")) rollupcomponents = aTradeLaneJson.get("ROLL_UP_COMPONENTS").asText("");
		aTradeLane.setRollupComponents("Y".equals(rollupcomponents));

	}



	private CumulationConfigContainer loadCumulationConfig(JsonNode aQEConfigJsonObject) throws Exception
	{

		MessageFormatter.trace(logger, "loadCumulationConfig", "Started cumulation rules  Config cache for CUMULATION_RULES");
		CumulationConfigContainer cumulationConfigContainer = new CumulationConfigContainer();
		JsonNode cumulationConfigJson = null;
		if (aQEConfigJsonObject.has("CUMULATION_RULES"))
		{
			cumulationConfigJson = aQEConfigJsonObject.get("CUMULATION_RULES");
			cumulationConfigContainer.setCumulationConfiguration(cumulationConfigJson);
		}

		JsonNode componentAgreementJson = null;
		if (aQEConfigJsonObject.has("COMPONENT_AGREEMENT"))
		{
			componentAgreementJson = aQEConfigJsonObject.get("COMPONENT_AGREEMENT");
			cumulationConfigContainer.setComponentAgreementConfig(componentAgreementJson);
		}

		return cumulationConfigContainer;
	}

	

	
	class QEConfigCacheRowCallbackHandler implements RowCallbackHandler
	{
		/**
		 ************************************************************************************* <P>
		 * </P>
		 * 
		 * @param theResultSet
		 */
		@Override
		public void processRow(ResultSet theResultSet) throws SQLException
		{

			String OrgCode = theResultSet.getString("NODE_CODE");

			try
			{
				String theConfig = theResultSet.getString("CONFIG");

				if (null == theConfig) return;

				QEConfig qeConfig = new QEConfig();

				String aQEConfigJsonObject = theConfig.replace("DefaultSchema:QE_CONFIG:", "");
				ObjectMapper objectMapper = new ObjectMapper();
				JsonNode rootNode = objectMapper.readTree(aQEConfigJsonObject);

				TradeLaneContainer laneContainer = loadFTAActivation(rootNode, OrgCode);
				laneContainer.setOrgCode(OrgCode);
				qeConfig.setTradeLaneContainer(laneContainer);

				BomAnalysisConfigData bomAnalysisConf = loadAnalysisConf(rootNode);
				qeConfig.setBomAnalysisConfigData(bomAnalysisConf);

				SubPullConfigContainer subPullConf = loadSubPullConf(rootNode, OrgCode);
				qeConfig.setSubPullConfigContainer(subPullConf);

				List<CompAnalysisConfData> loadConfAnalysisList = loadConfAnalysis(rootNode);
				qeConfig.setCompAnalysisConfDataList(loadConfAnalysisList);

				CumulationConfigContainer cumulationConfigContainer = loadCumulationConfig(rootNode);
				qeConfig.setCumulationConfigContainer(cumulationConfigContainer);
				qeConfigMap.put(OrgCode, qeConfig);

			}
			catch (Exception exec)
			{
				MessageFormatter.error(logger, "fullCacheRowCallbackHandler", exec, "Error while loading the full cache for QE_CONFIG and org code [{0}] ", OrgCode);
			}
		}
	}
}
