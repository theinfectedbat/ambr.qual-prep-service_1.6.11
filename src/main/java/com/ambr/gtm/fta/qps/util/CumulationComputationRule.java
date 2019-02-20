package com.ambr.gtm.fta.qps.util;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.list.FTAListContainer;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetails;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsCache;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsSourceIVAContainer;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVA;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAProductSourceContainer;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponent;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponentDataExtension;
import com.ambr.gtm.fta.qts.TrackerCodes;
import com.ambr.gtm.fta.qts.config.FTAHSListCache;
import com.ambr.gtm.fta.qts.config.QEConfigCache;
import com.ambr.gtm.fta.qts.util.CumulationConfigContainer.CumulationRule;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfiguration;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfigurationRepository;
import com.ambr.gtm.utils.legacy.rdbms.de.GroupNameSpecification;
import com.ambr.gtm.utils.legacy.sps.SimplePropertySheet;
import com.ambr.gtm.utils.legacy.sps.SimplePropertySheetManager;
import com.ambr.gtm.utils.legacy.sps.exception.PropertyDoesNotExistException;
import com.ambr.platform.utils.log.MessageFormatter;

public class CumulationComputationRule 
{

	private CurrencyExchangeRateManager beanCurrencyExchangeRate;
	private SimplePropertySheetManager propertySheetManager;
	DataExtensionConfigurationRepository dataRepos;
	public FTAListContainer				ftaListContainerCache	= new FTAListContainer();
	private static final Logger logger = LogManager.getLogger(CumulationComputationRule.class);
	public CurrencyExchangeRateManager getBeanCurrencyExchangeRate()
	{
		return beanCurrencyExchangeRate;
	}

	private QEConfigCache qeConfigCache;
	private  FTAHSListCache ftaHSListCache;
	
	public CumulationComputationRule(CurrencyExchangeRateManager beanCurrencyExchangeRate, QEConfigCache qeConfigCache, SimplePropertySheetManager propertySheetManager, DataExtensionConfigurationRepository dataRepos,FTAHSListCache ftaHSListCache) {
		this.qeConfigCache = qeConfigCache;
		this.beanCurrencyExchangeRate = beanCurrencyExchangeRate;
		this.propertySheetManager = propertySheetManager;
		this.dataRepos = dataRepos;
		this.ftaHSListCache=ftaHSListCache;
	}
	
			
	public void applyCumulationForComponent(QualTXComponent aQualTXComp, 
			GPMSourceIVAProductSourceContainer prodSourceContainer, GPMClaimDetailsCache claimDetailsCache, DataExtensionConfigurationRepository  dataExtRepos) 
			throws Exception
	{


		CumulationRule cumulationrule = getCumulationRdcByCOIAndCOMConfig(aQualTXComp.org_code,aQualTXComp.qualTX.fta_code,aQualTXComp.qualTX.ctry_of_import,aQualTXComp.qualTX.ctry_of_manufacture);
		if(cumulationrule == null)
		{
			MessageFormatter.debug(logger, "applyCumulationForComponent", "There is no Cumulation Configuration for Org Code [{0}] for FTA [{1}], COI[{2}], COM[{3}]", 
					aQualTXComp.org_code, aQualTXComp.qualTX.fta_code, aQualTXComp.qualTX.ctry_of_import, aQualTXComp.qualTX.ctry_of_manufacture);
			return;
		}
		try {
			long cumulative_fta_iva_key = 0;
			boolean contextualFTAHasYDecision = "QUALIFIED".equalsIgnoreCase(aQualTXComp.qualified_flg);
			if(!contextualFTAHasYDecision)
			{
				List<String> cumulationFTAs = cumulationrule.getDestinationFTAList();

				boolean hsInCumulationExcptList = false;
				if (cumulationFTAs != null && !cumulationFTAs.isEmpty())
				{
					List<String> hsExcpLists = cumulationrule.getHsExceptionList();
					// TA-70092 - PERF : GPMToBOMPropagationRule in GetGPMCC and
					// Cumulation Rules logic.
					if (aQualTXComp.hs_num != null && hsExcpLists != null && !hsExcpLists.isEmpty())
					{
						String aCompanyCode = aQualTXComp.org_code;
						for (String hsExcpList : hsExcpLists)
						{
							//TODO: End Point Service Required to do HS Exist in Exception list

							hsInCumulationExcptList = existsInFTAExceptionList(aCompanyCode, aQualTXComp.qualTX.ctry_of_import, aQualTXComp.hs_num, aQualTXComp.qualTX.fta_code, new Date(), hsExcpList);
							if (hsInCumulationExcptList) break;
						}
					}
				}

				if (!hsInCumulationExcptList)
				{
					List<GPMSourceIVA> alterNateIvaList = new ArrayList<>();
					for (String cumulationFtaCode : cumulationFTAs)
					{
						GPMSourceIVA alterNateIva = QualTXUtility.getGPMIVARecord(prodSourceContainer,cumulationFtaCode, null,aQualTXComp.qualTX.effective_from, aQualTXComp.qualTX.effective_to); 
						if(alterNateIva != null)
							alterNateIvaList.add(alterNateIva);
					}
					if(!alterNateIvaList.isEmpty())
					{
						for(GPMSourceIVA alterNateIVA : alterNateIvaList)
						{
							String aCumulatedRDCFTACode = alterNateIVA.ftaCode;
							String aCumulatedRDCFTACOI = alterNateIVA.ctryOfImport;

							if(aQualTXComp.qualTX.fta_code.equals(aCumulatedRDCFTACode) && aQualTXComp.qualTX.ctry_of_import.equals(aCumulatedRDCFTACOI))
								continue;

							Set <String> ftaCtrys = this.qeConfigCache.getCountryListByFTA(aQualTXComp.org_code, aCumulatedRDCFTACode.trim());
							if(ftaCtrys.stream().anyMatch(s-> s.equals(aCumulatedRDCFTACOI)))
							{
								cumulative_fta_iva_key = alterNateIVA.ivaKey; //Need to remove this 
								aQualTXComp.cumulation_rule_fta_used = aCumulatedRDCFTACode;
								aQualTXComp.cumulation_rule_applied = "Y";
								aQualTXComp.qualified_flg = "QUALIFIED";
								break;
							}
						}
					}
				}
			}
			// TA-75175 - BOM: Enhance Cumulation Configurations
			// REN band-aid fix
			boolean markAsNonOriginating = false;
			List<String> cumulationCOOList = cumulationrule.getCountryOfOriginList();
			boolean ivaCumulationApplied = false;

			if (cumulationCOOList != null && !cumulationCOOList.isEmpty())
			{
				if (contextualFTAHasYDecision  || (null != aQualTXComp.cumulation_rule_fta_used))
				{
					String aCOO = aQualTXComp.ctry_of_origin;
					//21-Jun-2018 - Check if COO is part of cumulation ctry list
					if (aCOO == null || aCOO.isEmpty() || !cumulationCOOList.contains(aCOO))
					{
						markAsNonOriginating = true;
					}

					if (!markAsNonOriginating)
					{
						Long theIvakey =  null;
						if( cumulative_fta_iva_key > 0)
							theIvakey = cumulative_fta_iva_key; 
						else if(aQualTXComp.prod_src_iva_key != null && aQualTXComp.prod_src_iva_key > 0)
							theIvakey =  aQualTXComp.prod_src_iva_key;

						GPMClaimDetailsSourceIVAContainer claimdetailsContainer = claimDetailsCache.getClaimDetails(theIvakey);
						if (claimdetailsContainer == null) return;
						GPMClaimDetails aClaimDetails = claimdetailsContainer.getPrimaryClaimDetails();
						if (aClaimDetails == null) return;

						String ftaCodeGroup = QualTXUtility.determineFTAGroupCode(aQualTXComp.org_code, aQualTXComp.cumulation_rule_fta_used != null ? aQualTXComp.cumulation_rule_fta_used : aQualTXComp.qualTX.fta_code, propertySheetManager);
						Map<String,String> flexFieldMap = getFeildMapping("STP", ftaCodeGroup);

						String cumulationapplied =(String) aClaimDetails.getValue(flexFieldMap.get("CUMULATION_APPLIED"));
						ivaCumulationApplied = "Y".equals(cumulationapplied);

						if(ivaCumulationApplied)
						{
							String suppCtryList = (String) aClaimDetails.getValue(flexFieldMap.get("CUMULATION_CTRY_LIST"));
							if (suppCtryList != null && !suppCtryList.isEmpty())
							{
								for (String suppCtry : suppCtryList.split(";"))
								{
									if (!cumulationCOOList.contains(suppCtry))
									{
										markAsNonOriginating = true;
										break;
									}
								}
							}
						}
						String decision = (String) aClaimDetails.getValue(flexFieldMap.get("DECISION"));

						// Supplier cumulation decision gets the precedence.
						if (!"Y".equals(decision))
						{
							aQualTXComp.cumulation_rule_applied = "";
							aQualTXComp.qualified_flg =  "NOT_QUALIFIED";
						}
						// Supplier cumulation applied flag should get the
						// precedence.
						if (contextualFTAHasYDecision && "Y".equals(cumulationapplied))
						{
							aQualTXComp.cumulation_rule_applied =  "Y";
						}
					}
				}
				if (!markAsNonOriginating && !ivaCumulationApplied)
				{
					String compCOO = aQualTXComp.ctry_of_origin;
					if (compCOO != null && !cumulationCOOList.contains(compCOO))
					{
						markAsNonOriginating = true;
					}
					else if (compCOO == null) markAsNonOriginating = true;

				}
				if (markAsNonOriginating)
				{
					aQualTXComp.cumulation_rule_applied = "N";
					aQualTXComp.qualified_flg = "NOT_QUALIFIED";
				}
			}

			QualTXComponentDataExtension qualTXCompDetails = null;
			if(aQualTXComp.deList != null && !aQualTXComp.deList.isEmpty())
			{
				for(QualTXComponentDataExtension qualTXCompDe: aQualTXComp.deList)
				{
					if(qualTXCompDe.group_name.contains("QUALTX:COMP_DTLS"))
					{
						qualTXCompDetails = qualTXCompDe;
						break;
					}
				}
			}
			if(qualTXCompDetails == null)
				qualTXCompDetails = aQualTXComp.createDataExtension("QUALTX:COMP_DTLS", dataExtRepos, null);

			Map<String,String> qualtxCOmpDtlflexFieldMap = getFeildMapping("QUALTX","COMP_DTLS");
			// TA 64389 - Addition of COO to cumulation configuration
			if (!"Y".equalsIgnoreCase(aQualTXComp.cumulation_rule_applied) && !"QUALIFIED".equalsIgnoreCase(aQualTXComp.qualified_flg))
			{
				if (null != cumulationrule)
				{
					if (cumulationCOOList != null && cumulationrule.useCOOList())
					{
						String cooByHierarchy = aQualTXComp.ctry_of_origin;
						/*
						 * if((!aIVA.isEmpty("CTRY_OF_ORIGIN") &&
						 * cumulationCOOList.contains(aIVA.getStringValue(
						 * "CTRY_OF_ORIGIN"))) || (coo != null &&
						 * cumulationCOOList.contains(coo)) ||
						 * (!this.getRDC().isEmpty("CTRY_OF_ORIGIN") &&
						 * cumulationCOOList.contains(this.getRDC().
						 * getStringValue( "CTRY_OF_ORIGIN")))){
						 */
						if (cooByHierarchy != null && cumulationCOOList.contains(cooByHierarchy))
						{
							aQualTXComp.qualified_flg = "QUALIFIED";
							aQualTXComp.cumulation_rule_applied = "Y";
						}
					}
					else {
						aQualTXComp.cumulation_rule_applied = "N";
						aQualTXComp.qualified_flg = "NOT_QUALIFIED";
					}
				}
			}
			if("Y".equals(aQualTXComp.cumulation_rule_applied))
			{
				Long theIvakey =  null;
				if( cumulative_fta_iva_key > 0)
					theIvakey = cumulative_fta_iva_key; 
				else if(aQualTXComp.prod_src_iva_key != null && aQualTXComp.prod_src_iva_key > 0)
					theIvakey =  aQualTXComp.prod_src_iva_key;

				String flexFieldName = qualtxCOmpDtlflexFieldMap.get("CUMULATION_CTRY_LIST");
				GPMClaimDetailsSourceIVAContainer claimdetailsContainer = claimDetailsCache.getClaimDetails(theIvakey);
				if (claimdetailsContainer == null)
				{
					if(flexFieldName != null)
						qualTXCompDetails.setValue(flexFieldName, aQualTXComp.ctry_of_origin);
					flexFieldName = qualtxCOmpDtlflexFieldMap.get("CUMULATION_RULE_APPLIED");
					if(flexFieldName != null)
						qualTXCompDetails.setValue(flexFieldName, aQualTXComp.cumulation_rule_applied);
					return;
				}
				GPMClaimDetails aClaimDetails = claimdetailsContainer.getPrimaryClaimDetails();
				if (aClaimDetails == null) 
				{
					if(flexFieldName != null)
						qualTXCompDetails.setValue(flexFieldName, aQualTXComp.ctry_of_origin);
					flexFieldName = qualtxCOmpDtlflexFieldMap.get("CUMULATION_RULE_APPLIED");
					if(flexFieldName != null)
						qualTXCompDetails.setValue(flexFieldName, aQualTXComp.cumulation_rule_applied);
					return;
				}

				String ftaCodeGroup = QualTXUtility.determineFTAGroupCode(aQualTXComp.org_code, aQualTXComp.cumulation_rule_fta_used != null ? aQualTXComp.cumulation_rule_fta_used : aQualTXComp.qualTX.fta_code, propertySheetManager);
				Map<String,String> flexFieldMap = getFeildMapping("STP", ftaCodeGroup);

				String cumulationCtryList = "";
				flexFieldName = flexFieldMap.get("CUMULATION_CTRY_LIST");
				if(flexFieldName != null && !"".equals(flexFieldName))
					cumulationCtryList = (String) aClaimDetails.claimDetailsValue.get(flexFieldName.toLowerCase());

				Object cumulationValueObj =  null;
				flexFieldName = flexFieldMap.get("CUMULATION_VALUE");
				if(flexFieldName != null && !"".equals(flexFieldName))
					cumulationValueObj = aClaimDetails.claimDetailsValue.get(flexFieldName.toLowerCase());

				if(null != cumulationValueObj) aQualTXComp.cumulation_value = ((Number)cumulationValueObj).doubleValue();

				Object cumulationCurrencyObj =  null;
				String cumulationCurrencycode = flexFieldMap.get("CUMULATION_CURRENCY");
				if(cumulationCurrencycode != null && !"".equals(cumulationCurrencycode))
					cumulationCurrencyObj = aClaimDetails.claimDetailsValue.get(cumulationCurrencycode.toLowerCase());

				if(null != cumulationCurrencyObj) aQualTXComp.cumulation_currency = (String)cumulationCurrencyObj;

				if(cumulationrule.useCOOList() && cumulationCtryList == null)
				{
					cumulationCtryList = aQualTXComp.ctry_of_origin;
				}
				else if((cumulationrule.useCOOList() || cumulationCOOList.contains(aQualTXComp.ctry_of_origin)) && cumulative_fta_iva_key > 0)
				{
					if(aQualTXComp.ctry_of_origin != null && !aQualTXComp.ctry_of_origin.isEmpty())
						cumulationCtryList = aQualTXComp.ctry_of_origin;
				}

				if(cumulationCtryList != null && !cumulationCtryList.isEmpty())
					qualTXCompDetails.setValue(qualtxCOmpDtlflexFieldMap.get("CUMULATION_CTRY_LIST"), cumulationCtryList);

				qualTXCompDetails.setValue(qualtxCOmpDtlflexFieldMap.get("CUMULATION_RULE_APPLIED"), aQualTXComp.cumulation_rule_applied);

				Double cumulationValue = calculateCumulationValue	(aQualTXComp, aClaimDetails);

				if(cumulationValue != null){
					if(aQualTXComp.qualTX.analysis_method == null 
							|| "".equals(aQualTXComp.qualTX.analysis_method))
					{
						String analysisMethod =  this.qeConfigCache.getQEConfig(aQualTXComp.org_code).getAnalysisConfig().getAnalysisMethod();
						if(TrackerCodes.AnalysisMethod.TOP_DOWN_ANALYSIS.name().equals(analysisMethod))
							aQualTXComp.td_cumulation_value = cumulationValue;
						else if(TrackerCodes.AnalysisMethod.RAW_MATERIAL_ANALYSIS.name().equals(analysisMethod))
							aQualTXComp.rm_cumulation_value = cumulationValue;
						else if(TrackerCodes.AnalysisMethod.INTERMEDIATE_ANALYSIS.name().equals(analysisMethod))
							aQualTXComp.in_cumulation_value = cumulationValue;
					}
					else if(TrackerCodes.AnalysisMethod.TOP_DOWN_ANALYSIS.name().equals(aQualTXComp.qualTX.analysis_method))
						aQualTXComp.td_cumulation_value = cumulationValue;
					else if(TrackerCodes.AnalysisMethod.RAW_MATERIAL_ANALYSIS.name().equals(aQualTXComp.qualTX.analysis_method))
						aQualTXComp.rm_cumulation_value = cumulationValue;
					else if(TrackerCodes.AnalysisMethod.INTERMEDIATE_ANALYSIS.name().equals(aQualTXComp.qualTX.analysis_method))
						aQualTXComp.in_cumulation_value = cumulationValue;
				}
			}
			else if(aQualTXComp.cumulation_value != null && aQualTXComp.cumulation_value.doubleValue() > 0){
				if(aQualTXComp.qualTX.analysis_method == null 
						|| "".equals(aQualTXComp.qualTX.analysis_method))
				{
					String analysisMethod =  this.qeConfigCache.getQEConfig(aQualTXComp.org_code).getAnalysisConfig().getAnalysisMethod();
					if(TrackerCodes.AnalysisMethod.TOP_DOWN_ANALYSIS.name().equals(analysisMethod))
						aQualTXComp.td_cumulation_value = aQualTXComp.cumulation_value;
					else if(TrackerCodes.AnalysisMethod.RAW_MATERIAL_ANALYSIS.name().equals(analysisMethod))
						aQualTXComp.rm_cumulation_value = aQualTXComp.cumulation_value;
					else if(TrackerCodes.AnalysisMethod.INTERMEDIATE_ANALYSIS.name().equals(analysisMethod))
						aQualTXComp.in_cumulation_value = aQualTXComp.cumulation_value;
				}
				else if(TrackerCodes.AnalysisMethod.TOP_DOWN_ANALYSIS.name().equals(aQualTXComp.qualTX.analysis_method))
					aQualTXComp.td_cumulation_value = aQualTXComp.cumulation_value;
				else if(TrackerCodes.AnalysisMethod.RAW_MATERIAL_ANALYSIS.name().equals(aQualTXComp.qualTX.analysis_method))
					aQualTXComp.rm_cumulation_value = aQualTXComp.cumulation_value;
				else if(TrackerCodes.AnalysisMethod.INTERMEDIATE_ANALYSIS.name().equals(aQualTXComp.qualTX.analysis_method))
					aQualTXComp.in_cumulation_value = aQualTXComp.cumulation_value;
			}
		}
		catch(Exception exec) {
			MessageFormatter.error(logger, "applyCumulationForComponent", exec, "There is an exception while processing cumulation for Qualtx Compoent [{0,number,#}] Org Code [{0}] for FTA [{1}], COI[{2}], COM[{3}]", 
					aQualTXComp.alt_key_qualtx, aQualTXComp.org_code, aQualTXComp.qualTX.fta_code, aQualTXComp.qualTX.ctry_of_import, aQualTXComp.qualTX.ctry_of_manufacture);
			throw exec;
		}
	}	
	
	public Double	calculateCumulationValue	(QualTXComponent aQualTXComp, GPMClaimDetails claimdetails)
	throws Exception{
		Double convertedCumulationValue = 0.0;
		String headerCurrencyCode = aQualTXComp.qualTX.currency_code; 
		
		Double value = aQualTXComp.cumulation_value;
		String cumulationCurrency = aQualTXComp.cumulation_currency;
		
		if(!"Y".equalsIgnoreCase(aQualTXComp.cumulation_rule_applied) 
				&& null == value 
				&&  null == cumulationCurrency
				&& (headerCurrencyCode == null || headerCurrencyCode.isEmpty()))
			
				return convertedCumulationValue;
		if(value == null || "".equals(value))
			return convertedCumulationValue;
		
		double claimCumulationValue = Double.valueOf(value.toString());
		double aQtyPer = 0.0;
		
		if(aQualTXComp.qualTX.analysis_method == null 
				|| "".equals(aQualTXComp.qualTX.analysis_method))
		{
			String analysisMethod =  this.qeConfigCache.getQEConfig(aQualTXComp.org_code).getAnalysisConfig().getAnalysisMethod();
			if(TrackerCodes.AnalysisMethod.TOP_DOWN_ANALYSIS.name().equals(analysisMethod))
				aQtyPer = aQualTXComp.qty_per;
			else if(TrackerCodes.AnalysisMethod.RAW_MATERIAL_ANALYSIS.name().equals(analysisMethod))
				aQtyPer = aQualTXComp.rm_qty_per;
			else if(TrackerCodes.AnalysisMethod.INTERMEDIATE_ANALYSIS.name().equals(analysisMethod))
				aQtyPer = aQualTXComp.in_qty_per;
		}
		else if(TrackerCodes.AnalysisMethod.TOP_DOWN_ANALYSIS.name().equals(aQualTXComp.qualTX.analysis_method))
			aQtyPer = aQualTXComp.qty_per;
		else if(TrackerCodes.AnalysisMethod.RAW_MATERIAL_ANALYSIS.name().equals(aQualTXComp.qualTX.analysis_method))
			aQtyPer = aQualTXComp.rm_qty_per;
		else if(TrackerCodes.AnalysisMethod.INTERMEDIATE_ANALYSIS.name().equals(aQualTXComp.qualTX.analysis_method))
			aQtyPer = aQualTXComp.in_qty_per;
		
		double cumulationValue = claimCumulationValue * aQtyPer;
		 
		 double exchangeRate  = beanCurrencyExchangeRate.getExchangeRate(cumulationCurrency,headerCurrencyCode); 
		 
		 return cumulationValue * exchangeRate;
	}
	
	public CumulationRule getCumulationRdcByCOIAndCOMConfig(String orgcode, String theFTACode, String ctryOfImport, String theCtryOfManufacture) throws Exception
	{
		CumulationRule cumulationConfigRdc = null;
		List<CumulationRule> cumulationRuleRDCs = this.qeConfigCache.getQEConfig(orgcode).getCumulationConfig().getCumulationRuleConfigByFTA(theFTACode);

		if (null == cumulationRuleRDCs) return cumulationConfigRdc;

		for (CumulationRule aCumulationRule : cumulationRuleRDCs)
		{
			List<String> coiList = aCumulationRule.getCountryOfImportList();
			List<String> comList = aCumulationRule.getCountryOfManufactureList();
			if ((null == coiList || coiList.isEmpty()) && (null == comList || comList.isEmpty())) cumulationConfigRdc = aCumulationRule;
			if (null == coiList || coiList.isEmpty() || null == comList || comList.isEmpty()) break;
			if (coiList.contains(ctryOfImport) && comList.contains(theCtryOfManufacture))
			{
				cumulationConfigRdc = aCumulationRule;
				return cumulationConfigRdc;
			}
		}
		if(cumulationConfigRdc != null)
			return cumulationConfigRdc;

		SimplePropertySheet aPropertySheet = this.propertySheetManager.getPropertySheet(orgcode, "BOM_SCREENING_CONFIG");
		
		
		String theCumulationConfig;
		
		try
		{
			theCumulationConfig = aPropertySheet.getStringValue("CUMULATION_PREFERENCE");
		}
		catch (PropertyDoesNotExistException p)
		{
			theCumulationConfig = null;
		}

		if(null == theCumulationConfig) theCumulationConfig = "COI:COM";
		
		String[] theConfigValues = theCumulationConfig.split(":");
		for (String theConfigVal : theConfigValues)
		{
				cumulationConfigRdc = getCumulationConfigByCountry(theConfigVal,cumulationRuleRDCs,ctryOfImport);
				if(cumulationConfigRdc != null)
					break;
		}
		return cumulationConfigRdc;
	}
	private CumulationRule getCumulationConfigByCountry(String theConfigVal,List<CumulationRule> cumulationRuleRDCs,String ctrycode) 
	{
		for (CumulationRule aCumulationRule : cumulationRuleRDCs)
		{
			List<String> ctryLst = null;
			if ("COI".equalsIgnoreCase(theConfigVal))
				ctryLst = aCumulationRule.getCountryOfImportList();
			else if ("COM".equalsIgnoreCase(theConfigVal))
				ctryLst = aCumulationRule.getCountryOfManufactureList();
			
			if (null != ctryLst && !ctryLst.isEmpty() && ctryLst.contains(ctrycode))
			{
					return aCumulationRule;
			}
		}
		return null;
	}
	
	public boolean existsInFTAExceptionList(String aCompanyCode, String ctryOfImport, String hs_num2, String ftaCode, Date date, String hsExcpList) throws Exception
	{
			FTAListContainer ftaListContainerCache = ftaHSListCache.getFTAList(hsExcpList, aCompanyCode);

			return ftaListContainerCache.doesHSExistsInList(hs_num2, date, hsExcpList, aCompanyCode, ctryOfImport);
	}
	public Map<String, String> getFeildMapping(String deName, String ftaCodeGroup) throws Exception {
		String groupName = MessageFormat.format("{0}{1}{2}", deName, GroupNameSpecification.SEPARATOR, ftaCodeGroup);
		DataExtensionConfiguration	aCfg = this.dataRepos.getDataExtensionConfiguration(groupName);
		return aCfg.getFlexColumnMapping();
	}
}