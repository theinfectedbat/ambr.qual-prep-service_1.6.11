package com.ambr.gtm.fta.qps.qualtx.engine;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang.time.DateUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.gtm.fta.qps.bom.BOMComponent;
import com.ambr.gtm.fta.qps.bom.BOMDataExtension;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassification;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVA;
import com.ambr.gtm.fta.qps.util.CumulationComputationRule;
import com.ambr.gtm.fta.qps.util.CurrencyExchangeRateManager;
import com.ambr.gtm.fta.qps.util.DetermineComponentCOO;
import com.ambr.gtm.fta.qts.config.FTAHSListCache;
import com.ambr.gtm.fta.qts.config.QEConfigCache;
import com.ambr.gtm.fta.qts.util.SubPullConfigContainer;
import com.ambr.gtm.fta.qts.util.SubPullConfigData;
import com.ambr.gtm.fta.qts.util.SubPullConfigData.BaseHSFallConfg;
import com.ambr.gtm.fta.qts.util.TradeLane;
import com.ambr.gtm.fta.qts.util.TradeLaneContainer;
import com.ambr.gtm.fta.qts.util.TradeLaneData;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfiguration;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfigurationRepository;
import com.ambr.gtm.utils.legacy.sps.SimplePropertySheet;
import com.ambr.gtm.utils.legacy.sps.SimplePropertySheetManager;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.gtm.fta.qps.util.PreviousYearQualificationRule;

public class QualTXBusinessLogicProcessor
{
	static Logger	logger = LogManager.getLogger(QualTXBusinessLogicProcessor.class);
	public QEConfigCache qeConfigCache;
	public FTAHSListCache ftaHSListCache;
	private  final String	PROD_CTRY_CMP_KEY	= "prod_ctry_cmpl_key";
	private  final String	HS_NUM				= "hs_num";
	private  final String	SUB_PULL_CTRY				= "sub_pull_ctry";
	private  final String	IVA_EFFECTIVE_FROM	= "IVA_EFFECTIVE_FROM";
	private  final String	CURRENT_DATE		= "CURRENT_DATE";
	

	public DetermineComponentCOO determineComponentCOO;
	public CurrencyExchangeRateManager currencyExchangeRateManager;
	public CumulationComputationRule cumulationComputationRule;
	public SimplePropertySheetManager propertySheetManager;
	public PreviousYearQualificationRule	previousYearQualificationRule;
	public DataExtensionConfigurationRepository dataExtensionConfigRepos;
	

	public QualTXBusinessLogicProcessor() {
		
	}
	
	public QualTXBusinessLogicProcessor(QEConfigCache qeConfigCache,FTAHSListCache ftaHSListCache)
	{
		this.qeConfigCache = qeConfigCache;
		this.ftaHSListCache=ftaHSListCache;
	}
	
	public void setDetermineComponentCOO(DetermineComponentCOO determineComponentCOO)
	{
		this.determineComponentCOO = determineComponentCOO;
	}

	public void setCurrencyExchangeRateManager(CurrencyExchangeRateManager currencyExchangeRateManager)
	{
		this.currencyExchangeRateManager = currencyExchangeRateManager;
	}

	public void setCumulationComputationRule(CumulationComputationRule cumulationComputationRule)
	{
		this.cumulationComputationRule = cumulationComputationRule;
	}
	
	public SimplePropertySheetManager getPropertySheetManager()
	{
		return propertySheetManager;
	}

	public void setPropertySheetManager(SimplePropertySheetManager propertySheetManager)
	{
		this.propertySheetManager = propertySheetManager;
	}
	
	public PreviousYearQualificationRule getPreviousYearQualificationRule()
	{
		return previousYearQualificationRule;
	}

	public void setPreviousYearQualificationRule(PreviousYearQualificationRule previousYearQualificationRule)
	{
		this.previousYearQualificationRule = previousYearQualificationRule;
	}

	public DataExtensionConfigurationRepository getDataExtensionConfigRepos() {
		return dataExtensionConfigRepos;
	}

	public void setDataExtensionConfigRepos(DataExtensionConfigurationRepository dataExtensionConfigRepos) {
		this.dataExtensionConfigRepos = dataExtensionConfigRepos;
	}

	
	public boolean checkEligibilityForAnnualQualificationTrigger(String orgCode, String ftaCode, String coi) throws Exception
	{
		String anualQualificationDate = getAnnualQualifictionDate(orgCode, ftaCode, coi);
		//Check with PdM, If no annual qualification has set then by default it will process it
		if (anualQualificationDate != null)
		{
			return isSystemDateInRangeAnnualQualificationDate(anualQualificationDate, false);
		}
		
		 return true;
	}

	public String getAnnualQualifictionDate(String orgCode, String ftaCode, String coi) throws Exception
	{

		TradeLane aTradeLane = getTradeLane(orgCode, ftaCode, coi);
		if (aTradeLane != null)
		{
			TradeLaneContainer laneContainer = this.qeConfigCache.getQEConfig(orgCode).getTradeLaneContainer();
			if (laneContainer != null)
			{
				TradeLaneData laneData = laneContainer.getTradeLaneData(aTradeLane);
				String annualQualificationDate = laneData.getAnnualQualificationDate();
				if (laneData.isAnnulaqualification() && annualQualificationDate != null) { return annualQualificationDate; }
			}
		}
		return null;
	}

	public Date getFutureQualificationFromDate() throws Exception
	{
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MMM-yyyy");
		String theFutureDate = "01-Jan-" + getFutureYear();
		return (Date) formatter.parseObject(theFutureDate);
	}

	public Date getFutureQualificationToDate() throws Exception
	{
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MMM-yyyy");
		String theFutureDate = "31-Dec-" + getFutureYear();
		return (Date) formatter.parseObject(theFutureDate);
	}

	private Integer getFutureYear()
	{
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		return Integer.valueOf(calendar.get(Calendar.YEAR) + 1);
	}

	public boolean isSystemDateInRangeAnnualQualificationDate(String theAnnualQualificationDate, boolean theDateEqualMatch) throws Exception
	{
		Date sysDate = new Date();
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MMM-yyyy");
		if (theAnnualQualificationDate != null && !theAnnualQualificationDate.equalsIgnoreCase(""))
		{
			String d1 = theAnnualQualificationDate + "-" + Integer.valueOf(cal.get(Calendar.YEAR));
			Date annualQualificationTriggerDate = (Date) formatter.parseObject(d1);

			if (theDateEqualMatch)
			{
				if (DateUtils.isSameDay(sysDate, annualQualificationTriggerDate)) { return true; }
			}
			else
			{
				if (sysDate.after(annualQualificationTriggerDate) || DateUtils.isSameDay(sysDate, annualQualificationTriggerDate)) { return true; }
			}
		}

		return false;
	}

	public TradeLane getTradeLane(String orgCode, String ftaCode, String coi) throws Exception
	{
		List<TradeLane> tradeLaneList = this.qeConfigCache.getQEConfig(orgCode).getTradeLaneList();
		if (tradeLaneList != null)
		{
			Optional<TradeLane> optTradeLane = tradeLaneList.stream().filter(p -> p.getFtaCode().equalsIgnoreCase(ftaCode) && p.getCtryOfImport().equalsIgnoreCase(coi)).findFirst();
			if (optTradeLane.isPresent()) return optTradeLane.get();
		}
		return null;
	}

	public boolean isTradeLaneEnabled(String orgCode, String ftaCode, String coi) throws Exception
	{
		List<TradeLane> tradeLaneList = this.qeConfigCache.getQEConfig(orgCode).getTradeLaneList();
		if (tradeLaneList != null) { return tradeLaneList.stream().anyMatch(p -> p.getFtaCode().equalsIgnoreCase(ftaCode) && p.getCtryOfImport().equalsIgnoreCase(coi)); }
		return false;
	}
	
	public  boolean isQualifiedDateRange(Date theEffectiveFromDate, Date theEffectiveToDate, Date theQualBeginDate, Date theQualEndDate) throws Exception
	{
		boolean aOutOfRangeFlag;

		if (theEffectiveFromDate == null || theEffectiveToDate == null || theQualBeginDate == null || theQualEndDate == null) return false;
		
		
		if (DateUtils.isSameDay(theEffectiveFromDate, theQualBeginDate) && DateUtils.isSameDay(theEffectiveToDate, theQualEndDate)) return true;
		
        // IVA dates may fall into BOM effective date
		aOutOfRangeFlag = (theQualBeginDate.before(theEffectiveFromDate) || DateUtils.isSameDay(theEffectiveFromDate, theQualBeginDate))  
				&& (theQualEndDate.after(theEffectiveToDate) || DateUtils.isSameDay(theEffectiveToDate, theQualEndDate));
	
		return aOutOfRangeFlag;
	}
	
	public boolean isBOMEligibleforFutureYearQualification(GPMSourceIVA aSrcIVA, BOM theBOM) throws Exception
	{
		//Considering priority not null is BOM has been saved. Priority will be populated either integration or UI
		if (theBOM.priority != null || this.checkEligibilityForAnnualQualificationTrigger(theBOM.org_code, aSrcIVA.ftaCode, aSrcIVA.ctryOfImport))
		{
			Date futureFromDate = this.getFutureQualificationFromDate();
			Date futureToDate = this.getFutureQualificationToDate();
			
			Date ivaEffectiveFromDate = aSrcIVA.effectiveFrom;
			Calendar aEffectiveFromCalendar = Calendar.getInstance();
			aEffectiveFromCalendar.setTime(ivaEffectiveFromDate);
			
			Calendar aCalendar = Calendar.getInstance();
			aCalendar.setTime(new Date(System.currentTimeMillis()));
			
			if (ivaEffectiveFromDate != null && (aEffectiveFromCalendar.get(Calendar.YEAR) < aCalendar.get(Calendar.YEAR))) return false;
            //IVA effective check and BOM Effective check
			if (this.isQualifiedDateRange(futureFromDate, futureToDate, aSrcIVA.effectiveFrom, aSrcIVA.effectiveTo) && this.isQualifiedDateRange(futureFromDate, futureToDate, theBOM.effective_from, theBOM.effective_to))
			{
				MessageFormatter.debug(logger, "isBOMEligibleforFutureYearQualification", "BOM [{0}]: Trade Lane [{1}/{2}] - is eligible for Future year qualification.",
						theBOM.bom_id,
                        aSrcIVA.ftaCode, 
                        aSrcIVA.ctryOfImport);
				return true;
			}
		}
		
		MessageFormatter.debug(logger, "isBOMEligibleforFutureYearQualification", "BOM [{0}]: Trade Lane [{1}/{2}] - is NOT eligible for Future year qualification.", 
				theBOM.bom_id, 
				aSrcIVA.ftaCode,
				aSrcIVA.ctryOfImport
			);

		return false;
	}
	
	public boolean isBOMEligibleForCurrentYearQualification(GPMSourceIVA aSrcIVA, BOM theBOM) throws Exception  {
		Date today = new Date(System.currentTimeMillis());
		//Check for Date, is in Current Year 
		if (aSrcIVA.effectiveFrom.after(today) || aSrcIVA.effectiveTo.before(today)) {
			MessageFormatter.debug(logger, "isBOMEligibleForCurrentYearQualification", "BOM [{0}]: Trade Lane [{1}/{2}] - is NOT Eligible for current year Qualification.", 
					theBOM.bom_id, 
					aSrcIVA.ftaCode,
					aSrcIVA.ctryOfImport
				);
			return false;
		}
		
		if (isQualifiedDateRange(aSrcIVA.effectiveFrom, aSrcIVA.effectiveTo, theBOM.effective_from, theBOM.effective_to)) {
			MessageFormatter.debug(logger, "isBOMEligibleForCurrentYearQualification", "BOM [{0}]: Trade Lane [{1}/{2}] - is Eligible for current year Qualification.", 
					theBOM.bom_id, 
					aSrcIVA.ftaCode,
					aSrcIVA.ctryOfImport
				);
			return true;
		}
		
		MessageFormatter.debug(logger, "isBOMEligibleForCurrentYearQualification", "BOM [{0}]: Trade Lane [{1}/{2}] - is NOT Eligible for current year Qualification.", 
				theBOM.bom_id, 
				aSrcIVA.ftaCode,
				aSrcIVA.ctryOfImport
			);
		return false;
		
	}
	

	
	

	public  void setQualTXHeaderHSNumber(QualTX qualtx, List<GPMClassification> classificationList) throws Exception
	{
		
		String ctryCode = qualtx.ctry_of_import;
		String manfCtry = null;
		int size = -1;
		Map<String, Object> dataMap = new HashMap<>();
		SubPullConfigData subpullConfigDate = getSubPullConfig(qualtx.org_code, qualtx.fta_code, qualtx.ctry_of_import);
		if (subpullConfigDate != null)
		{
			String lengthStr = subpullConfigDate.getHeaderHsLength();

			if (lengthStr != null && !lengthStr.isEmpty()) size = Integer.parseInt(lengthStr);

			String headerHsCtry = subpullConfigDate.getHeaderCtry();
			String hsFromManufacture = subpullConfigDate.getManufactureCountry();

			if (hsFromManufacture != null && "Y".equalsIgnoreCase(hsFromManufacture)) manfCtry = qualtx.ctry_of_manufacture;
			
			if (headerHsCtry != null && !headerHsCtry.isEmpty()) ctryCode = subpullConfigDate.getHeaderCtry();
			
			if(ctryCode == null || "".equals(ctryCode) || "DEFAULT".equals(ctryCode))
				ctryCode = qualtx.ctry_of_import;
		
						
			//Check by Manufacturer Country if configured.
			if(manfCtry != null)
				dataMap	= getComplianceHsData(classificationList, qualtx.org_code, qualtx.effective_from, qualtx.effective_to, manfCtry, size);	
			
			//Check by basic Header Country Configuration
			if(dataMap.isEmpty())
				dataMap = getComplianceHsData(classificationList, qualtx.org_code, qualtx.effective_from, qualtx.effective_to, ctryCode, size);
			
			//Check by fall-back  Header Country Configuration
			if (dataMap.isEmpty())
			{
				for (BaseHSFallConfg baseHsconf : subpullConfigDate.getBaseHSfallConfList())
				{
					dataMap = getComplianceHsData(classificationList, qualtx.org_code, qualtx.effective_from, qualtx.effective_to, baseHsconf.getHeaderCtry(), Integer.valueOf(baseHsconf.getHeaderHsLength()));
					if (!dataMap.isEmpty()) 
					{
						ctryCode = baseHsconf.getHeaderCtry();
						break;
					}
				}

			}
		}
		else
		{
			dataMap = getComplianceHsData(classificationList, qualtx.org_code, qualtx.effective_from, qualtx.effective_to, ctryCode, size);
		}
		

		if (!dataMap.isEmpty())
		{
			if (dataMap.get(HS_NUM) != null) qualtx.hs_num = (String) dataMap.get(HS_NUM);
			if (dataMap.get(PROD_CTRY_CMP_KEY) != null) qualtx.prod_ctry_cmpl_key = (Long) dataMap.get(PROD_CTRY_CMP_KEY);
			if (dataMap.get(SUB_PULL_CTRY) != null) qualtx.sub_pull_ctry = (String) dataMap.get(SUB_PULL_CTRY);
		}
		
	}

	public  Map<String, Object> getComplianceHsData(List<GPMClassification> classificationList, String orgCode, Date ivaEffectiveFrom, Date ivaEffectiveTo, String ctrycode, int size) throws Exception
	{
		Map<String, Object> dataMap = new HashMap<>();
		if (classificationList == null || classificationList.isEmpty()) return dataMap;
		
		SimplePropertySheet propsertySheet = this.propertySheetManager.getPropertySheet(orgCode, "FTA_HS_CONFIGURATION_LEVEL");
		
		List<String> propertyValue = new ArrayList<>();
		if(propsertySheet != null)
			propertyValue = Arrays.asList(propsertySheet.getStringValueList("BOM"));
		else
			propertyValue.add(CURRENT_DATE);
		

		for (GPMClassification pmClassification : classificationList)
		{
			boolean matched = false;
			matched = (pmClassification.ctryCode.equalsIgnoreCase(ctrycode) && (pmClassification.imHS1 != null && "Y".equalsIgnoreCase(pmClassification.isActive) && !pmClassification.imHS1.isEmpty()));

			boolean hsLevelPass = false;
			if (propertyValue.contains(IVA_EFFECTIVE_FROM)) hsLevelPass = hsLevelPass || (pmClassification.effectiveFrom != null && (pmClassification.effectiveFrom.before(ivaEffectiveFrom) || pmClassification.effectiveFrom.equals(ivaEffectiveFrom)) && (pmClassification.effectiveTo != null && (pmClassification.effectiveTo.after(ivaEffectiveTo) || pmClassification.effectiveTo.equals(ivaEffectiveTo))));
			if (propertyValue.contains(CURRENT_DATE))
			{
				Calendar calender = Calendar.getInstance();
				hsLevelPass = hsLevelPass || (pmClassification.effectiveFrom != null && (pmClassification.effectiveFrom.before(calender.getTime()) || pmClassification.effectiveFrom.equals(calender.getTime())) && (pmClassification.effectiveTo != null && (pmClassification.effectiveTo.after(ivaEffectiveTo) || ivaEffectiveTo.equals(pmClassification.effectiveTo))));
			}

			if (matched && hsLevelPass)
			{
				dataMap.put(HS_NUM, (size != -1 && pmClassification.imHS1.length() > size ? pmClassification.imHS1.substring(0, size) : pmClassification.imHS1));
				dataMap.put(PROD_CTRY_CMP_KEY, pmClassification.cmplKey);
				dataMap.put(SUB_PULL_CTRY, pmClassification.ctryCode);
				break;
			}
		}
		return dataMap;
	}

	public  void setQualTXComponentHSNumber(QualTXComponent aQualTXComp, List<GPMClassification> classificationList) throws Exception
	{
		String ctryCode = aQualTXComp.qualTX.ctry_of_import;
		String manfCtry = null;
		int size = -1;
		Map<String, Object> dataMap = new HashMap<>();
		SubPullConfigData subpullConfigDate = getSubPullConfig(aQualTXComp.org_code, aQualTXComp.qualTX.fta_code, aQualTXComp.qualTX.ctry_of_import);
		if (subpullConfigDate != null)
		{
			String lengthStr = subpullConfigDate.getCompHsLength();
			if (lengthStr != null && !lengthStr.isEmpty()) size = Integer.parseInt(lengthStr);

			String compHsCtry = subpullConfigDate.getCompCtry();
			String hsFromManufacture = subpullConfigDate.getManufactureCountry();

			if (hsFromManufacture != null && "Y".equalsIgnoreCase(hsFromManufacture)) manfCtry = aQualTXComp.qualTX.ctry_of_manufacture;
			
			if (compHsCtry != null && !compHsCtry.isEmpty()) ctryCode = subpullConfigDate.getCompCtry();
			
			if(ctryCode == null || "".equals(ctryCode) || "DEFAULT".equals(ctryCode))
				ctryCode = aQualTXComp.qualTX.ctry_of_import;
			
			//Check by Manufacturer Country if configured.
			if(manfCtry != null)
				dataMap = getComplianceHsData(classificationList, aQualTXComp.org_code, aQualTXComp.qualTX.effective_from, aQualTXComp.qualTX.effective_to, manfCtry, size);
			
			//Check by basic Header Country Configuration
			if(dataMap.isEmpty())
				dataMap = getComplianceHsData(classificationList, aQualTXComp.org_code, aQualTXComp.qualTX.effective_from, aQualTXComp.qualTX.effective_to, ctryCode, size);
			
			
			//Check by basic Component Country Configuration
			if (dataMap.isEmpty())
			{
				for (BaseHSFallConfg baseHsconf : subpullConfigDate.getBaseHSfallConfList())
				{
					dataMap = getComplianceHsData(classificationList, aQualTXComp.org_code, aQualTXComp.qualTX.effective_from, aQualTXComp.qualTX.effective_to, baseHsconf.getCompCtry(), Integer.valueOf(baseHsconf.getCompHslength()));
					if (!dataMap.isEmpty()) break;
				}
			}
		}
		else
		{
			dataMap = getComplianceHsData(classificationList, aQualTXComp.org_code, aQualTXComp.qualTX.effective_from, aQualTXComp.qualTX.effective_to, ctryCode, size);
		}
		

		if (!dataMap.isEmpty())
		{
			if (dataMap.get(HS_NUM) != null) aQualTXComp.hs_num = (String) dataMap.get(HS_NUM);
			if (dataMap.get(PROD_CTRY_CMP_KEY) != null) aQualTXComp.prod_ctry_cmpl_key = (Long) dataMap.get(PROD_CTRY_CMP_KEY);
			if (dataMap.get(SUB_PULL_CTRY) != null) aQualTXComp.sub_pull_ctry = (String) dataMap.get(SUB_PULL_CTRY);
		}
		
	}

	public  SubPullConfigData getSubPullConfig(String orgCode, String ftaCode, String ctryOfImport) throws Exception
	{
		SubPullConfigData subpullConfigDate = null;
		List<TradeLane> subPullConfig = qeConfigCache.getQEConfig(orgCode).getSubpullConfigList();
		if (subPullConfig != null)
		{
			Optional<TradeLane> tradeLane = subPullConfig.stream().filter(p -> p.getFtaCode().equalsIgnoreCase(ftaCode) && p.getCtryOfImport().equalsIgnoreCase(ctryOfImport)).findFirst();
			if (tradeLane.isPresent())
			{
				SubPullConfigContainer container = qeConfigCache.getQEConfig(orgCode).getSubPullConfigContainer();
				subpullConfigDate = container.getTradeLaneData(tradeLane.get());
			}
		}
		return subpullConfigDate;
	}
	
	
	public boolean isKnitToShapeChecked (BOM theBom) throws Exception
	{
		for (BOMDataExtension bomDataExtension : theBom.deList)
		{
			if ("IMPL_BOM_PROD_FAMILY:TEXTILES".equalsIgnoreCase(bomDataExtension.group_name))
			{
				DataExtensionConfiguration aDeCfg = this.dataExtensionConfigRepos.getDataExtensionConfiguration(bomDataExtension.group_name);
				if (aDeCfg != null)
				{
					Map<String, String> deColumnMap = aDeCfg.getFlexColumnMapping();
					String phyColumnName = deColumnMap.get("KNIT_TO_SHAPE");
					if ("Y".equals(bomDataExtension.getValue(phyColumnName))) return true;
				}
			}
		}

		return false;
	}
	public boolean isThereAnyEssentialComponent (BOM theBom) throws Exception
	{
		for (BOMComponent bomComponent : theBom.compList)
		{
			if ("Y".equals(bomComponent.essential_character)) return true;
		}
		return false;
	}
	
	public void populateRollupPriceDetails(BOM theBOM, QualTX theQualtx, String theRequestedPriceType) throws Exception
	{
		
		List<BOMDataExtension> appBOMDEList = getApplicableBomDEs(theBOM.deList, theQualtx.fta_code, theQualtx.ctry_of_import, theQualtx.effective_from, theQualtx.effective_to);

		if (appBOMDEList != null && !appBOMDEList.isEmpty())
		{
			DataExtensionConfiguration aDEConfig = this.dataExtensionConfigRepos.getDataExtensionConfiguration("BOM_STATIC:COST_ELEMENT");
			Map<String, String> deColumnMap = aDEConfig.getFlexColumnMapping();
			if (deColumnMap != null)
			{
				for (BOMDataExtension bomDataExtension : appBOMDEList)
				{
					
					String cctPhyColumnName = deColumnMap.get("COST_CATEGORY");
					String priceType = (String) bomDataExtension.getValue(cctPhyColumnName);
					if(!"ALL".equalsIgnoreCase(theRequestedPriceType) && !priceType.equalsIgnoreCase(theRequestedPriceType)) continue;
					
					QualTXPrice aQualTXPrice = theQualtx.getQualtxPrice(priceType);
					if(aQualTXPrice == null) aQualTXPrice = theQualtx.createPrice();

					
					String cvPhyColumnName = deColumnMap.get("CONVERTED_VALUE");
					String rawValue = (String) bomDataExtension.getValue(cvPhyColumnName);
					
					Double deValue = null;
					if(rawValue != null) deValue = Double.valueOf(rawValue);
					
					aQualTXPrice.price_type = priceType;
					aQualTXPrice.price = deValue;
					aQualTXPrice.currency_code = theBOM.currency_code;

					if ("TRANSACTION_VALUE".equalsIgnoreCase(priceType) && deValue != null)
					{
						theQualtx.value = deValue;
					}

					if ("NET_COST".equalsIgnoreCase(priceType) && deValue != null)
					{
						theQualtx.cost = deValue;
					}
				}
			}
		}
	}
	
	public List<BOMDataExtension> getApplicableBomDEs(List<BOMDataExtension> deList, String ftaCode, String coi, Date effectiveFrom, Date effectiveTO) throws Exception
	{
		List<BOMDataExtension> appBOMDEs = new ArrayList<BOMDataExtension>();
		if (deList != null && !deList.isEmpty())
		{
			DataExtensionConfiguration aDEConfig = this.dataExtensionConfigRepos.getDataExtensionConfiguration("BOM_STATIC:COST_ELEMENT");
			Map<String, String> deColumnMap = aDEConfig.getFlexColumnMapping();
			if (deColumnMap != null)
			{
				for (BOMDataExtension aBOMDE : deList)
				{
					if (!"BOM_STATIC:COST_ELEMENT".equalsIgnoreCase(aBOMDE.group_name)) continue;

					String ctPhyColumnName = deColumnMap.get("COST_TYPE");
					String costType = (String) aBOMDE.getValue(ctPhyColumnName);
					if (!"ROLLUP".equalsIgnoreCase(costType)) continue;

					String ftaPhyColumnName = deColumnMap.get("FTA");
					String coiPhyColumnName = deColumnMap.get("COI");
					String deFtaCode = (String) aBOMDE.getValue(ftaPhyColumnName);
					String deCOI = (String) aBOMDE.getValue(coiPhyColumnName);
					if (!ftaCode.equalsIgnoreCase(deFtaCode) || !coi.equalsIgnoreCase(deCOI)) continue;

					String eFromPhyColumnName = deColumnMap.get("EFFECTIVE_FROM");
					String eToPhyColumnName = deColumnMap.get("EFFECTIVE_TO");
					Date eFromDate = (Date) aBOMDE.getValue(eFromPhyColumnName);
					Date eToDate = (Date) aBOMDE.getValue(eToPhyColumnName);
					if (DateUtils.isSameDay(effectiveFrom, eFromDate) 
							&& (DateUtils.isSameDay(eToDate, effectiveTO)))
					{
						appBOMDEs.add(aBOMDE);
					}

				}
			}
		}
		return appBOMDEs;
	}
		
}
