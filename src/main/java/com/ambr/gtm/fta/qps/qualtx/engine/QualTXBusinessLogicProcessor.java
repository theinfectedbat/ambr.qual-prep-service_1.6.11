package com.ambr.gtm.fta.qps.qualtx.engine;

import java.text.SimpleDateFormat;
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
		int size = -1;

		SubPullConfigData subpullConfigDate = getSubPullConfig(qualtx.org_code, qualtx.fta_code, qualtx.ctry_of_import);
		if (subpullConfigDate != null)
		{
			String lengthStr = subpullConfigDate.getHeaderHsLength();

			if (lengthStr != null && !lengthStr.isEmpty()) size = Integer.parseInt(lengthStr);

			String headerHsCtry = subpullConfigDate.getHeaderCtry();
			String hsFromManufacture = subpullConfigDate.getManufactureCountry();

			if (hsFromManufacture != null && "Y".equalsIgnoreCase(hsFromManufacture)) ctryCode = qualtx.ctry_of_manufacture;
			else if (headerHsCtry != null && !headerHsCtry.isEmpty()) ctryCode = subpullConfigDate.getHeaderCtry();
			if(ctryCode == null || "".equals(ctryCode) || "DEFAULT".equals(ctryCode))
				ctryCode = qualtx.ctry_of_import;
		}
		Map<String, Object> dataMap = getComplianceHsDate(classificationList, qualtx.org_code, qualtx.effective_from, qualtx.effective_to, ctryCode, size);
		if (dataMap.isEmpty() && subpullConfigDate != null)
		{
			for (BaseHSFallConfg baseHsconf : subpullConfigDate.getBaseHSfallConfList())
			{
				dataMap = getComplianceHsDate(classificationList, qualtx.org_code, qualtx.effective_from, qualtx.effective_to, baseHsconf.getHeaderCtry(), Integer.valueOf(baseHsconf.getHeaderHsLength()));
				if (!dataMap.isEmpty()) break;
			}

		}
		if (!dataMap.isEmpty())
		{
			qualtx.hs_num = dataMap.get(HS_NUM) != null ? (String) dataMap.get(HS_NUM) : "";
			
			//TODO sankar, can now be null, is there a reason to map a null to a zero?
			qualtx.prod_ctry_cmpl_key = dataMap.get(PROD_CTRY_CMP_KEY) != null ? (Long) dataMap.get(PROD_CTRY_CMP_KEY) : 0;
			qualtx.sub_pull_ctry = ctryCode;
		}
	}

	public  Map<String, Object> getComplianceHsDate(List<GPMClassification> classificationList, String orgCode, Date ivaEffectiveFrom, Date ivaEffectiveTo, String ctrycode, int size) throws Exception
	{
		Map<String, Object> dataMap = new HashMap<>();
		SimplePropertySheet propsertySheet = this.propertySheetManager.getPropertySheet(orgCode, "FTA_HS_CONFIGURATION_LEVEL");
		List<String> propertyValue = Arrays.asList(propsertySheet.getStringValueList("BOM"));

		if (classificationList == null || classificationList.isEmpty()) return dataMap;

		for (GPMClassification pmClassification : classificationList)
		{
			boolean matched = false;
			matched = (pmClassification.ctryCode.equalsIgnoreCase(ctrycode) && (pmClassification.imHS1 != null && "Y".equalsIgnoreCase(pmClassification.isActive) && !pmClassification.imHS1.isEmpty()));

			boolean hsLevelPass = false;
			if (propertyValue.contains(IVA_EFFECTIVE_FROM)) hsLevelPass = hsLevelPass || ((pmClassification.effectiveFrom.before(ivaEffectiveFrom) || pmClassification.effectiveFrom.equals(ivaEffectiveFrom)) && (pmClassification.effectiveTo.after(ivaEffectiveTo) || pmClassification.effectiveTo.equals(ivaEffectiveTo)));
			if (propertyValue.contains(CURRENT_DATE))
			{
				Calendar calender = Calendar.getInstance();
				hsLevelPass = hsLevelPass || ((pmClassification.effectiveFrom.before(calender.getTime()) || pmClassification.effectiveFrom.equals(calender.getTime())) && (pmClassification.effectiveTo.after(ivaEffectiveTo) || ivaEffectiveTo.equals(pmClassification.effectiveTo)));
			}

			if (matched && hsLevelPass)
			{
				dataMap.put(HS_NUM, (size != -1 && pmClassification.imHS1.length() > size ? pmClassification.imHS1.substring(0, size) : pmClassification.imHS1));
				dataMap.put(PROD_CTRY_CMP_KEY, pmClassification.cmplKey);
				break;
			}
		}
		return dataMap;
	}

	public  void setQualTXComponentHSNumber(QualTXComponent aQualTXComp, List<GPMClassification> classificationList) throws Exception
	{
		String ctryCode = aQualTXComp.qualTX.ctry_of_import;
		int size = -1;
		SubPullConfigData subpullConfigDate = getSubPullConfig(aQualTXComp.org_code, aQualTXComp.qualTX.fta_code, aQualTXComp.qualTX.ctry_of_import);
		if (subpullConfigDate != null)
		{
			String lengthStr = subpullConfigDate.getCompHsLength();
			if (lengthStr != null && !lengthStr.isEmpty()) size = Integer.parseInt(lengthStr);

			String compHsCtry = subpullConfigDate.getCompCtry();
			String hsFromManufacture = subpullConfigDate.getManufactureCountry();

			if (hsFromManufacture != null && "Y".equalsIgnoreCase(hsFromManufacture)) ctryCode = aQualTXComp.qualTX.ctry_of_manufacture;
			else if (compHsCtry != null && !compHsCtry.isEmpty()) ctryCode = subpullConfigDate.getCompCtry();
			if(ctryCode == null || "".equals(ctryCode) || "DEFAULT".equals(ctryCode))
				ctryCode = aQualTXComp.qualTX.ctry_of_import;
		}
		Map<String, Object> dataMap = getComplianceHsDate(classificationList, aQualTXComp.org_code, aQualTXComp.qualTX.effective_from, aQualTXComp.qualTX.effective_to, ctryCode, size);
		if (dataMap.isEmpty() && subpullConfigDate!= null)
		{
			for (BaseHSFallConfg baseHsconf : subpullConfigDate.getBaseHSfallConfList())
			{
				dataMap = getComplianceHsDate(classificationList, aQualTXComp.org_code, aQualTXComp.qualTX.effective_from, aQualTXComp.qualTX.effective_to, baseHsconf.getCompCtry(), Integer.valueOf(baseHsconf.getCompHslength()));
				if (!dataMap.isEmpty()) break;
			}

		}
		if (!dataMap.isEmpty())
		{
			aQualTXComp.hs_num = dataMap.get(HS_NUM) != null ? (String) dataMap.get(HS_NUM) : "";
			if(dataMap.get(PROD_CTRY_CMP_KEY) != null)
				aQualTXComp.prod_ctry_cmpl_key =  (Long) dataMap.get(PROD_CTRY_CMP_KEY);
			
			aQualTXComp.sub_pull_ctry = ctryCode;
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
	
		
}
