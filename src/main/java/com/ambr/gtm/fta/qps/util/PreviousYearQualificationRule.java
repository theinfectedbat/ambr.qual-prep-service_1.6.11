package com.ambr.gtm.fta.qps.util;

import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.gtm.fta.qps.bom.BOMComponent;
import com.ambr.gtm.fta.qps.bom.BOMDataExtension;
import com.ambr.gtm.fta.qps.bom.BOMUniverse;
import com.ambr.gtm.fta.qps.bom.BOMUniversePartition;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsCache;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceCampaignDetail;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVA;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAProductSourceContainer;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponent;
import com.ambr.gtm.fta.qts.config.QEConfigCache;
import com.ambr.gtm.fta.qts.util.TradeLane;
import com.ambr.gtm.fta.qts.util.TradeLaneContainer;
import com.ambr.gtm.fta.qts.util.TradeLaneData;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfiguration;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfigurationRepository;
import com.ambr.gtm.utils.legacy.rdbms.de.GroupNameSpecification;

public class PreviousYearQualificationRule
{
	private QEConfigCache				qeConfigCache;
	private DataExtensionConfigurationRepository dataRepos;

	public PreviousYearQualificationRule(QEConfigCache qeConfigCache,DataExtensionConfigurationRepository dataRepos,BOMUniversePartition bomUniversePartition)
	{
		this.qeConfigCache = qeConfigCache;
		this.dataRepos = dataRepos;
	}
	public boolean applyPrevYearQualForComponent(BOMComponent aBOMComp,QualTXComponent aQualTXComp, GPMSourceIVAProductSourceContainer prodSourceContainer, GPMClaimDetailsCache claimDetailsCache, DataExtensionConfigurationRepository dataExtRepos, BOMUniverse bomUniverse) throws Exception
	{
		boolean usePrevYearQualification = usePrevYearQualification(aQualTXComp,bomUniverse);

		if (usePrevYearQualification)
		{
			Date aEffectiveFrom = null;
			Date aEffectiveTo = null;
			Date prevYearQualOverrideDate = null;
			// To check for prev Year Qual Override flag and Date
			ArrayList<GPMSourceCampaignDetail> campDetailList = prodSourceContainer.campDetailList;

			for (GPMSourceCampaignDetail campDetail : campDetailList)
			{
				/*
				 * if(aQualTXComp.qualTX.fta_code.equals(campDetail.ftaCode) &&
				 * "Y".equals(campDetail.prevYearQualOverride)) {
				 */
				if ("Y".equals(campDetail.prevYearQualOverride))
				{
					prevYearQualOverrideDate = campDetail.prevYearQualOverrideDate;
					break;
				}
			}

			Date origEffectiveFrom = aQualTXComp.qualified_from;
			Date origEffectiveTo = aQualTXComp.qualified_to;
			Calendar cal = Calendar.getInstance();

			if (prevYearQualOverrideDate != null)
			{
				Calendar prevYearQualOverrideCal = Calendar.getInstance();
				prevYearQualOverrideCal.setTime(prevYearQualOverrideDate);
				cal.setTimeInMillis(origEffectiveFrom.getTime());
				cal.set(Calendar.YEAR, prevYearQualOverrideCal.get(Calendar.YEAR));
				aEffectiveFrom = new Date(cal.getTimeInMillis());
				cal.setTimeInMillis(origEffectiveTo.getTime());
				cal.set(Calendar.YEAR, prevYearQualOverrideCal.get(Calendar.YEAR));
				aEffectiveTo = new Date(cal.getTimeInMillis());
			}
			else
			{
				cal.setTimeInMillis(origEffectiveFrom.getTime());
				cal.add(Calendar.YEAR, -1);
				aEffectiveFrom = new Date(cal.getTimeInMillis());
				cal.setTimeInMillis(origEffectiveTo.getTime());
				cal.add(Calendar.YEAR, -1);
				aEffectiveTo = new Date(cal.getTimeInMillis());
			}

			GPMSourceIVA aProdSrcIva = QualTXUtility.getGPMIVARecordForPYQ(prodSourceContainer, aQualTXComp.qualTX.fta_code, aQualTXComp.qualTX.ctry_of_import, aEffectiveFrom, aEffectiveTo);

			if (aProdSrcIva != null)
			{
				if (aProdSrcIva.finalDecision == null)
				{
					aQualTXComp.qualified_from = aEffectiveFrom;
					aQualTXComp.qualified_to = aEffectiveTo;
					return false;
				}
				else
				{
					String aQualifiedFlg = ("Y".equals(aProdSrcIva.finalDecision.name()) ? "QUALIFIED" : "NOT_QUALIFIED");
					aQualTXComp.qualified_flg = aQualifiedFlg;
					//aQualTXComp.prod_src_iva_key =aProdSrcIva.ivaKey;
					aQualTXComp.prev_year_qual_applied="Y";
				}
			}
		}
		else
		{
			aQualTXComp.prev_year_qual_applied="";
		}
		return true;
	}
	
	public boolean usePrevYearQualification(QualTXComponent aQualTXComp, BOMUniverse bomUniverse) throws Exception
	{
		BOM aActualBOM = bomUniverse.getBOM(aQualTXComp.qualTX.src_key);
		return getPrevYearQualFlgFromBom(aActualBOM) && getPrevYearQualification(aQualTXComp);
	}

	public boolean getPrevYearQualFlgFromBom(BOMComponent aBOMComp,BOMUniverse bomUniverse) throws Exception
	{
		if(aBOMComp == null)
			throw new Exception ("BOM Component can not be null.");
		if(bomUniverse == null)
			throw new Exception ("BOM Universe can not be null.");
		
		BOM aBOM = aBOMComp.getBOM();
		if(aBOM == null )
			aBOM = bomUniverse.getBOM(aBOMComp.alt_key_bom);
		
		return getPrevYearQualFlgFromBom(aBOM);
	}

	public boolean getPrevYearQualFlgFromBom(BOM aBOM) throws Exception
	{
		if(aBOM == null)
			throw new Exception("BOM Can not be Null");
		
		String aBomStaticDefaultDE = "BOM_STATIC:DEFAULT";

		if (aBOM.deList == null) return false;

		for (BOMDataExtension aBOMDE : aBOM.deList)
		{
			if (aBOMDE.group_name.equalsIgnoreCase(aBomStaticDefaultDE))
			{
				Map<String, String> qualtxCOmpDtlflexFieldMap = getFeildMapping("BOM_STATIC", "DEFAULT");
				return "Y".equals(aBOMDE.getValue(qualtxCOmpDtlflexFieldMap.get("USE_PREV_YEAR_QUAL"))) ? true : false;
			}
		}
		return false;
	}
	public boolean getPrevYearQualification(QualTXComponent aQualTXComp) throws Exception
	{
		TradeLane aTradeLane = getTradeLane(aQualTXComp.org_code, aQualTXComp.qualTX.fta_code, aQualTXComp.qualTX.ctry_of_import);
		if (aTradeLane != null)
		{
			TradeLaneContainer laneContainer = this.qeConfigCache.getQEConfig(aQualTXComp.org_code).getTradeLaneContainer();
			if (laneContainer != null)
			{
				TradeLaneData laneData = laneContainer.getTradeLaneData(aTradeLane);
				if(laneData.isUserPriviousYearQualification())
				{
					String aPrevYearQualUpto = laneData.getUsePrevYearQualUpto();
					return isQualUptoApplicable(aPrevYearQualUpto);
				}
			}
		}
		return false;
	}
	
	public boolean isQualUptoApplicable(String previousDate) 
	{
		if(previousDate == null || "".equals(previousDate.trim()))
			return true;
		
		LocalDate qualUptoDate = LocalDate.parse(previousDate+"-"+Calendar.getInstance().get(Calendar.YEAR), DateTimeFormatter.ofPattern("dd-MMM-yyyy"));
		return LocalDate.now().compareTo(qualUptoDate) <= 0;
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
	
	public Map<String, String> getFeildMapping(String deName, String ftaCodeGroup) throws Exception
	{
		String groupName = MessageFormat.format("{0}{1}{2}", deName, GroupNameSpecification.SEPARATOR, ftaCodeGroup);
		DataExtensionConfiguration aCfg = this.dataRepos.getDataExtensionConfiguration(groupName);
		return aCfg.getFlexColumnMapping();
	}

}
