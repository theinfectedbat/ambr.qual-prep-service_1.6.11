package com.ambr.gtm.fta.qps.util;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.ambr.gtm.fta.qps.bom.BOMComponent;
import com.ambr.gtm.fta.qps.bom.BOMDataExtension;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsCache;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVA;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAProductSourceContainer;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponent;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponentDataExtension;
import com.ambr.gtm.fta.qts.config.QEConfigCache;
import com.ambr.gtm.fta.qts.util.TradeLane;
import com.ambr.gtm.fta.qts.util.TradeLaneContainer;
import com.ambr.gtm.fta.qts.util.TradeLaneData;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfigurationRepository;

public class PreviousYearQualificationRule
{
	private QEConfigCache				qeConfigCache;

	public PreviousYearQualificationRule(QEConfigCache qeConfigCache)
	{
		this.qeConfigCache = qeConfigCache;
	}
	public boolean applyPrevYearQualForComponent(BOMComponent aBOMComp,QualTXComponent aQualTXComp, GPMSourceIVAProductSourceContainer prodSourceContainer, GPMClaimDetailsCache claimDetailsCache, DataExtensionConfigurationRepository dataExtRepos) throws Exception
	{
		boolean usePrevYearQualification = usePrevYearQualification(aBOMComp, aQualTXComp);

		if (usePrevYearQualification)
		{
			Date aEffectiveFrom = null;
			Date aEffectiveTo = null;
			Date prevYearQualOverrideDate = null;

			// TODO: To check for prev Year Qual Override flag and Date
			/*
			 * if (prodSourceContainer != null) { prodSourceContainer.
			 * RowDataContainer aGPMSrcCampaignDtlDE =
			 * aGPMSrcContainer.getChildRecord(aGPMSrcContainer.getTableDef(
			 * ).getTableName() + "_CAMPAIGN_DETAILS_DEFAULT", 0); if
			 * (aGPMSrcCampaignDtlDE != null &&
			 * aGPMSrcCampaignDtlDE.getBooleanValue( "PREV_YEAR_QUAL_OVERRIDE"))
			 * { prevYearQualOverrideDate = aGPMSrcCampaignDtlDE.getDateValue(
			 * "PREV_YEAR_QUAL_OVERRIDE_DATE"); } }
			 */
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

			GPMSourceIVA aProdSrcIva = QualTXUtility.getGPMIVARecord(prodSourceContainer, aQualTXComp.qualTX.fta_code, aQualTXComp.qualTX.ctry_of_import, aEffectiveFrom, aEffectiveTo);

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
				
					QualTXComponentDataExtension qualTXCompDetals = null;
					if (aQualTXComp.deList != null && !aQualTXComp.deList.isEmpty())
					{
						for (QualTXComponentDataExtension qualTXCompDe : aQualTXComp.deList)
						{
							if (qualTXCompDe.group_name.contains("QUALTX:COMP_DTLS"))
							{
								qualTXCompDetals = qualTXCompDe;
								break;
							}
						}
					}
					if (qualTXCompDetals == null)
					{
						qualTXCompDetals = aQualTXComp.createDataExtension("QUALTX:COMP_DTLS", dataExtRepos, null);
					}

					/*Map<String, String> qualtxCOmpDtlflexFieldMap = FlexConfigManager.getInstance().getFlexConfigContainer(aQualTXComp.org_code).getFlexColumn("QUALTX", "COMP_DTLS");

					qualTXCompDetals.deFieldMap.put(qualtxCOmpDtlflexFieldMap.get("PREV_YEAR_QUAL_APPLIED"), "Y");*/
					return true;
				}
			}

		}
		return true;
	}
	
	public boolean usePrevYearQualification(BOMComponent aBOMComp,QualTXComponent aQualTXComp) throws Exception
	{
		return getPrevYearQualFlgFromBom(aBOMComp) && getPrevYearQualification(aQualTXComp);
	}
	
	public boolean getPrevYearQualFlgFromBom(BOMComponent aBOMComp) throws Exception
	{
		String aBomStaticDefaultDE = "BOM_STATIC:DEFAULT";

		for (BOMDataExtension aBOMDE : aBOMComp.getBOM().deList)
		{
			if (aBOMDE.group_name.equalsIgnoreCase(aBomStaticDefaultDE))
			{ 
				/*Map<String,String> qualtxCOmpDtlflexFieldMap = FlexConfigManager.getInstance().getFlexConfigContainer(aBOMComp.org_code).getFlexColumn("BOM_STATIC","DEFAULT");
				
				return "Y".equals(aBOMDE.getValue(qualtxCOmpDtlflexFieldMap.get("USE_PREV_YEAR_QUAL"))) ? true : false;*/
				return true;
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
				return laneData.isUserPriviousYearQualification();
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

}
