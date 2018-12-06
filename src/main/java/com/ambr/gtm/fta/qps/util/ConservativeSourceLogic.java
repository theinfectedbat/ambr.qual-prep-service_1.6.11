package com.ambr.gtm.fta.qps.util;

import java.text.MessageFormat;
import java.util.Date;
import java.util.Map;

import com.ambr.gtm.fta.list.FTAListContainer;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetails;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsCache;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVA;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAProductContainer;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAProductSourceContainer;
import com.ambr.gtm.fta.qps.gpmsrciva.STPDecisionEnum;
import com.ambr.gtm.fta.qts.config.FTAHSListCache;
import com.ambr.gtm.fta.qts.config.QEConfigCache;
import com.ambr.gtm.fta.qts.util.CumulationConfigContainer;
import com.ambr.gtm.fta.qts.util.CumulationConfigContainer.ComponentAgreement;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfiguration;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfigurationRepository;
import com.ambr.gtm.utils.legacy.rdbms.de.GroupNameSpecification;
import com.ambr.gtm.utils.legacy.sps.SimplePropertySheetManager;;

public class ConservativeSourceLogic
{

	GPMSourceIVAProductContainer 		sourceIVsAByProduct;
	GPMClaimDetailsCache		 		claimDetailsCache;
	DataExtensionConfigurationRepository dataRepos;
	public ConservativeSourceDetails 	conservativeSrcDetails;
	
	private QEConfigCache beanQEConfigCache;
	public FTAHSListCache ftaHSListCache;
	public ConservativeSourceLogic(GPMSourceIVAProductContainer theSourceIVsAByProduct, GPMClaimDetailsCache theClaimDetailsCache, QEConfigCache beanQEConfigCache, DataExtensionConfigurationRepository dataRepos,FTAHSListCache ftaHSListCache) throws Exception
	{
		this.sourceIVsAByProduct =  theSourceIVsAByProduct;
		this.claimDetailsCache = theClaimDetailsCache;
		this.beanQEConfigCache = beanQEConfigCache;
		this.dataRepos = dataRepos;
		this.ftaHSListCache=ftaHSListCache;
	}

	public ConservativeSourceDetails determineConservativeSrcIVA(String theOrgCode, String theFTACode, String theIVACode, String theCOI, Date theEffectiveFrom, Date theEffectiveTo, SimplePropertySheetManager propertySheetManager) throws Exception
	{
		GPMSourceIVA aConservativeSourceIVA = null;
		GPMSourceIVAProductSourceContainer aConservativeSource = null;
		Double aNoDecisionCumulationValue = null;
		Double aYesDecisionTracedValue = null;
		boolean foundSrcWithFinalDecisionNO = false;
		boolean foundSrcWithEmptyFinalDecision = false;
		
		conservativeSrcDetails = new ConservativeSourceDetails();
		
		CumulationConfigContainer aCumululationConfigContainer = this.beanQEConfigCache.getQEConfig(theOrgCode).getCumulationConfig();
		String ftaCodeGroup = QualTXUtility.determineFTAGroupCode(theOrgCode, theFTACode, propertySheetManager);
		Map<String,String> flexFieldMap = getFeildMapping("STP", ftaCodeGroup);
		
		for(GPMSourceIVAProductSourceContainer aSrc : this.sourceIVsAByProduct.getSourceContainers())
		{
			GPMSourceIVA aSrcIVA = aSrc.getIVA(theFTACode, theIVACode, theCOI, theEffectiveFrom, theEffectiveTo);

			//If SRC IVA is not found inclusive of IVA_CODE, try without the IVA_CODE. 
			if (aSrcIVA == null)
			{
				aSrcIVA = aSrc.getIVA(theFTACode, theCOI, theEffectiveFrom, theEffectiveTo);
			}

			//If SRC IVA is not found check if an IVA is available based on the sub-pull configuration.
			if (aSrcIVA == null )
			{
				ComponentAgreement aCompAggr = null;
				if(aCumululationConfigContainer != null)
					aCompAggr = aCumululationConfigContainer.getComponentAgreementConfigByFTACOI(theFTACode, theCOI);
				
				if(aCompAggr != null && aCompAggr.getCtryOfImport() != null){
					aSrc.getIVA(theFTACode, theIVACode, aCompAggr.getCtryOfImport(), theEffectiveFrom, theEffectiveTo);

					if (aSrcIVA == null)
					{
						aSrcIVA = aSrc.getIVA(theFTACode, aCompAggr.getCtryOfImport(), theEffectiveFrom, theEffectiveTo);
					}
				}
			}

			//If there are no SRC IVA's mark the source as conservative source.
			if (aSrcIVA == null)
			{
				conservativeSrcDetails.setConservativeSource(aSrc);
				return conservativeSrcDetails;
			}
		
			
			//If the SRC IVA has empty/null decision mark the source as conservative source.
			if(aSrcIVA.finalDecision == null){
				conservativeSrcDetails.setConservativeSource(aSrc);
				foundSrcWithEmptyFinalDecision = true;
			}
			
			//if the SRC IVA has N decision, check for SRC with the highest cumulation value.
			if (STPDecisionEnum.valueOf("N") == aSrcIVA.finalDecision &&  !foundSrcWithEmptyFinalDecision)
			{
				foundSrcWithFinalDecisionNO = true;
				double aClaimDetailCumulationValue = 0.0;
				GPMClaimDetails claimdetails = claimDetailsCache.getClaimDetails(aSrcIVA.ivaKey);
				if(claimdetails != null)
				{
					aClaimDetailCumulationValue = (Double)claimdetails.claimDetailsValue.get(flexFieldMap.get("CUMULATION_VALUE"));
				}
				if (aNoDecisionCumulationValue == null || aNoDecisionCumulationValue.doubleValue() > aClaimDetailCumulationValue)
				{
					aConservativeSourceIVA = aSrcIVA;
					aConservativeSource = aSrc;
					aNoDecisionCumulationValue = aClaimDetailCumulationValue;
				}
				
			}
			
			//if the SRC IVA has Y decision, check for SRC with the lowest traced value.
			if(STPDecisionEnum.valueOf("Y") == aSrcIVA.finalDecision  && (!foundSrcWithFinalDecisionNO || !foundSrcWithEmptyFinalDecision))
			{
				if("NAFTA".equals(theFTACode))
				{
					double aTracedValueFromClaimDtls = 0.0;
					GPMClaimDetails claimdetails = claimDetailsCache.getClaimDetails(aSrcIVA.ivaKey);
					if(claimdetails != null)
					{
						
						//TODO : Write logic for checking if the HS falls in the trace list.
						boolean fallsInTraceList = false;
						String hsNum = (String)claimdetails.claimDetailsValue.get(flexFieldMap.get("IMPORT_HS"));
						if(hsNum != null){
							//fallsInTraceList = <Utility>.existsInFTAList(theOrgCode, theCOI, hsNum, theFTACode, new Date(), "TRACE");
						String hsExcpList="ANY_"+theFTACode+"_TRACE";
						FTAListContainer ftaListContainerCache = ftaHSListCache.getFTAList(hsExcpList, theOrgCode);
						fallsInTraceList= ftaListContainerCache.doesHSExistsInList(hsNum, new Date(), hsExcpList, theOrgCode, theCOI);
						}

						if(fallsInTraceList)
						{
							aTracedValueFromClaimDtls =(Double)claimdetails.claimDetailsValue.get(flexFieldMap.get("TRACED_VALUE"));
						}
					}
					if(aYesDecisionTracedValue == null || aYesDecisionTracedValue.doubleValue() < aTracedValueFromClaimDtls)
					{
						aConservativeSourceIVA = aSrcIVA;
						aConservativeSource = aSrc;
						aYesDecisionTracedValue = aTracedValueFromClaimDtls;
					}
				}
			}
		}
		
		conservativeSrcDetails.setConservativeSource(aConservativeSource);
		conservativeSrcDetails.setConservativeSourceIVA(aConservativeSourceIVA);
		return conservativeSrcDetails;
	}
	
	public ConservativeSourceDetails getConservativeSourceDetails()
	{
		return this.conservativeSrcDetails;
	}
	public Map<String, String> getFeildMapping(String deName, String ftaCodeGroup) throws Exception {
		String groupName = MessageFormat.format("{0}{1}{2}", deName, GroupNameSpecification.SEPARATOR, ftaCodeGroup);
		DataExtensionConfiguration	aCfg = this.dataRepos.getDataExtensionConfiguration(groupName);
		return aCfg.getFlexColumnMapping();
	}
	
	public class ConservativeSourceDetails
	{
		GPMSourceIVA aConservativeSourceIVA = null;
		GPMSourceIVAProductSourceContainer aConservativeSource = null;
		
		public ConservativeSourceDetails(GPMSourceIVA theConservativeSourceIVA, GPMSourceIVAProductSourceContainer theConservativeSource)
		{
			this.aConservativeSourceIVA = theConservativeSourceIVA;
			this.aConservativeSource = theConservativeSource;
		}

		public ConservativeSourceDetails()
		{
			this.aConservativeSource = null;
			this.aConservativeSourceIVA = null;
		}

		public GPMSourceIVA getConservativeSourceIVA()
		{
			return this.aConservativeSourceIVA;
		}
		public void setConservativeSourceIVA(GPMSourceIVA theConservativeSourceIVA)
		{
			this.aConservativeSourceIVA = theConservativeSourceIVA;
		}
		
		public GPMSourceIVAProductSourceContainer getConservativeSource()
		{
			return this.aConservativeSource;
		}
		
		public void setConservativeSource(GPMSourceIVAProductSourceContainer theConservativeSource)
		{
			this.aConservativeSource = theConservativeSource;
		}
	}
}
