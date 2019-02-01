package com.ambr.gtm.fta.qps.util;

import java.sql.Timestamp;
import java.math.BigDecimal;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.qps.bom.BOMComponent;
import com.ambr.gtm.fta.qps.bom.BOMComponentDataExtension;
import com.ambr.gtm.fta.qps.bom.BOMUniverse;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetails;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsCache;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsSourceIVAContainer;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationProductContainer;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationProductContainerCache;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVA;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAContainerCache;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAProductSourceContainer;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXBusinessLogicProcessor;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponent;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponentDataExtension;
import com.ambr.gtm.fta.qps.qualtx.engine.result.TradeLaneStatusTracker;
import com.ambr.gtm.fta.qts.TrackerCodes;
import com.ambr.gtm.fta.qts.util.CumulationConfigContainer;
import com.ambr.gtm.fta.qts.util.CumulationConfigContainer.ComponentAgreement;
import com.ambr.gtm.fta.qts.util.TradeLane;
import com.ambr.gtm.fta.qts.util.TradeLaneContainer;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfigurationRepository;
import com.ambr.platform.utils.log.MessageFormatter;

//TODO need to remove component once we solve the issue of injecting QualTXResourceUtil

public class QualTXComponentUtility
{
	private static final Logger logger = LogManager.getLogger(QualTXComponentUtility.class);
	QualTXComponent 							qualTXComp;
	BOMComponent								bomComp;
	GPMClaimDetailsCache						claimDetailsCache;
	GPMSourceIVAContainerCache					ivaCache;
	DataExtensionConfigurationRepository		dataExtCfgRepos;
	GPMClassificationProductContainerCache		gpmClassCache;
	TradeLaneStatusTracker						statusTracker;
	
	QualTXBusinessLogicProcessor qualTXBusinessLogicProcessor;
	private DetermineComponentCOO determineComponentCOO;
	private CumulationComputationRule cumulationComputationRule;
	private PreviousYearQualificationRule previousYearQualificationRule;
	private BOMUniverse bomUniverse;
		
	public QualTXBusinessLogicProcessor getQualTXBusinessLogicProcessor()
	{
		return qualTXBusinessLogicProcessor;
	}

	public void setQualTXBusinessLogicProcessor(QualTXBusinessLogicProcessor qualTXBusinessLogicProcessor)
	{
		this.qualTXBusinessLogicProcessor = qualTXBusinessLogicProcessor;
		this.determineComponentCOO = this.qualTXBusinessLogicProcessor.determineComponentCOO;
		this.cumulationComputationRule = this.qualTXBusinessLogicProcessor.cumulationComputationRule;
		this.previousYearQualificationRule= this.qualTXBusinessLogicProcessor.previousYearQualificationRule;
	}

	

	
	/**
	 * @param theQualTXComp
	 * @param theBOMComponent
	 * @throws Exception
	 */
	public QualTXComponentUtility(
		QualTXComponent 		theQualTXComp, 
		BOMComponent 			theBOMComponent,
		TradeLaneStatusTracker 	theStatusTracker) 
		throws Exception
	{
		this.qualTXComp = theQualTXComp;
		this.bomComp = theBOMComponent;
		this.statusTracker = theStatusTracker;
	}
	
	/**
	 * @param theQualTXComp
	 * @param theBOMComponent
	 * @param theClaimDetailsCache
	 * @param theIVACache
	 * @param theDataExtCfgRepos
	 * @throws Exception
	 */
	public QualTXComponentUtility(
		QualTXComponent 						theQualTXComp, 
		BOMComponent 							theBOMComponent, 
		GPMClaimDetailsCache 					theClaimDetailsCache, 
		GPMSourceIVAContainerCache 				theIVACache, 
		DataExtensionConfigurationRepository 	theDataExtCfgRepos,
		TradeLaneStatusTracker 					theStatusTracker) 
		throws Exception
	{
		this.qualTXComp = theQualTXComp;
		this.bomComp = theBOMComponent;
		this.claimDetailsCache = theClaimDetailsCache;
		this.ivaCache = theIVACache;
		this.dataExtCfgRepos = theDataExtCfgRepos;
		this.statusTracker = theStatusTracker;
	}
	
	/**
	 * @param theQualTXComp
	 * @param theBOMComponent
	 * @param theGPMClassCache
	 * @throws Exception
	 */
	public QualTXComponentUtility(
		QualTXComponent 						theQualTXComp, 
		BOMComponent 							theBOMComponent, 
		GPMClassificationProductContainerCache 	theGPMClassCache,
		TradeLaneStatusTracker 					theStatusTracker) 
		throws Exception
	{
		this.qualTXComp = theQualTXComp;
		this.bomComp = theBOMComponent;
		this.gpmClassCache = theGPMClassCache;
		this.statusTracker = theStatusTracker;
	}
	
	/**
	 * @param theQualTXComp
	 * @param theBOMComponent
	 * @param theDataExtCfgRepos
	 * @throws Exception
	 */
	public QualTXComponentUtility(
		QualTXComponent 						theQualTXComp, 
		BOMComponent 							theBOMComponent, 
		DataExtensionConfigurationRepository 	theDataExtCfgRepos,
		TradeLaneStatusTracker 					theStatusTracker) 
		throws Exception
	{
		this.qualTXComp = theQualTXComp;
		this.bomComp = theBOMComponent;
		this.dataExtCfgRepos = theDataExtCfgRepos;
		this.statusTracker = theStatusTracker;
	}
	
	/**
	 * @param theQualTXComp
	 * @param theBOMComponent
	 * @param theClaimDetailsCache
	 * @param theIVACache
	 * @param theGPMClassCache
	 * @param theDataExtCfgRepos
	 * @throws Exception
	 */
	public QualTXComponentUtility(
		QualTXComponent 						theQualTXComp, 
		BOMComponent 							theBOMComponent, 
		GPMClaimDetailsCache 					theClaimDetailsCache, 
		GPMSourceIVAContainerCache 				theIVACache, 
		GPMClassificationProductContainerCache 	theGPMClassCache, 
		DataExtensionConfigurationRepository 	theDataExtCfgRepos,
		TradeLaneStatusTracker 					theStatusTracker) 
		throws Exception
	{
		this.qualTXComp = theQualTXComp;
		this.bomComp = theBOMComponent;
		this.claimDetailsCache = theClaimDetailsCache;
		this.ivaCache = theIVACache;
		this.gpmClassCache = theGPMClassCache;
		this.dataExtCfgRepos = theDataExtCfgRepos;
		this.statusTracker = theStatusTracker;
	}
	
	public void createQualTXComponentDataExtensions()
		throws Exception
	{
		String							aTargetGroupName 	= "IMPL_BOM_PROD_FAMILY:TEXTILES";
		String							aPriceDE 			= "BOM_COMP_STATIC:PRICE";
		QualTXComponentDataExtension	aQualTXCompDE;
		
		Map<String, String> deCompPriceColumnMap = this.dataExtCfgRepos.getDataExtensionConfiguration("BOM_COMP_STATIC:PRICE").getFlexColumnMapping();
		for (BOMComponentDataExtension aBOMCompDE : this.bomComp.deList) {
			
			if (aBOMCompDE.group_name.equalsIgnoreCase(aPriceDE)) {
				
				Number price = (Number)aBOMCompDE.getValue(deCompPriceColumnMap.get("VALUE"));
				String priceType = (String)aBOMCompDE.getValue(deCompPriceColumnMap.get("TYPE"));
				if(price == null || priceType == null || priceType.isEmpty())
					continue;
					
				this.qualTXComp.createPrice(price, priceType);
				continue;
			}
			
			if (aBOMCompDE.group_name.equalsIgnoreCase(aTargetGroupName)) {
				aQualTXCompDE = this.qualTXComp.createDataExtension(aBOMCompDE.group_name, this.dataExtCfgRepos, null);
				this.mapDataExtensionFields(aQualTXCompDE, aBOMCompDE);
			}
		}
		
		createCompDtlsDE();
	}
	
	private void createCompDtlsDE() throws Exception
	{
		QualTXComponentDataExtension aQualTXCompDE = null;
		if (this.qualTXComp.deList != null && !this.qualTXComp.deList.isEmpty())
		{
			for (QualTXComponentDataExtension compDE : this.qualTXComp.deList)
			{
				if (compDE.group_name.contains("QUALTX:COMP_DTLS"))
				{
					aQualTXCompDE = compDE;
					break;
				}
			}
		}

		Timestamp now = new Timestamp(System.currentTimeMillis());
		if (aQualTXCompDE == null)
		{
			aQualTXCompDE = this.qualTXComp.createDataExtension("QUALTX:COMP_DTLS", dataExtCfgRepos, null);
			aQualTXCompDE.setValue("CREATED_DATE", now);
		}
		
		aQualTXCompDE.setValue("LAST_MODIFIED_DATE", now);
		aQualTXCompDE.setValue("LAST_MODIFIED_BY", this.qualTXComp.last_modified_by);
		aQualTXCompDE.setValue("FLEXFIELD_VAR2", this.bomComp.prod_id);
	}

	public  void mapDataExtensionFields(
		QualTXComponentDataExtension 	theQualTXCompDE,
		BOMComponentDataExtension 		theBOMCompDE) 
		throws Exception
	{
		for (String aColumnName : theBOMCompDE.getColumnNames()) {
			if (aColumnName.equalsIgnoreCase("alt_key_bom") ||
				aColumnName.equalsIgnoreCase("alt_key_comp") ||
				aColumnName.equalsIgnoreCase("parent_seq_num")) 
			{
				continue;
			}
			
			theQualTXCompDE.setValue(aColumnName, theBOMCompDE.getValue(aColumnName));
		}
	}

	public void pullComponentBasicInfo(boolean isTopDown) 
		throws Exception
	{
		this.qualTXComp.area = this.bomComp.area;
		this.qualTXComp.area_uom = this.bomComp.area_uom;
		this.qualTXComp.component_type = this.bomComp.component_type;
		this.qualTXComp.cost = this.bomComp.extended_cost;
		this.qualTXComp.critical_indicator = this.bomComp.critical_indicator;
		this.qualTXComp.ctry_of_manufacture = this.bomComp.ctry_of_manufacture;
		this.qualTXComp.ctry_of_origin = this.bomComp.ctry_of_origin;
		this.qualTXComp.description = this.bomComp.description;
		this.qualTXComp.essential_character = this.bomComp.essential_character;
		this.qualTXComp.unit_weight = this.bomComp.unit_weight;
		this.qualTXComp.gross_weight = this.bomComp.net_weight == null? this.bomComp.unit_weight:this.bomComp.net_weight;
		this.qualTXComp.manufacturer_key = this.bomComp.manufacturer_key;
		this.qualTXComp.make_buy_flg = (this.bomComp.sub_bom_key != null) && (this.bomComp.sub_bom_key > 0)? "M" : "B";
		this.qualTXComp.net_weight = this.bomComp.net_weight;
		this.qualTXComp.prod_key = this.bomComp.prod_key;
		this.qualTXComp.prod_src_key = this.bomComp.prod_src_key;
		this.qualTXComp.qty_per = this.bomComp.qty_per;
		//this.qualTXComp.qualified_from = this.bomComp.effective_from;
		//this.qualTXComp.qualified_to = this.bomComp.effective_to;
		this.qualTXComp.seller_key = this.bomComp.seller_key;
		this.qualTXComp.supplier_key = this.bomComp.supplier_key;
		this.qualTXComp.src_key = this.bomComp.alt_key_comp;
		this.qualTXComp.src_id = MessageFormat.format("{0,number,#}", this.bomComp.comp_num);
		this.qualTXComp.sub_bom_id = this.bomComp.sub_bom_id;
		this.qualTXComp.sub_bom_key = this.bomComp.sub_bom_key;
		this.qualTXComp.sub_bom_org_code = this.bomComp.sub_bom_org_code;
		if(isTopDown)
			this.qualTXComp.top_down_ind = "Y";
		if(null != this.bomComp.unit_cost)
			this.qualTXComp.unit_cost = this.bomComp.unit_cost;
		this.qualTXComp.weight = this.bomComp.net_weight == null? this.bomComp.unit_weight:this.bomComp.net_weight;
		this.qualTXComp.weight_uom = this.bomComp.weight_uom;
		
		this.createQualTXComponentDataExtensions();
	}
	
	public void pullComponentData() 
		throws Exception
	{
		this.pullComponentBasicInfo(false);
		this.pullIVAData();
		this.pullCtryCmplData();
	}


	public GPMClassificationProductContainer pullCtryCmplData() 
		throws Exception
	{
		GPMClassificationProductContainer		gpmClassContainer = null;
		try {
			gpmClassContainer = this.gpmClassCache.getGPMClassificationsByProduct(this.bomComp.prod_key);
			if (gpmClassContainer == null) {
				return null;
			}
	
			if (gpmClassContainer.classificationList.size() == 0) {
				return null;
			}
	
			//TODO Make this as spring bean and use it.
			qualTXBusinessLogicProcessor.setQualTXComponentHSNumber(this.qualTXComp, gpmClassContainer.classificationList);
			if(this.statusTracker != null)
				this.statusTracker.classificationPullSuccess(this.qualTXComp);
		}
		catch (Exception e) {
			if(this.statusTracker != null)
				this.statusTracker.classificationPullFailure(this.qualTXComp, e);
		}
		return gpmClassContainer;
	}

	public GPMSourceIVAProductSourceContainer pullIVAData() 
		throws Exception
	{
		GPMSourceIVAProductSourceContainer		aSourceIVAProductSourceContainer;
		if((this.bomComp.prod_src_key == null) || this.bomComp.prod_src_key <= 0)
		{
			//TODO: Write logic for re-qualification in conservative source analysis.
			ConservativeSourceLogic consSrcLogic = new ConservativeSourceLogic(this.ivaCache.getSourceIVAByProduct(this.bomComp.prod_key), this.claimDetailsCache, qualTXBusinessLogicProcessor.qeConfigCache, qualTXBusinessLogicProcessor.getDataExtensionConfigRepos(),qualTXBusinessLogicProcessor.ftaHSListCache);
			consSrcLogic.determineConservativeSrcIVA(this.qualTXComp.org_code, this.qualTXComp.qualTX.fta_code, this.qualTXComp.qualTX.iva_code, this.qualTXComp.qualTX.ctry_of_import, this.qualTXComp.qualTX.effective_from, this.qualTXComp.qualTX.effective_to, qualTXBusinessLogicProcessor.propertySheetManager);
			if(null  !=  consSrcLogic.conservativeSrcDetails.getConservativeSource())
				this.qualTXComp.prod_src_key = consSrcLogic.conservativeSrcDetails.getConservativeSource().prodSrcKey;
			aSourceIVAProductSourceContainer = consSrcLogic.conservativeSrcDetails.getConservativeSource();
			GPMSourceIVA aConservativeSRCIVA = consSrcLogic.conservativeSrcDetails.getConservativeSourceIVA();
			if(aConservativeSRCIVA != null)
			{
				String aQualified= (aConservativeSRCIVA.finalDecision != null && "Y".equals( aConservativeSRCIVA.finalDecision.name()) ? "QUALIFIED" : "NOT_QUALIFIED");
				
				this.qualTXComp.qualified_flg = (aConservativeSRCIVA.finalDecision == null)? "" : aQualified;
				this.qualTXComp.prod_src_iva_key = aConservativeSRCIVA.ivaKey;
			}
			else if(aConservativeSRCIVA == null || aConservativeSRCIVA.ivaKey == -1){
				this.qualTXComp.qualified_flg =  "N" ;
				this.qualTXComp.prod_src_iva_key = null;
			}
		}
		else
		{	
			aSourceIVAProductSourceContainer = this.ivaCache.getSourceIVABySource(this.bomComp.prod_src_key);
			if (aSourceIVAProductSourceContainer == null) 
				return null;
			
			this.qualTXComp.prod_src_key = this.bomComp.prod_src_key;
			for (GPMSourceIVA aSrcIVA : aSourceIVAProductSourceContainer.ivaList) {
				//This api allow only SYSTEM_DECISION=M and Full Year IVA Records
				if(!aSrcIVA.isIVAEligibleForProcessing())
					continue;
				
				//Check for FTA Code && COI 
				if(!this.qualTXComp.qualTX.fta_code.equals(aSrcIVA.ftaCode) 
						|| !this.qualTXComp.qualTX.ctry_of_import.equals(aSrcIVA.ctryOfImport))
					continue;
				//Compare the Date
				if(this.qualTXComp.qualTX.effective_from.compareTo(aSrcIVA.effectiveFrom)!=0
						&& this.qualTXComp.qualTX.effective_to.compareTo(aSrcIVA.effectiveTo)!=0)
					continue;
				
				String aQualified= (aSrcIVA.finalDecision != null && "Y".equals(aSrcIVA.finalDecision.name()) ? "QUALIFIED" : "NOT_QUALIFIED");
				
				this.qualTXComp.qualified_flg = (aSrcIVA.finalDecision == null)? "" : aQualified;
				this.qualTXComp.prod_src_iva_key = aSrcIVA.ivaKey;
				break;
			}
			

			//Check for an alternative FTA using the sub-pull configuration if IVA with the contextual trade lane information does not exist.
			if (this.qualTXComp.prod_src_iva_key == null)
			{
				GPMSourceIVA aSrcIVA = null;
				CumulationConfigContainer aCumululationConfigContainer = qualTXBusinessLogicProcessor.qeConfigCache.getQEConfig(this.qualTXComp.org_code).getCumulationConfig();
				ComponentAgreement aCompAggr = null;
				if(aCumululationConfigContainer != null)
					aCompAggr = aCumululationConfigContainer.getComponentAgreementConfigByFTACOI(this.qualTXComp.qualTX.fta_code, this.qualTXComp.qualTX.ctry_of_import);
				
				if(aCompAggr != null && aCompAggr.getCtryOfImport() != null){
					aSrcIVA = aSourceIVAProductSourceContainer.getIVA(this.qualTXComp.qualTX.fta_code, this.qualTXComp.qualTX.iva_code, aCompAggr.getCtryOfImport(), this.qualTXComp.qualTX.effective_from, this.qualTXComp.qualTX.effective_to);

					if (aSrcIVA == null)
					{
						aSrcIVA = aSourceIVAProductSourceContainer.getIVA(this.qualTXComp.qualTX.fta_code, aCompAggr.getCtryOfImport(), this.qualTXComp.qualTX.effective_from, this.qualTXComp.qualTX.effective_to);
					}
					
					if(aSrcIVA != null)
					{
						String aQualified= (aSrcIVA.finalDecision != null && "Y".equals(aSrcIVA.finalDecision.name()) ? "QUALIFIED" : "NOT_QUALIFIED");
						
						this.qualTXComp.qualified_flg = (aSrcIVA.finalDecision == null)? "" : aQualified;
						this.qualTXComp.prod_src_iva_key = aSrcIVA.ivaKey;
					} else
						MessageFormatter.debug(logger, "pullIVAData", "BOM Component with Key [{0,number,#}]: did not have an IVA identified for FTA [{1}], COI[{2}], Effective From[{3, date, dd-MMM-yyyy}], Effective To[{4, date, dd-MMM-yyyy}]", 
								this.bomComp.alt_key_comp, 
								this.qualTXComp.qualTX.fta_code, this.qualTXComp.qualTX.ctry_of_import,
								this.qualTXComp.qualTX.effective_from, this.qualTXComp.qualTX.effective_to);
				}
			}
		}
		
		this.qualTXComp.setClaimDetails(
				
			this.claimDetailsCache.getClaimDetails(this.qualTXComp.prod_src_iva_key),
			this.dataExtCfgRepos
		);
		
		String coo = null;
		GPMClassificationProductContainer aGPMClassContainer = null;
		if (this.determineComponentCOO != null)
		{
			aGPMClassContainer = this.gpmClassCache.getGPMClassificationsByProduct(this.bomComp.prod_key);
			coo = determineComponentCOO.determineCOOForComponentSource(this.qualTXComp, this.bomComp, aSourceIVAProductSourceContainer, aGPMClassContainer, this.qualTXBusinessLogicProcessor.propertySheetManager);
			this.qualTXComp.ctry_of_origin = coo;
		}

		if (this.cumulationComputationRule != null) cumulationComputationRule.applyCumulationForComponent(this.qualTXComp, aSourceIVAProductSourceContainer, this.claimDetailsCache, this.dataExtCfgRepos);

		if (!"Y".equalsIgnoreCase(this.qualTXComp.cumulation_rule_applied) && (this.qualTXComp.qualified_flg == null || "".equalsIgnoreCase(this.qualTXComp.qualified_flg)))
		{
			if (this.previousYearQualificationRule != null)
			{
				Date origEffectiveFrom = this.qualTXComp.qualified_from;
				Date origEffectiveTo = this.qualTXComp.qualified_to;
				boolean isPrevYearQualApplied = previousYearQualificationRule.applyPrevYearQualForComponent(this.bomComp, this.qualTXComp, aSourceIVAProductSourceContainer, claimDetailsCache, this.dataExtCfgRepos,this.bomUniverse);
				if (!isPrevYearQualApplied)
				{
					if (this.cumulationComputationRule != null) cumulationComputationRule.applyCumulationForComponent(this.qualTXComp, aSourceIVAProductSourceContainer, this.claimDetailsCache, this.dataExtCfgRepos);
					this.qualTXComp.qualified_from = origEffectiveFrom;
					this.qualTXComp.qualified_to = origEffectiveTo;
				}
			}
		}
		else if(this.qualTXComp.qualified_flg != null || !"".equals(this.qualTXComp.qualified_flg))
		{
			this.qualTXComp.prev_year_qual_applied = "";
		}
		
		TradeLaneContainer tradelaneCintainer = this.qualTXBusinessLogicProcessor.qeConfigCache.getQEConfig(this.qualTXComp.org_code).getTradeLaneContainer();
		TradeLane tradeLane = new TradeLane(this.qualTXComp.qualTX.fta_code, this.qualTXComp.qualTX.ctry_of_import);
		
		boolean useNonOriginatingMaterials = false;
		if(tradelaneCintainer.getTradeLaneData(tradeLane) != null) tradelaneCintainer.getTradeLaneData(tradeLane).isUseNonOriginatingMaterials();
		if(useNonOriginatingMaterials && (this.qualTXComp.make_buy_flg == null || this.qualTXComp.make_buy_flg.equals("B"))) 
		{
			setNonOriginatingMaterialCost();
		}
		
		return aSourceIVAProductSourceContainer;
	}

	public void setNonOriginatingMaterialCost() 
	{
		String groupName = "STP:"+this.qualTXComp.qualTX.fta_code_group + "_NON_ORIGINATING_MATERIALS";
		List<GPMClaimDetails> gpmNonOriginatingClaimDetails = new ArrayList<>();
	
		try {
		GPMClaimDetailsSourceIVAContainer  theClaimDetailsContainer = this.claimDetailsCache.getClaimDetails(this.qualTXComp.prod_src_iva_key);
		for(GPMClaimDetails gpmClaimDetails : theClaimDetailsContainer.claimDetailList)
		{
			if(gpmClaimDetails.claimDetailsValue.get("group_name").equals(groupName))
			{
				gpmNonOriginatingClaimDetails.add(gpmClaimDetails);
				break;
			}
		}

		if(!gpmNonOriginatingClaimDetails.isEmpty())
		{
			Map<String,String> flexConfigmap = this.qualTXBusinessLogicProcessor.dataExtensionConfigRepos.getDataExtensionConfiguration(groupName).getFlexColumnMapping();
			BigDecimal theTotalNonOriginatingComp = new BigDecimal(0);
			boolean useNonOriginatingMaterialsExists = false;
			for (GPMClaimDetails aNonOriginatingMaterialRdc : gpmNonOriginatingClaimDetails)
			{
				String aBOMHeaderCurrency = this.qualTXComp.qualTX.currency_code;
				String theNonOriginatingCurrency = null;
				String columnName = flexConfigmap.get("CURRENCY");
				if(columnName != null)
				{
					Object theNonOriginatingCurrencyObj = aNonOriginatingMaterialRdc.getValue(columnName);
					if(theNonOriginatingCurrencyObj != null)
						theNonOriginatingCurrency = (String)theNonOriginatingCurrencyObj;
				}

				BigDecimal theNonOriginatingValue = null;
				columnName = flexConfigmap.get("COST");
				if(columnName != null)
				{
					Object monOrignValue = aNonOriginatingMaterialRdc.getValue(columnName);
					if(monOrignValue != null)
						theNonOriginatingValue = (BigDecimal)monOrignValue;
				}

				if (aBOMHeaderCurrency != null && !aBOMHeaderCurrency.trim().isEmpty() 
						&& theNonOriginatingValue != null && theNonOriginatingValue.doubleValue() > 0 
						&& theNonOriginatingCurrency != null && !theNonOriginatingCurrency.trim().isEmpty())
				{
					useNonOriginatingMaterialsExists = true;

					if(!theNonOriginatingCurrency.equalsIgnoreCase(aBOMHeaderCurrency))
					{		
						double exchangeRate = this.qualTXBusinessLogicProcessor.currencyExchangeRateManager.getExchangeRate(theNonOriginatingCurrency,aBOMHeaderCurrency); 
						if (exchangeRate != 0) 
							theNonOriginatingValue = theNonOriginatingValue.multiply(BigDecimal.valueOf(exchangeRate));
					}
				}
				theTotalNonOriginatingComp = theTotalNonOriginatingComp.add(theNonOriginatingValue);
			}

			if (useNonOriginatingMaterialsExists && theTotalNonOriginatingComp.doubleValue() > 0)
			{
				double cumulationValue = (BigDecimal.valueOf(this.qualTXComp.unit_cost).subtract(theTotalNonOriginatingComp)).multiply(BigDecimal.valueOf(this.qualTXComp.qty_per)).doubleValue();
				
				if(this.qualTXComp.qualTX.analysis_method == null 
						|| "".equals(this.qualTXComp.qualTX.analysis_method))
				{
					String analysisMethod =  this.qualTXBusinessLogicProcessor.qeConfigCache.getQEConfig(this.qualTXComp.org_code).getAnalysisConfig().getAnalysisMethod();
					if(TrackerCodes.AnalysisMethod.TOP_DOWN_ANALYSIS.name().equals(analysisMethod))
						this.qualTXComp.td_cumulation_value = cumulationValue;
					else if(TrackerCodes.AnalysisMethod.RAW_MATERIAL_ANALYSIS.name().equals(analysisMethod))
						this.qualTXComp.rm_cumulation_value = cumulationValue;
					else if(TrackerCodes.AnalysisMethod.INTERMEDIATE_ANALYSIS.name().equals(analysisMethod))
						this.qualTXComp.in_cumulation_value = cumulationValue;
				}
				else if(TrackerCodes.AnalysisMethod.TOP_DOWN_ANALYSIS.name().equals(this.qualTXComp.qualTX.analysis_method))
					this.qualTXComp.td_cumulation_value = cumulationValue;
				else if(TrackerCodes.AnalysisMethod.RAW_MATERIAL_ANALYSIS.name().equals(this.qualTXComp.qualTX.analysis_method))
					this.qualTXComp.rm_cumulation_value = cumulationValue;
				else if(TrackerCodes.AnalysisMethod.INTERMEDIATE_ANALYSIS.name().equals(this.qualTXComp.qualTX.analysis_method))
					this.qualTXComp.in_cumulation_value = cumulationValue;
			}
		}
		}
		catch(Exception exec)
		{
			MessageFormatter.error(logger, "pullIVAData", exec, "BOM Component with Key [{0,number,#}]: error while calulating the non-originating material cost [{1}], COI[{2}], Effective From[{3, date, dd-MMM-yyyy}], Effective To[{4, date, dd-MMM-yyyy}]", 
					this.bomComp.alt_key_comp, 
					this.qualTXComp.qualTX.fta_code, this.qualTXComp.qualTX.ctry_of_import,
					this.qualTXComp.qualTX.effective_from, this.qualTXComp.qualTX.effective_to);
		}
	}

	public void setGPMClassificationCache(GPMClassificationProductContainerCache gpmClassCache) {
		this.gpmClassCache = gpmClassCache;
		
	}

	public void setBOMUniverse(BOMUniverse bomUniverse) {
		this.bomUniverse = bomUniverse;
	}
}