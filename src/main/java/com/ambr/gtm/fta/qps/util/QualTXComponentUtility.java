package com.ambr.gtm.fta.qps.util;

import java.text.MessageFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.qps.bom.BOMComponent;
import com.ambr.gtm.fta.qps.bom.BOMComponentDataExtension;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsCache;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationProductContainer;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationProductContainerCache;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVA;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAContainerCache;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAProductSourceContainer;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXBusinessLogicProcessor;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponent;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponentDataExtension;
import com.ambr.gtm.fta.qps.qualtx.engine.result.TradeLaneStatusTracker;
import com.ambr.gtm.fta.qts.util.CumulationConfigContainer;
import com.ambr.gtm.fta.qts.util.CumulationConfigContainer.ComponentAgreement;
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
		
	public QualTXBusinessLogicProcessor getQualTXBusinessLogicProcessor()
	{
		return qualTXBusinessLogicProcessor;
	}

	public void setQualTXBusinessLogicProcessor(QualTXBusinessLogicProcessor qualTXBusinessLogicProcessor)
	{
		this.qualTXBusinessLogicProcessor = qualTXBusinessLogicProcessor;
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
		
		for (BOMComponentDataExtension aBOMCompDE : this.bomComp.deList) {
			
			if (aBOMCompDE.group_name.equalsIgnoreCase(aPriceDE)) {
				
				Number price = (Number)aBOMCompDE.getValue("PRICE");
				String priceType = (String)aBOMCompDE.getValue("PRICE_TYPE");
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
		this.qualTXComp.qualified_from = this.bomComp.effective_from;
		this.qualTXComp.qualified_to = this.bomComp.effective_to;
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
		return aSourceIVAProductSourceContainer;
	}
}
