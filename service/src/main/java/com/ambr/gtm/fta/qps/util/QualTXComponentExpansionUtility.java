package com.ambr.gtm.fta.qps.util;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.gtm.fta.qps.bom.BOMComponent;
import com.ambr.gtm.fta.qps.bom.BOMUniverse;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsCache;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationProductContainerCache;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAContainerCache;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTX;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXBusinessLogicProcessor;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponent;
import com.ambr.gtm.fta.qps.qualtx.engine.result.TradeLaneStatusTracker;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfigurationRepository;
import com.ambr.platform.utils.log.MessageFormatter;

public class QualTXComponentExpansionUtility
{
	static Logger	logger = LogManager.getLogger(QualTXComponentExpansionUtility.class);
	BOMUniverse 								bomUniverse;
	QualTX 										qualTX;
	GPMClaimDetailsCache						claimDetailsCache;
	GPMSourceIVAContainerCache					ivaCache;
	DataExtensionConfigurationRepository		dataExtCfgRepos;
	GPMClassificationProductContainerCache		gpmClassCache;
	ArrayList<QualTXComponent> 					topDownComponentList;
	HashMap<String, QualTXComponent>  			rawMaterialComponentList;
	HashMap<String, QualTXComponent>  			intermediateMaterialComponentList;
	boolean										evaulateConsolidatedComponents;
	QualTXBusinessLogicProcessor                qualTXBusinessLogicProcessor;
	TradeLaneStatusTracker						statusTracker;
	boolean										isRawMaterialApproach;
	boolean										isIntermediateApproach;
	ArrayList<Long>								processedSubBOMKeys;
	
	
	/**
	 * @param theBOMUniverse
	 * @param theQualTX
	 * @param theDataExtCfgRepos
	 * @param theClaimDetailsCache
	 * @param theIVACache
	 * @param theGPMClassCache
	 * @param theEvaluateConsolidatedComponentsFlag
	 * @throws Exception
	 */
	public QualTXComponentExpansionUtility(
		BOMUniverse 							theBOMUniverse, 
		QualTX 									theQualTX, 
		DataExtensionConfigurationRepository 	theDataExtCfgRepos, 
		GPMClaimDetailsCache 					theClaimDetailsCache, 
		GPMSourceIVAContainerCache 				theIVACache, 
		GPMClassificationProductContainerCache 	theGPMClassCache, 
		QualTXBusinessLogicProcessor 			qualTXBusinessLogicProcessor, 
		boolean 								theEvaluateConsolidatedComponentsFlag, 
		TradeLaneStatusTracker 					theTracker)
		throws Exception
	{
		this.bomUniverse = theBOMUniverse;
		this.qualTX = theQualTX;
		this.dataExtCfgRepos = theDataExtCfgRepos;
		this.claimDetailsCache = theClaimDetailsCache;
		this.ivaCache = theIVACache;
		this.gpmClassCache = theGPMClassCache;
		this.qualTXBusinessLogicProcessor = qualTXBusinessLogicProcessor;
		topDownComponentList = this.qualTX.getTopDownComponentList();
		this.rawMaterialComponentList = new HashMap<String, QualTXComponent>();
		this.intermediateMaterialComponentList = new HashMap<String, QualTXComponent>();
		UniqueComponent aUniqueCompKey = null;
		for (QualTXComponent aQualTXComp : this.qualTX.getRawMaterialComponentList())
		{
			aUniqueCompKey = new UniqueComponent(aQualTXComp.prod_key, aQualTXComp.prod_src_key, aQualTXComp.unit_cost);
			this.rawMaterialComponentList.put(aUniqueCompKey.getKey(), aQualTXComp);
		}
		
		for (QualTXComponent aQualTXComp : this.qualTX.getIntermediateComponentList())
		{
			aUniqueCompKey = new UniqueComponent(aQualTXComp.prod_key, aQualTXComp.prod_src_key, aQualTXComp.unit_cost);
			this.intermediateMaterialComponentList.put(aUniqueCompKey.getKey(), aQualTXComp);
		}
		this.evaulateConsolidatedComponents = theEvaluateConsolidatedComponentsFlag;
		this.statusTracker = theTracker;
		this.processedSubBOMKeys = new ArrayList<Long>();
	}
	
	
	public ConsolidatedComponentList determineIntermmediateComponentsList() throws Exception
	{
		
		UniqueComponent aUniqueCompKey = null;
		HashMap<String, QualTXComponent> uniqueComponents = new HashMap<String, QualTXComponent>();	
		this.isIntermediateApproach =  true;
		for(QualTXComponent aQualTXComp : this.topDownComponentList)
		{
			aUniqueCompKey = new UniqueComponent(aQualTXComp.prod_key, aQualTXComp.prod_src_key, aQualTXComp.unit_cost);
			if(aQualTXComp.sub_bom_key == null || aQualTXComp.sub_bom_key == 0) {
				QualTXComponent existingQualTXComp = uniqueComponents.get(aUniqueCompKey.getKey());
				if(existingQualTXComp != null)
				{
					existingQualTXComp.in_qty_per = (existingQualTXComp.in_qty_per != null ? existingQualTXComp.in_qty_per : 0) + aQualTXComp.qty_per;
					existingQualTXComp.in_cost = aQualTXComp.unit_cost *  existingQualTXComp.in_qty_per;
					existingQualTXComp.in_cumulation_value = ((aQualTXComp.cumulation_value == null ? 0 : aQualTXComp.cumulation_value)/aQualTXComp.qty_per) *  existingQualTXComp.in_qty_per;
					existingQualTXComp.in_traced_value =  ((aQualTXComp.traced_value == null ? 0 : aQualTXComp.traced_value)/aQualTXComp.qty_per) *  existingQualTXComp.in_qty_per;
					existingQualTXComp.intermediate_ind = "Y";
				}
				else
				{
					aQualTXComp.intermediate_ind = "Y";
					aQualTXComp.in_qty_per = aQualTXComp.qty_per;
					aQualTXComp.in_cost = aQualTXComp.cost;
					aQualTXComp.in_cumulation_value = aQualTXComp.cumulation_value;
					aQualTXComp.in_traced_value = aQualTXComp.traced_value;
					uniqueComponents.put(aUniqueCompKey.getKey(), aQualTXComp);
				}
			}
			else
			{
				//MAKE component from the Top-Down approach should not be disturbed.
				uniqueComponents.put(aUniqueCompKey.getKey(), aQualTXComp);
				boolean isFlattened = this.flattenSubBOM(aQualTXComp.sub_bom_key, uniqueComponents, true);
				if(!isFlattened)
				{
					aQualTXComp.intermediate_ind = "Y";
					aQualTXComp.in_qty_per = aQualTXComp.qty_per;
					aQualTXComp.in_cost = aQualTXComp.cost;
					aQualTXComp.in_cumulation_value = aQualTXComp.cumulation_value;
					aQualTXComp.in_traced_value = aQualTXComp.traced_value;
				}
			}
		}	
		
		//Update the component list with the current state of the QUAL TX Components as rendered post the expansion of components.
		this.qualTX.compList = new ArrayList<QualTXComponent>(uniqueComponents.values());
		
		if(evaulateConsolidatedComponents)
			return this.consolidateComponents(uniqueComponents);
		else
			return new ConsolidatedComponentList();
	}
	
	public ConsolidatedComponentList determineRawMaterialComponentsList() throws Exception
	{
		this.isRawMaterialApproach = true;
		UniqueComponent aUniqueCompKey = null;
		HashMap<String, QualTXComponent> uniqueComponents = new HashMap<String, QualTXComponent>();	
		
		for(QualTXComponent aQualTXComp : this.topDownComponentList)
		{
			aUniqueCompKey = new UniqueComponent(aQualTXComp.prod_key, aQualTXComp.prod_src_key, aQualTXComp.unit_cost);
			if(aQualTXComp.sub_bom_key == null || aQualTXComp.sub_bom_key == 0) {
				QualTXComponent existingQualTXComp = uniqueComponents.get(aUniqueCompKey.getKey());
				if(existingQualTXComp != null)
				{
					existingQualTXComp.rm_qty_per = (existingQualTXComp.rm_qty_per != null ? existingQualTXComp.rm_qty_per : 0) + aQualTXComp.qty_per;
					existingQualTXComp.rm_cost = aQualTXComp.unit_cost *  existingQualTXComp.rm_qty_per;
					existingQualTXComp.rm_cumulation_value = ((aQualTXComp.cumulation_value == null ? 0 : aQualTXComp.cumulation_value)/aQualTXComp.qty_per) *  existingQualTXComp.rm_qty_per;
					existingQualTXComp.rm_traced_value =  ((aQualTXComp.traced_value == null ? 0 : aQualTXComp.traced_value)/aQualTXComp.qty_per) *  existingQualTXComp.rm_qty_per;
					existingQualTXComp.raw_material_ind = "Y";
				}
				else
				{
					aQualTXComp.raw_material_ind = "Y";
					aQualTXComp.rm_qty_per = aQualTXComp.qty_per;
					aQualTXComp.rm_cost = aQualTXComp.cost;
					aQualTXComp.rm_cumulation_value = aQualTXComp.cumulation_value;
					aQualTXComp.rm_traced_value = aQualTXComp.traced_value;
					uniqueComponents.put(aUniqueCompKey.getKey(), aQualTXComp);
				}
			}
			else
			{
				//MAKE component from the Top-Down approach should not be disturbed.
				uniqueComponents.put(aUniqueCompKey.getKey(), aQualTXComp);
				this.flattenSubBOM(aQualTXComp.sub_bom_key, uniqueComponents, false);
			}
		}	
		
		//Update the component list with the current state of the QUAL TX Components as rendered post the expansion of components.
		this.qualTX.compList = new ArrayList<QualTXComponent>(uniqueComponents.values());
		
		if(evaulateConsolidatedComponents)
			return this.consolidateComponents(uniqueComponents);
		else
			return new ConsolidatedComponentList();
	}
	
	
	public ConsolidatedComponentList consolidateComponents(HashMap<String, QualTXComponent> theUniqueComponents) throws Exception
	{
		ConsolidatedComponentList aConsolidatedComponentList = new ConsolidatedComponentList();
		
		if(theUniqueComponents.isEmpty())
			return aConsolidatedComponentList;
		
		UniqueComponent aUniqueCompKey = null;
		for(QualTXComponent aComponent : this.qualTX.compList)
		{
			aUniqueCompKey = new UniqueComponent(aComponent.prod_key, aComponent.prod_src_key, aComponent.unit_cost);
			QualTXComponent aQualTXComp = theUniqueComponents.get(aUniqueCompKey.getKey());
			if(aQualTXComp != null)
			{
				aConsolidatedComponentList.existingComponents.add(aQualTXComp);
			} 
			else
			{
				aConsolidatedComponentList.obsoleteComponents.add(aQualTXComp);
			}
		}
		
		ArrayList<QualTXComponent> newCompList =  new ArrayList<QualTXComponent>(theUniqueComponents.values());
		newCompList.removeAll(this.qualTX.compList);
		
		aConsolidatedComponentList.newComponents = newCompList;
		
		return aConsolidatedComponentList;
	}


	private boolean flattenSubBOM(Long theSubBOMKey, HashMap<String, QualTXComponent> theUniqueComponents, boolean checkForRVCFlag) throws Exception
	{
		UniqueComponent aUniqueCompKey = null;
		String qualifiedFlag = null;
		boolean passedViaRVCSatisfied = false;
		
		if(this.processedSubBOMKeys.contains(theSubBOMKey))
		{
			MessageFormatter.debug(logger, "flattenSubBOM", "Sub BOM with Key [{0,number,#}]: is referred multiple times.", theSubBOMKey);
			return true;
		}
			
		this.processedSubBOMKeys.add(theSubBOMKey);
		BOM aSubBOM = this.bomUniverse.getBOM(theSubBOMKey);
		
		if(this.isIntermediateApproach && checkForRVCFlag)
		{
			qualifiedFlag = aSubBOM.getBOMQualifiedFlag(this.qualTX.fta_code, this.qualTX.iva_code, this.qualTX.ctry_of_import, this.qualTX.effective_from, this.qualTX.effective_to);
			if("QUALIFIED".equalsIgnoreCase(qualifiedFlag))
			{
				//Check for all the Intermediate BOM's that are QUALIFIED and passed via RVC
				passedViaRVCSatisfied = this.checkBOMsPassedViaRVC(aSubBOM, this.qualTX.fta_code, this.qualTX.iva_code, this.qualTX.ctry_of_import, this.qualTX.effective_from, this.qualTX.effective_to, aSubBOM.passedViaRVC ? 1 : 0);
				//If there are more than one such Intermediate BOM flatten the current BOM all the way to leaf node, otherwise the Sub-BOM is considered as an component itself for qualification. 
				if(!passedViaRVCSatisfied)
					return false;
			}
		}
		if (aSubBOM != null && aSubBOM.compList != null)
		{
			for (BOMComponent aSubBOMComp : aSubBOM.compList)
			{
				if (ComponentType.DEFUALT.EXCLUDE_QUALIFICATION.name().equalsIgnoreCase(aSubBOMComp.component_type) || ComponentType.DEFUALT.PACKING.name().equalsIgnoreCase(aSubBOMComp.component_type)) continue;

				if (aSubBOMComp.sub_bom_key == null || aSubBOMComp.sub_bom_key == 0)
				{
					aUniqueCompKey = new UniqueComponent(aSubBOMComp.prod_key, aSubBOMComp.prod_src_key, aSubBOMComp.unit_cost);
					QualTXComponent existingQualTXComp = theUniqueComponents.get(aUniqueCompKey.getKey());
					if (existingQualTXComp != null)
					{
						if (this.isRawMaterialApproach)
						{
							existingQualTXComp.rm_qty_per = (existingQualTXComp.rm_qty_per != null ? existingQualTXComp.rm_qty_per : 0) + aSubBOMComp.qty_per;
							existingQualTXComp.rm_cost = aSubBOMComp.unit_cost * existingQualTXComp.rm_qty_per;
							existingQualTXComp.rm_cumulation_value = ((existingQualTXComp.cumulation_value == null ? 0 : existingQualTXComp.cumulation_value) / existingQualTXComp.qty_per) * existingQualTXComp.rm_qty_per;
							existingQualTXComp.rm_traced_value = ((existingQualTXComp.traced_value == null ? 0 : existingQualTXComp.traced_value) / existingQualTXComp.qty_per) * existingQualTXComp.rm_qty_per;
							existingQualTXComp.raw_material_ind = "Y";
						}
						else if (isIntermediateApproach)
						{
							existingQualTXComp.in_qty_per = (existingQualTXComp.in_qty_per != null ? existingQualTXComp.in_qty_per : 0) + aSubBOMComp.qty_per;
							existingQualTXComp.in_cost = aSubBOMComp.unit_cost * existingQualTXComp.in_qty_per;
							existingQualTXComp.in_cumulation_value = ((existingQualTXComp.cumulation_value == null ? 0 : existingQualTXComp.cumulation_value) / existingQualTXComp.qty_per) * existingQualTXComp.in_qty_per;
							existingQualTXComp.in_traced_value = ((existingQualTXComp.traced_value == null ? 0 : existingQualTXComp.traced_value) / existingQualTXComp.qty_per) * existingQualTXComp.in_qty_per;
							existingQualTXComp.intermediate_ind = "Y";
						}
					}
					else
					{
						// The component may exist from a previous raw-material
						// qualification, so can avoid pulling the component
						// data again but just update the qty_per and unit_cost
						if (this.isRawMaterialApproach)
						{
							QualTXComponent existingRawMaterialQualTXComp = this.rawMaterialComponentList.get(aUniqueCompKey.getKey());
							if (existingRawMaterialQualTXComp != null)
							{
								existingRawMaterialQualTXComp.rm_qty_per = aSubBOMComp.qty_per;
								existingRawMaterialQualTXComp.rm_cost = existingRawMaterialQualTXComp.unit_cost * existingRawMaterialQualTXComp.rm_qty_per;
								theUniqueComponents.put(aUniqueCompKey.getKey(), existingRawMaterialQualTXComp);
								continue;
							}
						}
						else if (isIntermediateApproach)
						{
							QualTXComponent existingIntermediateQualTXComp = this.intermediateMaterialComponentList.get(aUniqueCompKey.getKey());
							if (existingIntermediateQualTXComp != null)
							{
								existingIntermediateQualTXComp.in_qty_per = aSubBOMComp.qty_per;
								existingIntermediateQualTXComp.in_cost = existingIntermediateQualTXComp.unit_cost * existingIntermediateQualTXComp.in_qty_per;
								theUniqueComponents.put(aUniqueCompKey.getKey(), existingIntermediateQualTXComp);
								continue;
							}
						}
						// Create the component if it already does not exist and
						// pull Basic Info, Ctry Cmpl & IVA data.
						if(!subBOMCompAlreadyInQualtxComp(aSubBOMComp)){
							QualTXComponent aNewQualTXComp = this.qualTX.createComponent();
							QualTXComponentUtility aQualTXComponentUtility = new QualTXComponentUtility(aNewQualTXComp, aSubBOMComp, this.claimDetailsCache, this.ivaCache, this.gpmClassCache, this.dataExtCfgRepos, this.statusTracker);
							aQualTXComponentUtility.setQualTXBusinessLogicProcessor(qualTXBusinessLogicProcessor);
							aQualTXComponentUtility.setBOMUniverse(this.bomUniverse);
							aQualTXComponentUtility.pullComponentData();
							if (this.isRawMaterialApproach)
							{
								aNewQualTXComp.raw_material_ind = "Y";
								aNewQualTXComp.rm_qty_per = aNewQualTXComp.qty_per;
								aNewQualTXComp.rm_cost = aNewQualTXComp.unit_cost * aNewQualTXComp.rm_qty_per;
								aNewQualTXComp.rm_cumulation_value = aNewQualTXComp.cumulation_value;
								aNewQualTXComp.rm_traced_value = aNewQualTXComp.traced_value;
	
							}
	
							if (this.isIntermediateApproach)
							{
								aNewQualTXComp.intermediate_ind = "Y";
								aNewQualTXComp.in_qty_per = aNewQualTXComp.qty_per;
								aNewQualTXComp.in_cost = aNewQualTXComp.unit_cost * aNewQualTXComp.in_qty_per;
								aNewQualTXComp.in_cumulation_value = aNewQualTXComp.cumulation_value;
								aNewQualTXComp.in_traced_value = aNewQualTXComp.traced_value;
							}
							aNewQualTXComp.src_id = aSubBOM.bom_id + "~" + MessageFormat.format("{0,number,#}", aSubBOMComp.comp_num);
							theUniqueComponents.put(aUniqueCompKey.getKey(), aNewQualTXComp);
					    }
					}

				}
				else
				{
					this.flattenSubBOM(aSubBOMComp.sub_bom_key, theUniqueComponents, !passedViaRVCSatisfied);
				}
			}
		}
		else
		{
			MessageFormatter.debug(logger, "flattenSubBOM", "Sub BOM with Key [{0,number,#}]: has zero componenets.", theSubBOMKey);
		}
		return true;
	}

	private boolean subBOMCompAlreadyInQualtxComp(BOMComponent aSubBOMComp)
	{
		if(this.qualTX.compList == null || this.qualTX.compList.isEmpty()) return false;
	
		for(QualTXComponent qualTXComponent : this.qualTX.compList)
		{
			if(qualTXComponent.prod_key.equals(aSubBOMComp.prod_key) && qualTXComponent.prod_src_key.equals(aSubBOMComp.prod_src_key) && qualTXComponent.unit_cost.equals(aSubBOMComp.unit_cost))
			{
				if(this.isRawMaterialApproach)
				{	
					qualTXComponent.rm_qty_per = (qualTXComponent.rm_qty_per != null ? qualTXComponent.rm_qty_per : 0) + aSubBOMComp.qty_per;
					qualTXComponent.rm_cost = aSubBOMComp.unit_cost * qualTXComponent.rm_qty_per;
					qualTXComponent.rm_cumulation_value = ((qualTXComponent.cumulation_value == null ? 0 : qualTXComponent.cumulation_value) / qualTXComponent.qty_per) * qualTXComponent.rm_qty_per;
					qualTXComponent.rm_traced_value = ((qualTXComponent.traced_value == null ? 0 : qualTXComponent.traced_value) / qualTXComponent.qty_per) * qualTXComponent.rm_qty_per;
					qualTXComponent.raw_material_ind = "Y";
				}
				if(this.isIntermediateApproach)
				{
					qualTXComponent.in_qty_per = (qualTXComponent.in_qty_per != null ? qualTXComponent.in_qty_per : 0) + aSubBOMComp.qty_per;
					qualTXComponent.in_cost = aSubBOMComp.unit_cost * qualTXComponent.in_qty_per;
					qualTXComponent.in_cumulation_value = ((qualTXComponent.cumulation_value == null ? 0 : qualTXComponent.cumulation_value) / qualTXComponent.qty_per) * qualTXComponent.in_qty_per;
					qualTXComponent.in_traced_value = ((qualTXComponent.traced_value == null ? 0 : qualTXComponent.traced_value) / qualTXComponent.qty_per) * qualTXComponent.in_qty_per;
					qualTXComponent.intermediate_ind = "Y";
				}
				return true;
			}
		}
	
		return false;
	}

	class UniqueComponent
	{
		String prodKey;
		String prodSrcKey;
		String unitCost;
		
		
		public UniqueComponent(Long theProdKey, Long theProdSrcKey, Double theUnitCost) throws Exception
		{
			this.prodKey =  theProdKey + "";
			this.prodSrcKey = (theProdSrcKey == null || theProdSrcKey < 0) ? "NO_SOURCE" :  theProdSrcKey + "";
			this.unitCost = theUnitCost == null ? "NO_COST" :  theUnitCost + "";
		}
		
		
		public String getKey() throws Exception
		{
			return this.prodKey + "~" + this.prodSrcKey + "~" + this.unitCost;
		}
	}
	
	
	class ConsolidatedComponentList
	{
		ArrayList<QualTXComponent> newComponents;
		ArrayList<QualTXComponent> existingComponents;
		ArrayList<QualTXComponent> obsoleteComponents;
		
		
		public ConsolidatedComponentList() throws Exception
		{
			this.existingComponents = new ArrayList<QualTXComponent>();
			this.newComponents = new ArrayList<QualTXComponent>();
			this.obsoleteComponents =  new ArrayList<QualTXComponent>();
		}
		
		
		public ArrayList<QualTXComponent> getNewComponents() throws Exception
		{
			return this.newComponents;
		}
		
		public ArrayList<QualTXComponent> getExistingComponents() throws Exception
		{
			return this.existingComponents;
		}
		
		public ArrayList<QualTXComponent> getObsoleteComponents() throws Exception
		{
			return this.obsoleteComponents;
		}
	}
	
	public boolean checkBOMsPassedViaRVC(BOM theBOM, String theFTACode, String theIVACode, String theCOI, Date theEffectiveFrom, Date theEffectiveTo, int passedViaRVCCount) throws Exception
	{
		for(BOMComponent aComponent : theBOM.compList)
		{
			if(aComponent.isSubAssembly())
			{
				BOM aSubBOM = this.bomUniverse.getBOM(aComponent.sub_bom_key);
				if(aSubBOM.isIntermediateBOM() && 
						"QUALIFIED".equalsIgnoreCase(aSubBOM.getBOMQualifiedFlag(this.qualTX.fta_code, this.qualTX.iva_code, this.qualTX.ctry_of_import, this.qualTX.effective_from, this.qualTX.effective_to)))
				{
					if(aSubBOM.passedViaRVC)
						passedViaRVCCount ++;
					
					if(passedViaRVCCount > 1)
						return true;
				}
				
				this.checkBOMsPassedViaRVC(theBOM, theFTACode, theIVACode, theCOI, theEffectiveFrom, theEffectiveTo, passedViaRVCCount);
			}
		}
		if(passedViaRVCCount > 1)
			return true;
		return false;
	}
}