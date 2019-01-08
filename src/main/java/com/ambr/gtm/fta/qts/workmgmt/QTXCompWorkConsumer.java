package com.ambr.gtm.fta.qts.workmgmt;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.ambr.gtm.fta.qps.util.ComponentType;

import com.ambr.gtm.fta.qps.bom.BOMComponent;
import com.ambr.gtm.fta.qps.bom.BOMComponentDataExtension;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetails;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsCache;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsSourceIVAContainer;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassification;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationProductContainerCache;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVA;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAContainerCache;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAProductContainer;
import com.ambr.gtm.fta.qps.gpmsrciva.STPDecisionEnum;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTX;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXBusinessLogicProcessor;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponent;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponentDataExtension;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponentPrice;
import com.ambr.gtm.fta.qps.util.QualTXComponentUtility;
import com.ambr.gtm.fta.qps.util.QualTXUtility;
import com.ambr.gtm.fta.qts.QTXCompWork;
import com.ambr.gtm.fta.qts.QTXCompWorkHS;
import com.ambr.gtm.fta.qts.QTXCompWorkIVA;
import com.ambr.gtm.fta.qts.QTXWork;
import com.ambr.gtm.fta.qts.RequalificationWorkCodes;
import com.ambr.gtm.fta.qts.TrackerCodes;
import com.ambr.gtm.fta.qts.config.QEConfigCache;
import com.ambr.gtm.fta.qts.util.SubPullConfigContainer;
import com.ambr.gtm.fta.qts.util.SubPullConfigData;
import com.ambr.gtm.fta.qts.util.TradeLane;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfiguration;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfigurationRepository;
import com.ambr.gtm.utils.legacy.rdbms.de.GroupNameSpecification;
import com.ambr.gtm.utils.legacy.sps.SimplePropertySheet;


/*
 * NOTE!!!!!
 * Any changes to reason codes or logic within the handling of a reason code
 * requires a review of QTXWorkStaging as it will change the requirements as to when BOM/Prod/IVA resource data
 * is collected from cache
 */
public class QTXCompWorkConsumer extends QTXConsumer<CompWorkPackage>
{
	private static Logger logger = LogManager.getLogger(QTXCompWorkConsumer.class);
	
	private DataExtensionConfigurationRepository repos;
	private QualTXBusinessLogicProcessor qtxBusinessLogicProcessor;
	public QualTXBusinessLogicProcessor getQtxBusinessLogicProcessor()
	{
		return qtxBusinessLogicProcessor;
	}

	public void setQtxBusinessLogicProcessor(QualTXBusinessLogicProcessor qtxBusinessLogicProcessor)
	{
		this.qtxBusinessLogicProcessor = qtxBusinessLogicProcessor;
	}

	public QTXCompWorkConsumer(ArrayList<CompWorkPackage> workList)
	{
		super(workList);
	}

	public QTXCompWorkConsumer(CompWorkPackage work)
	{
		super(work);
	}
	
	public void setDataExtensionRepository(DataExtensionConfigurationRepository repos)
	{
		this.repos = repos;
	}
	
	//TODO what if bomcomp is removed then added, merged into one work item.  based on logic below the add will execute then the remove.  consolidation logic might have to detect this and skip the add.
	private void doWork(CompWorkPackage compWorkPackage) throws Exception
	{
		//An error occurred while staging the data for this WorkPackage (usually resource data failed to pull)
		if (compWorkPackage.failure != null)
			throw compWorkPackage.failure;
		WorkPackage parentWorkPackage = compWorkPackage.getParentWorkPackage();
		QTXWork parentWork = parentWorkPackage.work;
		QTXCompWork work = compWorkPackage.compWork;
		QualTX qualtx = parentWorkPackage.qualtx;
		QualTXComponent qualtxComp = compWorkPackage.qualtxComp;
		BOMComponent bomComp = (parentWorkPackage.bom != null) ? parentWorkPackage.bom.getBOMComponentByAltKey(work.bom_comp_key) : null;
		boolean isConfigChange = false;
		if (((QTXCompWorkProducer)(this.producer)).queueUniverse == null) {
			throw new IllegalStateException("Queue Universe has not been initialized");
		}
		
		GPMClaimDetailsCache aClaimsDetailCache = ((QTXCompWorkProducer)(this.producer)).queueUniverse.gpmClaimDetailsCache;
		GPMSourceIVAContainerCache aGPMSourceIVAContainerCache = ((QTXCompWorkProducer)(this.producer)).queueUniverse.ivaCache;
		
		DataExtensionConfigurationRepository aDataExtensionConfigurationRepository = ((QTXCompWorkProducer)(this.producer)).queueUniverse.dataExtCfgRepos;
		QEConfigCache qeConfigCache = ((QTXCompWorkProducer)(this.producer)).queueUniverse.qeConfigCache;
		GPMClassificationProductContainerCache gpmClassCache = ((QTXCompWorkProducer)(this.producer)).queueUniverse.gpmClassCache;
		QualTXComponentUtility aQualTXComponentUtilityforComp = null;

		if(work.isReasonCodeFlagSet(RequalificationWorkCodes.COMP_CONFIG_CHANGE) == true)
		{
			if (bomComp == null) throw new Exception("BOMComponent (" + work.bom_comp_key + ") not found on BOM(" + parentWorkPackage.bom.alt_key_bom + ")");

			if (qualtxComp == null)
			{
				qualtxComp = qualtx.createComponent();

			}
			aQualTXComponentUtilityforComp = new QualTXComponentUtility(qualtxComp, bomComp, aClaimsDetailCache, aGPMSourceIVAContainerCache, gpmClassCache, aDataExtensionConfigurationRepository, null);
			aQualTXComponentUtilityforComp.setQualTXBusinessLogicProcessor(this.qtxBusinessLogicProcessor);

			aQualTXComponentUtilityforComp.pullIVAData();
			aQualTXComponentUtilityforComp.pullCtryCmplData();
			isConfigChange = true;

		}
		
		if (work.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_COMP_ADDED) == true)
		{
			if (bomComp == null) throw new Exception("BOMComponent (" + work.bom_comp_key + ") not found on BOM(" + parentWorkPackage.bom.alt_key_bom + ")");

			if (qualtxComp == null) {
			
			if (bomComp.component_type != null && !bomComp.component_type.isEmpty() && !ComponentType.DEFUALT.EXCLUDE_QUALIFICATION.name().equalsIgnoreCase(bomComp.component_type) && !ComponentType.DEFUALT.PACKING.name().equalsIgnoreCase(bomComp.component_type))
			{
				
					qualtxComp = qualtx.createComponent();

					// TODO bom_id is not a column on qualtx_comp, may be we need to
					// set to src_id but it looks like src_id is populting with
					// numbers as of now.

					// TODO review with claude/pavan - can this section be removed
					// and replaced with routines from prep service?

//					 qualtxComp.comp_id = (bomComp.comp_num != null) ?
//					 bomComp.comp_num.toString() : null;
//					 qualtxComp.cost = bomComp.extended_cost;
//					 qualtxComp.src_key = bomComp.alt_key_comp;
//					 qualtxComp.weight_uom = bomComp.weight_uom;
//					 qualtxComp.component_type = bomComp.component_type;
//					 qualtxComp.area = bomComp.area;
//					 qualtxComp.area_uom = bomComp.area_uom;
//					 qualtxComp.prod_key = bomComp.prod_key;
//					 qualtxComp.prod_src_key = bomComp.prod_src_key;
//					 qualtxComp.supplier_key = bomComp.supplier_key;
//					 qualtxComp.manufacturer_key = bomComp.manufacturer_key;
//					 qualtxComp.seller_key = bomComp.seller_key;
//					 qualtxComp.net_weight = bomComp.net_weight;
//					 qualtxComp.unit_weight = bomComp.unit_weight;
//					 qualtxComp.qty_per = bomComp.qty_per;
//					 qualtxComp.unit_cost = bomComp.unit_cost;
					 aQualTXComponentUtilityforComp = new QualTXComponentUtility(qualtxComp, bomComp, aClaimsDetailCache, aGPMSourceIVAContainerCache, gpmClassCache, aDataExtensionConfigurationRepository, null);
					 aQualTXComponentUtilityforComp.setQualTXBusinessLogicProcessor(this.qtxBusinessLogicProcessor);
					 aQualTXComponentUtilityforComp.pullComponentData();

				
				// if the current analysis method is Top-Down mark the
				// RM_CONSTRUCTION_STATUS as INIT, if a component is added. The
				// Preparation Engine will need to re-construct the Qual TX
				// Components.
				parentWorkPackage.qualtx.compList.add(qualtxComp);
				if (parentWork.details.analysis_method == TrackerCodes.AnalysisMethod.TOP_DOWN_ANALYSIS)
				{
					qualtxComp.top_down_ind = "Y";
					qualtxComp.qualTX.rm_construction_status = TrackerCodes.QualTXContructionStatus.INIT.ordinal();
					qualtxComp.qualTX.in_construction_status = TrackerCodes.QualTXContructionStatus.INIT.ordinal();
				}

				compWorkPackage.qualtxComp = qualtxComp;
				/*
				 * EntityManager<QualTXComponent> entityMgr = new
				 * EntityManager<QualTXComponent>(QualTXComponent.class,
				 * this.txMgr, this.schemaDesc, template);
				 * entityMgr.setExistingEntity(qualtxComp);
				 * compWorkPackage.entityMgr = entityMgr;
				 */
			}}
		}
		boolean isCompDeleted = false;
		if (work.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_COMP_DELETED) == true || isConfigChange)
		{
			if (bomComp == null) {
			if (qualtxComp == null) throw new Exception("Qualtx component " + work.qualtx_comp_key + " not found on qualtx " + parentWork.details.qualtx_key);
			qualtx.removeComponent(qualtxComp);
			qualtxComp.qualTX = qualtx;
			//if the current analysis method is Top-Down mark the RM_CONSTRUCTION_STATUS as INIT, if a component is deleted. The Preparation Engine will need to re-construct the Qual TX Components.
			if(parentWork.details.analysis_method == TrackerCodes.AnalysisMethod.TOP_DOWN_ANALYSIS)
			{
				qualtxComp.qualTX.rm_construction_status = TrackerCodes.QualTXContructionStatus.INIT.ordinal();
				qualtxComp.qualTX.in_construction_status = TrackerCodes.QualTXContructionStatus.INIT.ordinal();
			}
			isCompDeleted = true;
			}
			
		}
		
		if (work.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_COMP_MODIFIED) == true || (isConfigChange && !isCompDeleted))
		{
			if (qualtxComp == null) throw new Exception("Qualtx component " + work.qualtx_comp_key + " not found on qualtx " + parentWork.details.qualtx_key);
			if (bomComp == null) throw new Exception("BOMComponent (" + work.bom_comp_key + ") not found on BOM(" + work.bom_key + ")");

			if (bomComp.component_type != null && !bomComp.component_type.isEmpty() && (ComponentType.DEFUALT.EXCLUDE_QUALIFICATION.name().equalsIgnoreCase(bomComp.component_type) || ComponentType.DEFUALT.PACKING.name().equalsIgnoreCase(bomComp.component_type)))
			{
				qualtx.removeComponent(qualtxComp);
				qualtxComp.qualTX = qualtx;
			}
			else
			{

				qualtxComp.cost = bomComp.extended_cost;
				qualtxComp.src_key = bomComp.alt_key_comp;
				qualtxComp.weight_uom = bomComp.weight_uom;
				qualtxComp.component_type = bomComp.component_type;
				qualtxComp.area = bomComp.area;
				qualtxComp.area_uom = bomComp.area_uom;
				qualtxComp.prod_key = bomComp.prod_key;
				qualtxComp.prod_src_key = bomComp.prod_src_key;
				qualtxComp.supplier_key = bomComp.supplier_key;
				qualtxComp.manufacturer_key = bomComp.manufacturer_key;
				qualtxComp.seller_key = bomComp.seller_key;
				qualtxComp.net_weight = bomComp.net_weight;
				qualtxComp.unit_weight = bomComp.unit_weight;
				qualtxComp.qty_per = bomComp.qty_per;
				if(null != bomComp.unit_cost)
					qualtxComp.unit_cost = bomComp.unit_cost;
				qualtxComp.qualTX = qualtx;
				// //if the current analysis method is Top-Down mark the
				// RM_CONSTRUCTION_STATUS as INIT, if a component
				// unit_cost/qty_per is updated. The Preparation Engine will
				// need to re-construct the Qual TX Components.
				// if(parentWork.details.analysis_method ==
				// TrackerCodes.AnalysisMethod.TOP_DOWN_ANALYSIS
				// && (qualtxComp.sub_bom_key != null && qualtxComp.sub_bom_key
				// != 0)
				// && (!BOMQualAuditEntity.equal(bomComp.unit_cost,
				// qualtxComp.unit_cost))
				// || !BOMQualAuditEntity.equal(bomComp.qty_per,
				// qualtxComp.qty_per))
				// qualtxComp.qualTX.rm_construction_status =
				// TrackerCodes.QualTXContructionStatus.INIT.ordinal();

			}

		}
		
		if (work.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_COMP_YARN_DTLS_CHG) == true)
		{
			if (qualtxComp == null) throw new Exception("Qualtx component " + work.qualtx_comp_key + " not found on qualtx " + parentWork.details.qualtx_key);
			if (bomComp == null) throw new Exception("BOMComponent (" + work.bom_comp_key + ") not found on BOM(" + parentWorkPackage.bom.alt_key_bom + ")");
			qualtxComp.qualTX = qualtx;
			parentWorkPackage.entityMgr.getLoader().setIgnoreParentRecordMissingException(true);
			parentWorkPackage.entityMgr.loadTable("MDI_QUALTX_COMP_DE");
			ArrayList<BOMComponentDataExtension> bomCompYarnDetailsList = bomComp.getDataExtensionByGroupName("IMPL_BOM_PROD_FAMILY:TEXTILES");

			Set<QualTXComponentDataExtension> qualtxYarnDtlsCompDEs = new HashSet<>(); 
			for (QualTXComponentDataExtension qualtxYarnDtlsCompDE : qualtxComp.deList)
			{
				if(qualtxYarnDtlsCompDE.group_name.equalsIgnoreCase("IMPL_BOM_PROD_FAMILY:TEXTILES")) qualtxYarnDtlsCompDEs.add(qualtxYarnDtlsCompDE);
			}
			
			Set<QualTXComponentDataExtension> deleteQualtxYarnDetailsDE = new HashSet<QualTXComponentDataExtension>(); 
			deleteQualtxYarnDetailsDE.addAll(qualtxYarnDtlsCompDEs);
			
			for (BOMComponentDataExtension bomCompYarnDetail : bomCompYarnDetailsList)
			{
				if (bomCompYarnDetail.getValue("FLEXFIELD_VAR1") == null)
					continue;
				
				boolean isYarnTypeExist = false;
				for (QualTXComponentDataExtension qualtxYarnDtlsCompDE : qualtxYarnDtlsCompDEs)
				{
					if (null != qualtxYarnDtlsCompDE.getValue("FLEXFIELD_VAR1") && qualtxYarnDtlsCompDE.getValue("FLEXFIELD_VAR1").equals(bomCompYarnDetail.getValue("FLEXFIELD_VAR1")))
					{
						isYarnTypeExist = true;
						deleteQualtxYarnDetailsDE.remove(qualtxYarnDtlsCompDE);
						qualtxYarnDtlsCompDE.setValue("FLEXFIELD_VAR2", bomCompYarnDetail.getValue("FLEXFIELD_VAR2"));
						qualtxYarnDtlsCompDE.setValue("FLEXFIELD_VAR3", bomCompYarnDetail.getValue("FLEXFIELD_VAR3"));
						qualtxYarnDtlsCompDE.setValue("FLEXFIELD_VAR4", bomCompYarnDetail.getValue("FLEXFIELD_VAR4"));
						qualtxYarnDtlsCompDE.setValue("FLEXFIELD_VAR5", bomCompYarnDetail.getValue("FLEXFIELD_VAR5"));
						qualtxYarnDtlsCompDE.setValue("FLEXFIELD_VAR6", bomCompYarnDetail.getValue("FLEXFIELD_VAR6"));	
						qualtxYarnDtlsCompDE.setValue("FLEXFIELD_VAR7", bomCompYarnDetail.getValue("FLEXFIELD_VAR7"));	
						qualtxYarnDtlsCompDE.setValue("FLEXFIELD_NUM1", bomCompYarnDetail.getValue("FLEXFIELD_NUM1"));	
					}
				}
				if(!isYarnTypeExist)
				{
					QualTXComponentDataExtension de = qualtxComp.createDataExtension("IMPL_BOM_PROD_FAMILY:TEXTILES", this.repos, null);
					
					Timestamp now = new Timestamp(System.currentTimeMillis());
					de.setValue("CREATED_BY", parentWork.userId); 
					de.setValue("CREATED_DATE", now);
					de.setValue("LAST_MODIFIED_BY", parentWork.userId);
					de.setValue("LAST_MODIFIED_DATE", now);

					de.setValue("FLEXFIELD_VAR1", bomCompYarnDetail.getValue("FLEXFIELD_VAR1"));	//type
					de.setValue("FLEXFIELD_VAR2", bomCompYarnDetail.getValue("FLEXFIELD_VAR2"));	//originating_status
					de.setValue("FLEXFIELD_VAR3", bomCompYarnDetail.getValue("FLEXFIELD_VAR3"));	//ctry_of_origin
					de.setValue("FLEXFIELD_VAR4", bomCompYarnDetail.getValue("FLEXFIELD_VAR4"));	//ctry_of_manufacture
					de.setValue("FLEXFIELD_VAR5", bomCompYarnDetail.getValue("FLEXFIELD_VAR5"));	//weight_type
					de.setValue("FLEXFIELD_VAR6", bomCompYarnDetail.getValue("FLEXFIELD_VAR6"));	//knit_to_shape
					de.setValue("FLEXFIELD_VAR7", bomCompYarnDetail.getValue("FLEXFIELD_VAR7"));	//uom
					de.setValue("FLEXFIELD_NUM1", bomCompYarnDetail.getValue("FLEXFIELD_NUM1"));	//weight

				} 
			}
			qualtxComp.deList.removeAll(deleteQualtxYarnDetailsDE);
		}

		//TODO load existing price records add/remove/update price records as needed
		if (work.isReasonCodeFlagSet(RequalificationWorkCodes.COMP_PRC_CHG) == true)
		{
			if (qualtxComp == null) throw new Exception("Qualtx component " + work.qualtx_comp_key + " not found on qualtx " + parentWork.details.qualtx_key);
			if (bomComp == null) throw new Exception("BOMComponent (" + work.bom_comp_key + ") not found on BOM(" + parentWorkPackage.bom.alt_key_bom + ")");
			qualtxComp.qualTX = qualtx;
			ArrayList<BOMComponentDataExtension> priceDetailsList = bomComp.getDataExtensionByGroupName("BOM_COMP_STATIC:PRICE");
			parentWorkPackage.entityMgr.getLoader().setIgnoreParentRecordMissingException(true);
			parentWorkPackage.entityMgr.loadTable("MDI_QUALTX_COMP_PRICE");
			Set<QualTXComponentPrice> deleteCompPriceList = new HashSet<QualTXComponentPrice>(); 
			deleteCompPriceList.addAll(qualtxComp.priceList);
			
			for (BOMComponentDataExtension bomCompPrice : priceDetailsList)
			{
				if(bomCompPrice.getValue("flexfield_var1") == null) continue;	//PRICE_TYPE
				
				boolean isPriceTypeExist = false;
				for (QualTXComponentPrice qualtxCompPrice : qualtxComp.priceList)
				{
					if (bomCompPrice.getValue("flexfield_var1").equals(qualtxCompPrice.price_type))
					{
						isPriceTypeExist = true;
						deleteCompPriceList.remove(qualtxCompPrice);
						if (bomCompPrice.getValue("flexfield_num1") != qualtxCompPrice.price)
						{
							qualtxCompPrice.price =  ((Integer)bomCompPrice.getValue("flexfield_num1")).doubleValue();
						}
						if (!bomCompPrice.getValue("flexfield_var2").equals(qualtxCompPrice.currency_code))
						{
							qualtxCompPrice.currency_code = (String) bomCompPrice.getValue("flexfield_var2");
						}
					}
				}
				if(!isPriceTypeExist)
				{
					QualTXComponentPrice price = qualtxComp.createPrice((Number) bomCompPrice.getValue("flexfield_num1"), (String) bomCompPrice.getValue("flexfield_var1"));
					
					price.currency_code = (String) bomCompPrice.getValue("flexfield_var2");	//CURRENCY_CODE
					
					price.created_by = parentWork.userId;
					price.created_date = new Timestamp(System.currentTimeMillis());
					price.last_modified_by = price.created_by;
					price.last_modified_date = price.created_date;
				}
			}
			qualtxComp.priceList.removeAll(deleteCompPriceList);
		}
		
		if (qualtxComp == null) throw new Exception("Qualtx component " + work.qualtx_comp_key + " not found on qualtx " + parentWork.details.qualtx_key);
		if (bomComp == null && !isCompDeleted) throw new Exception("BOMComponent (" + work.bom_comp_key + ") not found on BOM(" + parentWorkPackage.bom.alt_key_bom + ")");
		qualtxComp.qualTX = qualtx;
		int cooSource = qualtxComp.coo_source;
		SimplePropertySheet propertySheet = qtxBusinessLogicProcessor.propertySheetManager.getPropertySheet(qualtx.org_code, "BOM_SCREENING_CONFIG");
		List<String> propertyValue = Arrays.asList(propertySheet.getStringValueList("COO_DETERMINATION_HIERARCHY"));
		
		if (work.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_COMP_COO_CHG) && (cooSource == RequalificationWorkCodes.BOM_COMP_COO_CHG || propertyValue.indexOf(RequalificationWorkCodes.BOM_COMP_COO_CHG) <  propertyValue.indexOf(cooSource)))
		{
			qtxBusinessLogicProcessor.determineComponentCOO.determineCOOForComponentSource(qualtxComp, bomComp, aGPMSourceIVAContainerCache.getSourceIVABySource(qualtxComp.prod_key), compWorkPackage.gpmClassificationProductContainer, qtxBusinessLogicProcessor.propertySheetManager);
		}
		if (work.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_COMP_COM_COO_CHG) && (cooSource == RequalificationWorkCodes.BOM_COMP_COM_COO_CHG || propertyValue.indexOf(RequalificationWorkCodes.BOM_COMP_COM_COO_CHG) <  propertyValue.indexOf(cooSource)))
		{
			qtxBusinessLogicProcessor.determineComponentCOO.determineCOOForComponentSource(qualtxComp, bomComp, aGPMSourceIVAContainerCache.getSourceIVABySource(qualtxComp.prod_key), compWorkPackage.gpmClassificationProductContainer, qtxBusinessLogicProcessor.propertySheetManager);
		}
		
		if (work.isReasonCodeFlagSet(RequalificationWorkCodes.COMP_GPM_COO_CHG) && (cooSource == RequalificationWorkCodes.COMP_GPM_COO_CHG || propertyValue.indexOf(RequalificationWorkCodes.COMP_GPM_COO_CHG) <  propertyValue.indexOf(cooSource)))
		{
			qtxBusinessLogicProcessor.determineComponentCOO.determineCOOForComponentSource(qualtxComp, bomComp, aGPMSourceIVAContainerCache.getSourceIVABySource(qualtxComp.prod_key), compWorkPackage.gpmClassificationProductContainer, qtxBusinessLogicProcessor.propertySheetManager);
		}
		
		if (work.isReasonCodeFlagSet(RequalificationWorkCodes.COMP_STP_COO_CHG) && (cooSource == RequalificationWorkCodes.COMP_GPM_COO_CHG  || propertyValue.indexOf(RequalificationWorkCodes.COMP_STP_COO_CHG) <  propertyValue.indexOf(cooSource)))
		{
			qtxBusinessLogicProcessor.determineComponentCOO.determineCOOForComponentSource(qualtxComp, bomComp, aGPMSourceIVAContainerCache.getSourceIVABySource(qualtxComp.prod_key), compWorkPackage.gpmClassificationProductContainer, qtxBusinessLogicProcessor.propertySheetManager);
		}
		if (work.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_COMP_PREV_YEAR_QUAL_CHANGE))
		{
			this.qtxBusinessLogicProcessor.cumulationComputationRule.applyCumulationForComponent(qualtxComp, compWorkPackage.gpmSourceIVAProductContainer.getGPMSourceIVAProductSourceContainerByProdSourceKey(qualtxComp.prod_src_key), aClaimsDetailCache, this.repos);
			
			if (!"Y".equalsIgnoreCase(qualtxComp.cumulation_rule_applied) && ( qualtxComp.qualified_flg == null || "".equalsIgnoreCase(qualtxComp.qualified_flg)))
			{
				Date origEffectiveFrom = qualtxComp.qualified_from;
				Date origEffectiveTo = qualtxComp.qualified_to;
				boolean isPrevYearQualApplied = this.qtxBusinessLogicProcessor.previousYearQualificationRule.applyPrevYearQualForComponent(bomComp, qualtxComp, compWorkPackage.gpmSourceIVAProductContainer.getGPMSourceIVAProductSourceContainerByProdSourceKey(qualtxComp.prod_src_key), aClaimsDetailCache, this.repos);
				if (!isPrevYearQualApplied)
				{
					this.qtxBusinessLogicProcessor.cumulationComputationRule.applyCumulationForComponent(qualtxComp, compWorkPackage.gpmSourceIVAProductContainer.getGPMSourceIVAProductSourceContainerByProdSourceKey(qualtxComp.prod_src_key), aClaimsDetailCache, this.repos);
					qualtxComp.qualified_from = origEffectiveFrom;
					qualtxComp.qualified_to = origEffectiveTo;
				}
			}
		}
		
		
		if (work.compWorkHSList != null)
		{
			for (QTXCompWorkHS compWorkHS : work.compWorkHSList)
			{
				logger.debug("Processing ar_qtx_comp_work_hs " + compWorkHS.qtx_comp_hspull_wid);
				int compHsLength = -1;
				List<TradeLane> subPullConfig = qeConfigCache.getQEConfig(parentWork.company_code).getSubpullConfigList();
				if (subPullConfig != null)
				{
					Optional<TradeLane> tradeLane = subPullConfig.stream().filter(p -> p.getFtaCode().equalsIgnoreCase(qualtx.fta_code) && p.getCtryOfImport().equalsIgnoreCase(qualtx.ctry_of_import)).findFirst();
					if (tradeLane.isPresent())
					{
						SubPullConfigContainer container = qeConfigCache.getQEConfig(parentWork.company_code).getSubPullConfigContainer();
						SubPullConfigData subpullConfigDate = container.getTradeLaneData(tradeLane.get());
						if (subpullConfigDate != null)
						{
							String lengthStr = subpullConfigDate.getCompHsLength();
							if (lengthStr != null && !lengthStr.isEmpty()) compHsLength = Integer.parseInt(lengthStr);
						}
					}
				}
				
				if (compWorkHS.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_CTRY_CMPL_CHANGE) == true)
				{
					if (qualtxComp == null) throw new Exception("Qualtx component " + work.qualtx_comp_key + " not found on qualtx " + parentWork.details.qualtx_key);
					if (compWorkPackage.gpmClassificationProductContainer == null)
						throw new Exception("GPMClassificationProductContainer not present during comp HS pull for work " + compWorkHS.qtx_wid + ":" + compWorkHS.qtx_comp_wid + ":" + compWorkHS.qtx_comp_hspull_wid);

					if (compWorkHS.ctry_cmpl_key == null)
						throw new Exception("Error in attempting to process pull with null ctry cmpl key for comp work HS " + compWorkHS.qtx_wid + ":" + compWorkHS.qtx_comp_wid + ":" + compWorkHS.qtx_comp_hspull_wid);
					
					GPMClassification gpmClassification = compWorkPackage.gpmClassificationProductContainer.getGPMClassificationByCtryCmplKey(compWorkHS.ctry_cmpl_key);
							
					if (gpmClassification == null) 
						throw new Exception("Failed to find GPMClassification " + compWorkHS.ctry_cmpl_key + " for work HS " + compWorkHS.qtx_wid + ":" + compWorkHS.qtx_comp_wid + ":" + compWorkHS.qtx_comp_hspull_wid);
					
					qualtxComp.hs_num = ((compHsLength != -1 && gpmClassification.imHS1.length() > compHsLength) ? gpmClassification.imHS1.substring(0, compHsLength) : gpmClassification.imHS1);
					qualtxComp.sub_pull_ctry = gpmClassification.ctryCode;
					
					//qualtxComp.prod_ctry_cmpl_key be will be there already. we are just updating the HS number
				}
				
				if (compWorkHS.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_CTRY_CMPL_DELETED) == true)
				{
					if (qualtxComp == null) throw new Exception("Qualtx component " + work.qualtx_comp_key + " not found on qualtx " + parentWork.details.qualtx_key);
					qualtxComp.hs_num = null;
					qualtxComp.sub_pull_ctry = null;
					qualtxComp.prod_ctry_cmpl_key = null;
				}
				
				//TODO why isnt TA storing the HS number - avoid unecessary select
				if (compWorkHS.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_CTRY_CMPL_ADDED) == true)
				{	
					if (qualtxComp == null) throw new Exception("Qualtx component " + work.qualtx_comp_key + " not found on qualtx " + parentWork.details.qualtx_key);

					if (compWorkPackage.gpmClassificationProductContainer == null)
						throw new Exception("GPMClassificationProductContainer not present during comp HS pull for work " + compWorkPackage.compWork.qtx_wid + ":" + compWorkPackage.compWork.qtx_comp_wid);

					if (compWorkHS.ctry_cmpl_key == null)
						throw new Exception("Error in attempting to process pull with null ctry cmpl key for comp work HS " + compWorkHS.qtx_wid + ":" + compWorkHS.qtx_comp_wid + ":" + compWorkHS.qtx_comp_hspull_wid);
					
					GPMClassification gpmClassification = compWorkPackage.gpmClassificationProductContainer.getGPMClassificationByCtryCmplKey(compWorkHS.ctry_cmpl_key);
					
					if (gpmClassification == null) 
						throw new Exception("Failed to find GPMClassification " + compWorkHS.ctry_cmpl_key + " for work HS " + compWorkHS.qtx_wid + ":" + compWorkHS.qtx_comp_wid + ":" + compWorkHS.qtx_comp_hspull_wid);

					qualtxComp.hs_num = ((compHsLength != -1 && gpmClassification.imHS1.length() > compHsLength) ? gpmClassification.imHS1.substring(0, compHsLength) : gpmClassification.imHS1);
					qualtxComp.sub_pull_ctry = gpmClassification.ctryCode;
					qualtxComp.prod_ctry_cmpl_key = gpmClassification.cmplKey;
				}
			}
		}
			
		if (work.compWorkIVAList != null)
		{
			for (QTXCompWorkIVA compWorkIVA : work.compWorkIVAList)
			{
				logger.debug("Processing ar_qtx_comp_work_iva " + compWorkIVA.qtx_comp_iva_wid);

				if (compWorkIVA.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_COMP_FINAL_DECISION_CHANGE))
				{
					if (qualtxComp == null) throw new Exception("Qualtx component " + work.qualtx_comp_key + " not found on qualtx " + parentWork.details.qualtx_key);
					//if (bomComp == null) throw new Exception("BOMComponent (" + work.bom_comp_key + ") not found on BOM(" + work.bom_key + ")");
					
					//Long prodSourceKey = (qualtxComp != null) ? qualtxComp.prod_src_key : ((bomComp.prod_src_key > 0) ? new Long(bomComp.prod_src_key) : null);
					
					Long prodSourceKey =  qualtxComp.prod_src_key;
					
					if (prodSourceKey == null)
						throw new Exception("Error attempting to pull comp IVA data with invalid prod source key " + prodSourceKey + " for comp iva work " + compWorkIVA.qtx_wid + ":" + compWorkIVA.qtx_comp_wid + ":" + compWorkIVA.qtx_comp_iva_wid);
					
					if (compWorkPackage.gpmSourceIVAProductContainer == null)
						throw new Exception("GPMSourceIVAProductContainer not present during comp IVA pull for work " + compWorkPackage.compWork.qtx_wid + ":" + compWorkPackage.compWork.qtx_comp_wid);

					GPMSourceIVAProductContainer	aContainer =  aGPMSourceIVAContainerCache.getSourceIVAByProduct(qualtxComp.prod_key);
					aContainer.indexByProdSourceKey();
					GPMSourceIVA gpmSourceIVA = aContainer.getGPMSourceIVA(prodSourceKey, compWorkIVA.iva_key);
			
					//GPMSourceIVA gpmSourceIVA = compWorkPackage.gpmSourceIVAProductContainer.getGPMSourceIVA(prodSourceKey, compWorkIVA.iva_key);

					if(gpmSourceIVA != null)
					{
						String aQualified= (gpmSourceIVA.finalDecision != null && "Y".equals( gpmSourceIVA.finalDecision.name()) ? "QUALIFIED" : "NOT_QUALIFIED");
						qualtxComp.qualified_flg = (gpmSourceIVA.finalDecision == null)? "" : aQualified;
					}else
					{
						logger.error("GPM SROURCE IVA NOT FOUND, Failed to update the iva decision " + compWorkPackage.compWork.qtx_wid + ":" + compWorkPackage.compWork.qtx_wid);
					}
				}
				
				if (compWorkIVA.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_COMP_TRACE_VALUE_CHANGE))
				{
					GPMClaimDetailsSourceIVAContainer claimdetailsContainer = aClaimsDetailCache.getClaimDetails(compWorkIVA.iva_key);
					GPMClaimDetails aClaimDetails = claimdetailsContainer.getPrimaryClaimDetails();
					String ftaCodeGroup = QualTXUtility.determineFTAGroupCode(qualtx.org_code, qualtx.fta_code, qtxBusinessLogicProcessor.propertySheetManager);
					Map<String,String> flexFieldMap = getFeildMapping("STP", ftaCodeGroup);
					BigDecimal traced_value = (BigDecimal)aClaimDetails.getValue(flexFieldMap.get("TRACED_VALUE"));
					if(traced_value != null )
					{
						qualtxComp.traced_value = traced_value.doubleValue();
					}
					qualtxComp.traced_value_currency =(String) aClaimDetails.getValue(flexFieldMap.get("TRACED_VALUE_CURRENCY"));
				}
				if (compWorkIVA.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_COMP_CUMULATION_CHANGE))
				{
				
					this.qtxBusinessLogicProcessor.cumulationComputationRule.applyCumulationForComponent(qualtxComp, aGPMSourceIVAContainerCache.getSourceIVABySource(qualtxComp.prod_key), aClaimsDetailCache ,this.repos);
				}
				if (compWorkIVA.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_COMP_PREV_YEAR_QUAL_CHANGE))
				{
					Long prodSourceKey = (qualtxComp != null) ? qualtxComp.prod_src_key : ((bomComp.prod_src_key != 0) ? new Long(bomComp.prod_src_key) : null);
					
					if (prodSourceKey == null)
						throw new Exception("Error attempting to pull comp IVA data with invalid prod source key " + prodSourceKey + " for comp iva work " + compWorkIVA.qtx_wid + ":" + compWorkIVA.qtx_comp_wid + ":" + compWorkIVA.qtx_comp_iva_wid);
					
					if (compWorkPackage.gpmSourceIVAProductContainer == null)
						throw new Exception("GPMSourceIVAProductContainer not present during comp IVA pull for work " + compWorkPackage.compWork.qtx_wid + ":" + compWorkPackage.compWork.qtx_comp_wid);

					this.qtxBusinessLogicProcessor.cumulationComputationRule.applyCumulationForComponent(qualtxComp, compWorkPackage.gpmSourceIVAProductContainer.getGPMSourceIVAProductSourceContainerByProdSourceKey(qualtxComp.prod_src_key), aClaimsDetailCache, this.repos);
					
					if (!"Y".equalsIgnoreCase(qualtxComp.cumulation_rule_applied) && ( qualtxComp.qualified_flg == null || "".equalsIgnoreCase(qualtxComp.qualified_flg)))
					{
						Date origEffectiveFrom = qualtxComp.qualified_from;
						Date origEffectiveTo = qualtxComp.qualified_to;
						boolean isPrevYearQualApplied = this.qtxBusinessLogicProcessor.previousYearQualificationRule.applyPrevYearQualForComponent(bomComp, qualtxComp, compWorkPackage.gpmSourceIVAProductContainer.getGPMSourceIVAProductSourceContainerByProdSourceKey(qualtxComp.prod_src_key), aClaimsDetailCache, this.repos);
						if (!isPrevYearQualApplied)
						{
							this.qtxBusinessLogicProcessor.cumulationComputationRule.applyCumulationForComponent(qualtxComp, compWorkPackage.gpmSourceIVAProductContainer.getGPMSourceIVAProductSourceContainerByProdSourceKey(qualtxComp.prod_src_key), aClaimsDetailCache, this.repos);
							qualtxComp.qualified_from = origEffectiveFrom;
							qualtxComp.qualified_to = origEffectiveTo;
						}
					}
				}
				
				if (compWorkIVA.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_SRC_IVA_DELETED))
				{
					if (qualtxComp == null) throw new Exception("Qualtx component " + work.qualtx_comp_key + " not found on qualtx " + parentWork.details.qualtx_key);
					qualtxComp.prod_src_iva_key = null;
				}
				
				if (compWorkIVA.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_IVA_CHANGE_M_I))
				{
					if (qualtxComp == null) throw new Exception("Qualtx component " + work.qualtx_comp_key + " not found on qualtx " + parentWork.details.qualtx_key);

					Long prodSourceKey = qualtxComp.prod_src_key;

					if (prodSourceKey == null) throw new Exception("Error attempting to pull comp IVA data with invalid prod source key " + prodSourceKey + " for comp iva work " + compWorkIVA.qtx_wid + ":" + compWorkIVA.qtx_comp_wid + ":" + compWorkIVA.qtx_comp_iva_wid);

					if (compWorkPackage.gpmSourceIVAProductContainer == null) throw new Exception("GPMSourceIVAProductContainer not present during comp IVA pull for work " + compWorkPackage.compWork.qtx_wid + ":" + compWorkPackage.compWork.qtx_comp_wid);

					GPMSourceIVAProductContainer aContainer = aGPMSourceIVAContainerCache.getSourceIVAByProduct(qualtxComp.prod_key);
					aContainer.indexByProdSourceKey();
					GPMSourceIVA gpmSourceIVA = aContainer.getGPMSourceIVA(prodSourceKey, compWorkIVA.iva_key);

					if (gpmSourceIVA != null)
					{
						if (STPDecisionEnum.M.equals(gpmSourceIVA.systemDecision))
						{
							qualtxComp.is_active = "Y";
						}
						else if (STPDecisionEnum.I.equals(gpmSourceIVA.systemDecision))
						{
							qualtxComp.is_active = "N";
						}
					}

				}
				
				if (compWorkIVA.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_SRC_CHANGE) || compWorkIVA.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_SRC_DELETED))
				{
					if (qualtxComp == null) throw new Exception("Qualtx component " + work.qualtx_comp_key + " not found on qualtx " + parentWork.details.qualtx_key);

					qualtxComp.prod_src_key = null;
					qualtxComp.supplier_key = null;
					qualtxComp.manufacturer_key = null;
					qualtxComp.prod_src_iva_key = null;
					aQualTXComponentUtilityforComp = new QualTXComponentUtility(qualtxComp, bomComp, aClaimsDetailCache, aGPMSourceIVAContainerCache, gpmClassCache, aDataExtensionConfigurationRepository, null);
					aQualTXComponentUtilityforComp.setQualTXBusinessLogicProcessor(this.qtxBusinessLogicProcessor);
					aQualTXComponentUtilityforComp.pullIVAData();
				}
				
				if (compWorkIVA.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_SRC_ADDED) || compWorkIVA.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_NEW_IVA_IDENTIFED)) 
				{
					if (qualtxComp == null) throw new Exception("Qualtx component " + work.qualtx_comp_key + " not found on qualtx " + parentWork.details.qualtx_key);
					if (bomComp == null) throw new Exception("BOMComponent (" + work.bom_comp_key + ") not found on BOM(" + parentWorkPackage.bom.alt_key_bom + ")");
					aQualTXComponentUtilityforComp = new QualTXComponentUtility(qualtxComp, bomComp, aClaimsDetailCache, aGPMSourceIVAContainerCache, gpmClassCache, aDataExtensionConfigurationRepository, null);
					aQualTXComponentUtilityforComp.setQualTXBusinessLogicProcessor(this.qtxBusinessLogicProcessor);
					aQualTXComponentUtilityforComp.pullIVAData();
				}
			}			
		}
		
		//TODO move all of this logic to QTXPersistenceConsumer.  use one date to update all records modified in qualtx hierarchy.
		Timestamp currentDate = new Timestamp(System.currentTimeMillis());
		if (work.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_COMP_ADDED))
		{
			qualtxComp.created_by = parentWork.userId;
			qualtxComp.created_date = currentDate;
		}
	}
	public Map<String, String> getFeildMapping(String deName, String ftaCodeGroup) throws Exception {
		String groupName = MessageFormat.format("{0}{1}{2}", deName, GroupNameSpecification.SEPARATOR, ftaCodeGroup);
		DataExtensionConfiguration	aCfg = this.repos.getDataExtensionConfiguration(groupName);
		return aCfg.getFlexColumnMapping();
	}

	protected void processWork() throws Exception
	{
		for (CompWorkPackage compWorkPackage : this.workList)
		{
			logger.debug("Processing ar_qtx_comp_work " + compWorkPackage.compWork.qtx_wid + ":" +compWorkPackage.compWork.qtx_comp_wid);
			try
			{
				this.doWork(compWorkPackage);
			}
			catch (Exception e)
			{
				logger.error("Failed processing comp work " + compWorkPackage.compWork.qtx_wid + ":" + compWorkPackage.compWork.qtx_wid);
				compWorkPackage.failure = e;
				
				throw e;
			}
			finally
			{
				((QTXCompWorkProducer) this.producer).registeredCompWorkCompleted(compWorkPackage);
			}
		}
	}
}
