package com.ambr.gtm.fta.qts.workmgmt;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.gtm.fta.qps.bom.BOMDataExtension;
import com.ambr.gtm.fta.qps.bom.BOMPrice;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassification;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVA;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAContainerCache;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAProductContainer;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTX;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXBusinessLogicProcessor;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXDataExtension;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXPrice;
import com.ambr.gtm.fta.qts.QTXWork;
import com.ambr.gtm.fta.qts.QTXWorkHS;
import com.ambr.gtm.fta.qts.RequalificationWorkCodes;
import com.ambr.gtm.fta.qts.TrackerCodes;
import com.ambr.gtm.fta.qts.config.QEConfigCache;
import com.ambr.gtm.fta.qts.util.Env;
import com.ambr.gtm.fta.qts.util.SubPullConfigContainer;
import com.ambr.gtm.fta.qts.util.SubPullConfigData;
import com.ambr.gtm.fta.qts.util.TradeLane;
import com.ambr.gtm.fta.trade.client.TradeQualtxClient;

public class QTXWorkConsumer extends QTXConsumer<WorkPackage>
{
	private static Logger logger = LogManager.getLogger(QTXWorkConsumer.class);

	private QualTXBusinessLogicProcessor qtxBusinessLogicProcessor;
	

	public QualTXBusinessLogicProcessor getQtxBusinessLogicProcessor()
	{
		return qtxBusinessLogicProcessor;
	}

	public void setQtxBusinessLogicProcessor(QualTXBusinessLogicProcessor qtxBusinessLogicProcessor)
	{
		this.qtxBusinessLogicProcessor = qtxBusinessLogicProcessor;
	}

	public QTXWorkConsumer(ArrayList<WorkPackage> workList) {
		super(workList);
	}

	public QTXWorkConsumer(WorkPackage workPackage) {
		super(workPackage);
	}

	/*
	 * NOTE!!!!!
	 * Any changes to reason codes or logic within the handling of a reason code
	 * requires a review of QTXWorkStaging as it will change the requirements as to when BOM/Prod/IVA resource data
	 * is collected from cache
	 */
	public void doWork(WorkPackage workPackage) throws Exception
	{
		//An error occurred while staging the data for this WorkPackage (usually resource data failed to pull)
		if (workPackage.failure != null)
			throw workPackage.failure;
		
		QTXWork work = workPackage.work;
		QualTX qualtx = workPackage.qualtx;
		BOM bom = workPackage.bom;
		QEConfigCache qeConfigCache = ((QTXWorkProducer)(this.producer)).queueUniverse.qeConfigCache;
		if (qualtx == null)
		{
			throw new Exception("Failed to process work item " + workPackage.work.qtx_wid + " qualtx not found (" + workPackage.work.details.qualtx_key + ")");
		}
		
		for (QualTXDataExtension  qualtxDE :qualtx.deList)
		{
			if(qualtxDE.group_name.equalsIgnoreCase("STP:"+qualtx.fta_code_group))
			{
				qualtx.deList.remove(qualtxDE);
			}
		}
		
		if(work.details.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_MASS_QUALIFICATION) == true)
		{
			qualtx.is_active = "N";
			workPackage.isReadyForQualification = false;
			return;
		}
		
		if(work.details.isReasonCodeFlagSet(RequalificationWorkCodes.SUB_BOM_CHG) == true)
		{
			qualtx.rm_construction_status = TrackerCodes.QualTXContructionStatus.INIT.ordinal();
			qualtx.in_construction_status = TrackerCodes.QualTXContructionStatus.INIT.ordinal();
		}
		
		if (work.details.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_HDR_CHG) == true)
		{
			if (bom == null) throw new Exception("BOM resource not present (" + work.bom_key + ") for work item " + work.qtx_wid);
			
			qualtx.currency_code = bom.currency_code;
			qualtx.gross_weight = bom.gross_weight;
			qualtx.uom = bom.uom;
			qualtx.area = bom.area; 
			qualtx.area_uom = bom.area_uom;
			qualtx.direct_processing_cost = bom.direct_processing_cost;
			qualtx.assembly_type = bom.assembly_type;
			qualtx.rvc_restricted = TrackerCodes.AssemblyType.INTERMEDIATE.name().equals(qualtx.assembly_type) ? "Y" : "";
			
			//Logic for re-setting include for traced value flag if product family changes from AUTOMOBILES to something else.
			if("AUTOMOBILES".equalsIgnoreCase(qualtx.prod_family) && !"AUTOMOBILES".equalsIgnoreCase(bom.prod_family))
			{
				qualtx.include_for_trace_value = "N";
				qualtx.target_roo_id = null;
			}
			qualtx.prod_family = bom.prod_family;
			qualtx.cost = bom.cost;
			qualtx.value = bom.price;
		}

		if (work.details.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_PRC_CHG) == true || work.details.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_COST_ELEMENT) == true)
		{
			if (bom == null) throw new Exception("BOM resource not present (" + work.bom_key + ") for work item " + work.qtx_wid);
			
			workPackage.entityMgr.loadTable("MDI_QUALTX_PRICE");
			
			Double transactionValue = qualtx.cost;
			Double netValue = qualtx.value;
			ArrayList<QualTXPrice> qualTXPricelist = qualtx.priceList;
			boolean isTransactionValueExist = false;
			boolean isNetValueExist = false;
			Set<QualTXPrice> deleteBomPrice = new HashSet(); 
			deleteBomPrice.addAll(qualtx.priceList);
			for (BOMPrice bomPrice : bom.priceList)
			{
				if (bomPrice.price_type == null)
					continue;
				
				boolean isPriceTypeExist = false;
				for (QualTXPrice Qualprice : qualTXPricelist)
				{
					if (bomPrice.price_type.equals(Qualprice.price_type))
					{
						isPriceTypeExist = true;
						deleteBomPrice.remove(Qualprice);
						if (bomPrice.price_type.equalsIgnoreCase("TRANSACTION_VALUE"))
						{
							transactionValue = bomPrice.price;
							isTransactionValueExist = true;
						}
						else if (bomPrice.price_type.equalsIgnoreCase("NET_COST"))
						{
							netValue = bomPrice.price;
							isNetValueExist = true;
						}
						if (bomPrice.price != Qualprice.price)
						{
							Qualprice.price = bomPrice.price;
						}
						if (!bomPrice.currency_code.equals(Qualprice.currency_code))
						{
							Qualprice.currency_code = bomPrice.currency_code;
						}
					}
				}
				if(!isPriceTypeExist)
				{
					QualTXPrice price = qualtx.createPrice();
	
					price.price_type = bomPrice.price_type;
					price.price = bomPrice.price;
					price.currency_code = bomPrice.currency_code;
	
					price.created_by = work.userId;
					price.created_date = new Timestamp(System.currentTimeMillis());
					price.last_modified_by = work.userId;
					price.last_modified_date = price.created_date;
	
					if (bomPrice.price_type.equalsIgnoreCase("TRANSACTION_VALUE"))
					{
						transactionValue = bomPrice.price;
						isTransactionValueExist = true;
					}
					else if (bomPrice.price_type.equalsIgnoreCase("NET_COST")) 
					{
						netValue = bomPrice.price;
						isNetValueExist = true;
					}

				}
			}
			qualtx.priceList.removeAll(deleteBomPrice);
			qualtx.value = (isTransactionValueExist) ? transactionValue : 0.0;
			qualtx.cost = (isNetValueExist) ? netValue : 0.0;
			qtxBusinessLogicProcessor.populateRollupPriceDetails(bom, qualtx, "ALL");

		}

		if (work.details.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_PROD_TXT_DE) == true)
		{
			if (bom == null) throw new Exception("BOM resource not present (" + work.bom_key + ") for work item " + work.qtx_wid);
			
			ArrayList<BOMDataExtension> bomDataExtensions = bom.getDataExtensionByGroupName("IMPL_BOM_PROD_FAMILY:TEXTILES");
			if(bomDataExtensions.size() > 0)
			{
				qualtx.knit_to_shape = (String) ((BOMDataExtension)bomDataExtensions.get(0)).getValue("FLEXFIELD_VAR6");
			}
		}

		if (work.details.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_PROD_AUTO_DE) == true)
		{
			if (bom == null) throw new Exception("BOM resource not present (" + work.bom_key + ") for work item " + work.qtx_wid);
			
			ArrayList<BOMDataExtension> bomDataExtensions = bom.getDataExtensionByGroupName("IMPL_BOM_PROD_FAMILY:AUTOMOBILES");
			if(bomDataExtensions.size() > 0)
			{
				qualtx.include_for_trace_value = (String) ((BOMDataExtension)bomDataExtensions.get(0)).getValue("FLEXFIELD_VAR1");
				qualtx.listed_material = (String) ((BOMDataExtension)bomDataExtensions.get(0)).getValue("FLEXFIELD_VAR5");
			}
		}

		if (work.details.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_SRC_IVA_DELETED))
		{
			workPackage.deleteBOMQual = true;
		}

		if (work.details.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_IVA_AND_CLAIM_DTLS_CHANGE) == true)
		{
			// As discussed with Pavan, we donot need to pull the any data to
			// qualtx since we are refering directly the DE columns(claim
			// details) during qualification.
		}

		if (work.details.isReasonCodeFlagSet(RequalificationWorkCodes.CONTENT_ROO_CHANGE) == true)
		{
			// None of the data required to pull, simply run the qualification
		}

		if (work.details.isReasonCodeFlagSet(RequalificationWorkCodes.BOM_TXREF_CHG) == true)
		{
			// This is an BOM data change, we are not copying any data to qualtx
			// and qualtx_comp.simply run the qualification
		}

		if (work.details.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_NEW_HEADER_IVA_IDENTIFED) == true) {
			// prep service will create a qualtx and qualtx comp table data,we
			// are not processing this reason code.
		}

		if (work.details.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_SRC_CHANGE) == true || work.details.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_IVA_CHANGE_M_I))
		{
			GPMSourceIVAContainerCache aGPMSourceIVAContainerCache = ((QTXWorkProducer)(this.producer)).queueUniverse.ivaCache;
			GPMSourceIVA gpmSourceIVA = null;

			Long prodSourceKey = qualtx.prod_src_key;

			if (prodSourceKey == null) throw new Exception("Error attempting to pull comp IVA data with invalid prod source key for qualtx : " + qualtx.alt_key_qualtx);

			GPMSourceIVAProductContainer aContainer = aGPMSourceIVAContainerCache.getSourceIVAByProduct(qualtx.prod_key);
			if(null != aContainer)
			{
				aContainer.indexByProdSourceKey();
			    gpmSourceIVA = aContainer.getGPMSourceIVA(prodSourceKey, qualtx.prod_src_iva_key);
			}
			
			qualtx.prod_src_iva_key = null;
			qualtx.supplier_key = null;
			qualtx.manufacturer_key = null;
			qualtx.qualified_flg = "";
			workPackage.deleteBOMQual = true;
			qualtx.is_active = "N";
		}

		if (work.workHSList != null)
		{
			for (QTXWorkHS workHS : work.workHSList)
			{
				logger.debug("Processing work hs " + workHS.qtx_hspull_wid);

				int parentHsLength = -1;
				List<TradeLane> subPullConfig = qeConfigCache.getQEConfig(work.company_code).getSubpullConfigList();
				if (subPullConfig != null)
				{
					Optional<TradeLane> tradeLane = subPullConfig.stream().filter(p -> p.getFtaCode().equalsIgnoreCase(qualtx.fta_code) && p.getCtryOfImport().equalsIgnoreCase(qualtx.ctry_of_import)).findFirst();
					if (tradeLane.isPresent())
					{
						SubPullConfigContainer container = qeConfigCache.getQEConfig(work.company_code).getSubPullConfigContainer();
						SubPullConfigData subpullConfigDate = container.getTradeLaneData(tradeLane.get());
						if (subpullConfigDate != null)
						{
							String lengthStr = subpullConfigDate.getHeaderHsLength();
							if (lengthStr != null && !lengthStr.isEmpty()) parentHsLength = Integer.parseInt(lengthStr);
						}
					}
				}
				if (workHS.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_CTRY_CMPL_CHANGE) == true || workHS.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_CTRY_CMPL_ADDED) == true)
				{
					if (workPackage.gpmClassificationProductContainer == null)
						throw new Exception("GPMClassificationProductContainer not present during HS pull for work " + workHS.qtx_wid + ":" + workHS.qtx_hspull_wid);
					
					if (workHS.ctry_cmpl_key == null)
						throw new Exception("Error in attempting to process pull with null ctry cmpl key for work HS " + workHS.qtx_wid + ":" + workHS.qtx_hspull_wid);
					
					GPMClassification gpmClassification = workPackage.gpmClassificationProductContainer.getGPMClassificationByCtryCmplKey(workHS.ctry_cmpl_key);
					
					if (gpmClassification == null) 
						throw new Exception("Failed to find GPMClassification " + workHS.ctry_cmpl_key + " for work HS " + workHS.qtx_wid + ":" + workHS.qtx_hspull_wid);
					
					qualtx.hs_num= ((parentHsLength != -1 && gpmClassification.imHS1.length() > parentHsLength) ? gpmClassification.imHS1.substring(0, parentHsLength) : gpmClassification.imHS1);
					qualtx.sub_pull_ctry = gpmClassification.ctryCode;
					qualtx.prod_ctry_cmpl_key = gpmClassification.cmplKey;
				}

				if (workHS.isReasonCodeFlagSet(RequalificationWorkCodes.GPM_CTRY_CMPL_DELETED) == true)
				{
					qualtx.hs_num = null;
					qualtx.sub_pull_ctry = null;
					qualtx.prod_ctry_cmpl_key = null;
				}
			}
		}
	}

	@Override
	protected void processWork() throws Exception
	{
		TradeQualtxClient TradeQualtxClient = Env.getSingleton().getTradeQualtxClient();
		
		for (WorkPackage workPackage : this.workList)
		{
			try
			{
				logger.debug("Processing work item " + workPackage.work.qtx_wid + " alt qualtx key "
						+ ((workPackage.qualtx != null) ? workPackage.qualtx.alt_key_qualtx : "NOT_FOUND") 
						+ ", workcomp count = "
						+ workPackage.work.compWorkList.size());

				//Lock is acquired only once for chain of workpackages - set on rootWorkPackage
				if (workPackage.getLockId() == null)
				{
					workPackage.setLockId(TradeQualtxClient.acquireLock(workPackage.work.company_code,
					 	workPackage.work.userId,
					 	workPackage.work.details.qualtx_key, false));
				}
				// Do all business logic
				this.doWork(workPackage); 
			}
			catch (Exception e)
			{
				logger.error("Failed processing work " + workPackage.work.qtx_wid, e);
				workPackage.failure = e;
			}
			finally
			{
				((QTXWorkProducer) this.producer).registeredWorkCompleted(workPackage);
			}
		}
	}
}
