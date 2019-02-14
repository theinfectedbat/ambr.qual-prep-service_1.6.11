package com.ambr.gtm.fta.qps.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVA;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAProductSourceContainer;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTX;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponent;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponentDataExtension;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponentPrice;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXPrice;
import com.ambr.gtm.fta.qps.qualtx.engine.result.TradeLaneStatusTracker;
import com.ambr.gtm.fta.qts.QtxStatusUpdateRequest;
import com.ambr.gtm.fta.qts.api.TrackerClientAPI;
import com.ambr.gtm.fta.qts.trade.MDIQualTx;
import com.ambr.gtm.fta.trade.model.BOMQualAuditEntity;
import com.ambr.gtm.fta.trade.model.BOMQualAuditEntity.STATE;
import com.ambr.gtm.utils.legacy.sps.SimplePropertySheet;
import com.ambr.gtm.utils.legacy.sps.SimplePropertySheetManager;
import com.ambr.platform.rdbms.orm.DataRecordColumnModification;
import com.ambr.platform.rdbms.orm.DataRecordModificationTracker;
import com.ambr.platform.rdbms.orm.EntityManager;
import com.ambr.platform.utils.log.MessageFormatter;

public class QualTXUtility 
{
	static Logger		logger = LogManager.getLogger(QualTXUtility.class);
	QualTX				qualTX;
	TrackerClientAPI    trackerClientAPI;
	TradeLaneStatusTracker	statusTracker;
	
	public QualTXUtility(QualTX theQualTX, TrackerClientAPI theTrackClientAPI, TradeLaneStatusTracker theStatusTracker) throws Exception
	{
		this.qualTX = theQualTX;
		this.trackerClientAPI = theTrackClientAPI;
		this.statusTracker = theStatusTracker;
	}
	
	public static String determineFTAGroupCode(String orgCode, String ftacode, SimplePropertySheetManager propertySheetManager)
	{
		try
		{
			if (ftacode != null && !ftacode.isEmpty())
			{
				List<String> includedFTACodeList = null;
				List<String> excludedFTACodeList = null;
				boolean isEURInclusionFtaCode = false;
				boolean existsInExcludedList = false;

				SimplePropertySheet aPropertySheet = propertySheetManager.getPropertySheet(orgCode, "FTA_EUR_INCLUSIONS");
				if (aPropertySheet != null)
				{
					String aIncludedFTACode = aPropertySheet.getStringValue("FTA_CODE_LIST");
					if (aIncludedFTACode != null && !aIncludedFTACode.isEmpty())
					{
						includedFTACodeList = new ArrayList<String>(Arrays.asList(aIncludedFTACode.split(",")));
						if (includedFTACodeList.size() > 0 && includedFTACodeList.contains(ftacode))
						{
							isEURInclusionFtaCode = true;
						}
					}
				}
				if (isEURInclusionFtaCode)
				{
					return "EUR";
				}
				else if (ftacode.startsWith("EFTA")) // If FTA code is EFTA
					return "EFTA";
				else if (ftacode.startsWith("EU_") || ftacode.startsWith("CH_") || ftacode.startsWith("TR_") || ftacode.startsWith("MA_"))
				{
					String countryCode = "";

					if (ftacode.startsWith("EU_")) countryCode = "EUR";
					else if (ftacode.startsWith("CH_")) countryCode = "CH";
					else if (ftacode.startsWith("MA_")) countryCode = "MA";
					else countryCode = "TR";

					aPropertySheet = propertySheetManager.getPropertySheet(orgCode, "FTA_EUR_EXCLUSIONS");
					if (aPropertySheet != null)
					{
						String aExcludedFTACode = aPropertySheet.getStringValue("FTA_CODE_LIST");
						if (aExcludedFTACode != null && !aExcludedFTACode.isEmpty())
						{
							excludedFTACodeList = new ArrayList<String>(Arrays.asList(aExcludedFTACode.split(",")));
							if (excludedFTACodeList.size() > 0 && excludedFTACodeList.contains(ftacode))
							{
								existsInExcludedList = true;
							}
						}
					}

					// If it is EU FTA Code and the FTA CODE is not in the
					// Excluded list then copy "EUR" to FTA_CODE_GROUP
					if (!existsInExcludedList && "EUR".equals(countryCode)) return "EUR"; // START with EUR and not in EUR Exclusion
				}
			}
			else return ftacode;
		}
		catch (Exception exec)
		{
			MessageFormatter.error(logger, "determineFTAGroupCode", exec, "Org Code [{0}]: Error while determining FTA code group for FTA [{1}]", orgCode, ftacode);
		}
		// For all other scenarion it will return the Same FTA CODE
		return ftacode;
	}
	
	public static GPMSourceIVA getGPMIVARecord(GPMSourceIVAProductSourceContainer prodSourceContainer, String fta_code, String ctry_of_import, Date effective_from, Date effective_to)
	{
		boolean isMatched = false;
		if (prodSourceContainer!= null && prodSourceContainer.ivaList != null)
		{
			for (GPMSourceIVA gpmSRCIva : prodSourceContainer.ivaList)
			{
				if(gpmSRCIva == null)
					continue;
				
				isMatched = fta_code.equals(gpmSRCIva.ftaCode);
				if (ctry_of_import != null) isMatched = isMatched && gpmSRCIva.ctryOfImport.equals(ctry_of_import);

				Date srcIVAeffectiveFrom = new Date(gpmSRCIva.effectiveFrom.getTime());
				Date srcIVAeffectiveTo = new Date(gpmSRCIva.effectiveTo.getTime());

				isMatched = isMatched && (srcIVAeffectiveFrom.before(effective_from) || srcIVAeffectiveFrom.equals(effective_from)) && (srcIVAeffectiveTo.after(effective_to) || srcIVAeffectiveTo.equals(effective_to));

				isMatched = isMatched && (gpmSRCIva.finalDecision != null && gpmSRCIva.finalDecision.name().equals("Y") && gpmSRCIva.systemDecision != null && (!gpmSRCIva.systemDecision.name().equals("C") && !gpmSRCIva.systemDecision.name().equals("I")) && gpmSRCIva.ftaEnabledFlag);
				if (isMatched) return gpmSRCIva;
			}
		}

		return null;
	}
	
	
	public static GPMSourceIVA getGPMIVARecordForPYQ(GPMSourceIVAProductSourceContainer prodSourceContainer, String fta_code, String ctry_of_import, Date effective_from, Date effective_to)
	{
		boolean isMatched = false;
		for (GPMSourceIVA gpmSRCIva : prodSourceContainer.ivaList)
		{
			isMatched = gpmSRCIva.ftaCode.equals(fta_code);
			if (ctry_of_import != null) isMatched = isMatched && gpmSRCIva.ctryOfImport.equals(ctry_of_import);

			Date srcIVAeffectiveFrom = new Date(gpmSRCIva.effectiveFrom.getTime());
			Date srcIVAeffectiveTo = new Date(gpmSRCIva.effectiveTo.getTime());

			isMatched = isMatched && (srcIVAeffectiveFrom.before(effective_from) || srcIVAeffectiveFrom.equals(effective_from)) && (srcIVAeffectiveTo.after(effective_to) || srcIVAeffectiveTo.equals(effective_to));

			isMatched = isMatched && ((gpmSRCIva.finalDecision == null || ("Y").equals(gpmSRCIva.finalDecision.name())
					||("N").equals(gpmSRCIva.finalDecision.name())) && gpmSRCIva.systemDecision != null && (!gpmSRCIva.systemDecision.name().equals("C") && !gpmSRCIva.systemDecision.name().equals("I")) && gpmSRCIva.ftaEnabledFlag);
			if (isMatched) return gpmSRCIva;
		}

		return null;
	}
	
	
	public void readyForQualification(Integer theAnalysisMethod, Integer priority) throws Exception
	{
		try
		{
			QtxStatusUpdateRequest theQualificationRequest = new QtxStatusUpdateRequest();
			theQualificationRequest.setBOMKey(this.qualTX.src_key);
			theQualificationRequest.setQualtxKey(this.qualTX.alt_key_qualtx);
			theQualificationRequest.setUserId(this.qualTX.created_by);
			theQualificationRequest.setOrgCode(this.qualTX.org_code);
			theQualificationRequest.setIvaKey(this.qualTX.prod_src_iva_key);
			theQualificationRequest.setPriority(priority);
			theQualificationRequest.setAnalysisMethod(theAnalysisMethod);

			if (!this.trackerClientAPI.executeQualification(theQualificationRequest))
			{
				MessageFormatter.debug(logger, "readyForQualificaiton", "BOM [{0,number,#}] Qual TX [{1,number,#}]: Failed to post qualification work", this.qualTX.src_key, this.qualTX.alt_key_qualtx);
			}

			this.statusTracker.qualificationSucceeded();
		}
		catch (Exception e)
		{
			this.statusTracker.qualificationFailed(e);
			MessageFormatter.error(logger, "readyForQualificaiton", e, "BOM [{0,number,#}] Qual TX [{1,number,#}]: Failed to post qualification work",  this.qualTX.src_key, this.qualTX.alt_key_qualtx);
		}
	}
	
	

	/**
	 * @param theAltKeyQualTX
	 * @param theCompanyCode
	 * @param theUserId
	 * @param theEntityManager
	 * @return
	 * @throws Exception
	 */
	public static BOMQualAuditEntity buildAudit(Long theAltKeyQualTX, String theCompanyCode, String theUserId, EntityManager<QualTX> theEntityManager,String theReason) throws Exception
	{
		BOMQualAuditEntity audit = new BOMQualAuditEntity(MDIQualTx.TABLE_NAME.toUpperCase(), theAltKeyQualTX);
		audit.setSurrogateKeyColumn("ALT_KEY_QUALTX");
		audit.setOrgCode(theCompanyCode);
		audit.setUserID(theUserId);
		audit.setState(STATE.MODIFY);
		audit.setReason(theReason);
		HashMap<Long, BOMQualAuditEntity> auditMap = new HashMap<Long, BOMQualAuditEntity>();
		boolean componentDeleted = false;

		for (DataRecordModificationTracker<?> deletedRecord : theEntityManager.getTracker().getDeletedRecords())
		{
			if (deletedRecord.modifiableRecord instanceof QualTXPrice)
			{
				HashMap<String, Object> rowKeyColumns = new HashMap<String, Object>();
				rowKeyColumns.put("TX_ID", ((QualTXPrice) deletedRecord.modifiableRecord).tx_id);
				rowKeyColumns.put("ORG_CODE", ((QualTXPrice) deletedRecord.modifiableRecord).org_code);
				rowKeyColumns.put("PRICE_SEQ_NUM", ((QualTXPrice) deletedRecord.modifiableRecord).price_seq_num);
				Long altkey = ((QualTXPrice) deletedRecord.modifiableRecord).alt_key_price;
				BOMQualAuditEntity priceAudit = auditMap.get(altkey);
				if (priceAudit == null) priceAudit = new BOMQualAuditEntity("MDI_QUALTX_PRICE", altkey);
				priceAudit.setSurrogateKeyColumn("ALT_KEY_PRICE");
				priceAudit.setState(STATE.DELETE);
				priceAudit.setRowKeyColumns(rowKeyColumns);
				auditMap.put(altkey, priceAudit);
				audit.addChildTable(priceAudit);
			}
			else if (deletedRecord.modifiableRecord instanceof QualTXComponent)
			{
				HashMap<String, Object> rowKeyColumns = new HashMap<String, Object>();
				rowKeyColumns.put("TX_ID", ((QualTXComponent) deletedRecord.modifiableRecord).tx_id);
				rowKeyColumns.put("ORG_CODE", ((QualTXComponent) deletedRecord.modifiableRecord).org_code);
				rowKeyColumns.put("COMP_ID", ((QualTXComponent) deletedRecord.modifiableRecord).comp_id);
				Long altkey = ((QualTXComponent) deletedRecord.modifiableRecord).alt_key_comp;
				BOMQualAuditEntity compAudit = auditMap.get(altkey);
				if (compAudit == null) compAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP", altkey);
				compAudit.setSurrogateKeyColumn("ALT_KEY_COMP");
				compAudit.setState(STATE.DELETE);
				compAudit.setRowKeyColumns(rowKeyColumns);
				auditMap.put(altkey, compAudit);
				audit.addChildTable(compAudit);
				componentDeleted = true;
			}
		}

		if (!componentDeleted)
		{
			for (DataRecordModificationTracker<?> deletedRecord : theEntityManager.getTracker().getDeletedRecords())
			{
				if (deletedRecord.modifiableRecord instanceof QualTXComponentPrice)
				{
					HashMap<String, Object> rowKeyColumns = new HashMap<String, Object>();
					rowKeyColumns.put("TX_ID", ((QualTXComponentPrice) deletedRecord.modifiableRecord).tx_id);
					rowKeyColumns.put("ORG_CODE", ((QualTXComponentPrice) deletedRecord.modifiableRecord).org_code);
					rowKeyColumns.put("COMP_ID", ((QualTXComponentPrice) deletedRecord.modifiableRecord).comp_id);
					rowKeyColumns.put("PRICE_SEQ_NUM", ((QualTXComponentPrice) deletedRecord.modifiableRecord).price_seq_num);
					Long altkey = ((QualTXComponentPrice) deletedRecord.modifiableRecord).alt_key_price;
					BOMQualAuditEntity compPriceAudit = auditMap.get(altkey);
					if (compPriceAudit == null) compPriceAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP_PRICE", altkey);
					compPriceAudit.setSurrogateKeyColumn("ALT_KEY_PRICE");
					compPriceAudit.setState(STATE.DELETE);
					compPriceAudit.setRowKeyColumns(rowKeyColumns);
					auditMap.put(altkey, compPriceAudit);
					audit.addChildTable(compPriceAudit);
				}

				else if (deletedRecord.modifiableRecord instanceof QualTXComponentDataExtension)
				{
					HashMap<String, Object> rowKeyColumns = new HashMap<String, Object>();
					rowKeyColumns.put("TX_ID", ((QualTXComponentDataExtension) deletedRecord.modifiableRecord).tx_id);
					rowKeyColumns.put("ORG_CODE", ((QualTXComponentDataExtension) deletedRecord.modifiableRecord).org_code);
					rowKeyColumns.put("COMP_ID", ((QualTXComponentDataExtension) deletedRecord.modifiableRecord).comp_id);
					rowKeyColumns.put("SEQ_NUM", ((QualTXComponentDataExtension) deletedRecord.modifiableRecord).seq_num);
					Long altkey = ((QualTXComponentDataExtension) deletedRecord.modifiableRecord).seq_num;
					BOMQualAuditEntity compDeAudit = auditMap.get(altkey);
					if (compDeAudit == null)
					{
						String thegroupName = ((QualTXComponentDataExtension) deletedRecord.modifiableRecord).group_name;
						compDeAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP", altkey, thegroupName.replaceAll(":", "_"), STATE.DELETE);
					}
					compDeAudit.setSurrogateKeyColumn("SEQ_NUM");
					compDeAudit.setState(STATE.DELETE);
					compDeAudit.setRowKeyColumns(rowKeyColumns);
					auditMap.put(altkey, compDeAudit);
					audit.addChildTable(compDeAudit);
				}

			}
		}

		for (DataRecordModificationTracker<?> modRecord : theEntityManager.getTracker().getModifiedRecords())
		{
			if (modRecord.modifiableRecord instanceof QualTX)
			{
				for (DataRecordColumnModification columnMod : modRecord.getColumnModifications())
				{
					audit.setModifiedColumn(columnMod.getColumnName().toUpperCase(), columnMod.getModifiedValue(), columnMod.getOriginalValue());
				}
				auditMap.put(theAltKeyQualTX, audit);
			}

			else if (modRecord.modifiableRecord instanceof QualTXPrice)
			{
				HashMap<String, Object> rowKeyColumns = new HashMap<String, Object>();
				rowKeyColumns.put("TX_ID", ((QualTXPrice) modRecord.modifiableRecord).tx_id);
				rowKeyColumns.put("ORG_CODE", ((QualTXPrice) modRecord.modifiableRecord).org_code);
				rowKeyColumns.put("PRICE_SEQ_NUM", ((QualTXPrice) modRecord.modifiableRecord).price_seq_num);

				Long altkey = ((QualTXPrice) modRecord.modifiableRecord).alt_key_price;
				BOMQualAuditEntity priceAudit = auditMap.get(altkey);

				if (priceAudit == null) priceAudit = new BOMQualAuditEntity("MDI_QUALTX_PRICE", altkey);
				for (DataRecordColumnModification columnMod : modRecord.getColumnModifications())
				{
					priceAudit.setModifiedColumn(columnMod.getColumnName().toUpperCase(), columnMod.getModifiedValue(), columnMod.getOriginalValue());
				}
				priceAudit.setRowKeyColumns(rowKeyColumns);
				priceAudit.setSurrogateKeyColumn("ALT_KEY_PRICE");
				priceAudit.setState(STATE.MODIFY);
				auditMap.put(altkey, priceAudit);
				audit.addChildTable(priceAudit);
			}
			else if (modRecord.modifiableRecord instanceof QualTXComponent)
			{
				HashMap<String, Object> rowKeyColumns = new HashMap<String, Object>();
				rowKeyColumns.put("TX_ID", ((QualTXComponent) modRecord.modifiableRecord).tx_id);
				rowKeyColumns.put("ORG_CODE", ((QualTXComponent) modRecord.modifiableRecord).org_code);
				rowKeyColumns.put("COMP_ID", ((QualTXComponent) modRecord.modifiableRecord).comp_id);

				Long altkey = ((QualTXComponent) modRecord.modifiableRecord).alt_key_comp;
				BOMQualAuditEntity compAudit = auditMap.get(altkey);
				if (compAudit == null) compAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP", altkey);
				compAudit.setSurrogateKeyColumn("ALT_KEY_COMP");
				compAudit.setState(STATE.MODIFY);
				for (DataRecordColumnModification columnMod : modRecord.getColumnModifications())
				{
					compAudit.setModifiedColumn(columnMod.getColumnName().toUpperCase(), columnMod.getModifiedValue(), columnMod.getOriginalValue());
				}
				compAudit.setRowKeyColumns(rowKeyColumns);
				auditMap.put(altkey, compAudit);
				audit.addChildTable(compAudit);
			}

			else if (modRecord.modifiableRecord instanceof QualTXComponentPrice)
			{
				HashMap<String, Object> rowKeyColumns = new HashMap<String, Object>();
				rowKeyColumns.put("TX_ID", ((QualTXComponentPrice) modRecord.modifiableRecord).tx_id);
				rowKeyColumns.put("ORG_CODE", ((QualTXComponentPrice) modRecord.modifiableRecord).org_code);
				rowKeyColumns.put("COMP_ID", ((QualTXComponentPrice) modRecord.modifiableRecord).comp_id);
				rowKeyColumns.put("PRICE_SEQ_NUM", ((QualTXComponentPrice) modRecord.modifiableRecord).price_seq_num);

				Long altkey = ((QualTXComponentPrice) modRecord.modifiableRecord).alt_key_price;
				BOMQualAuditEntity compPriceAudit = auditMap.get(altkey);
				if (compPriceAudit == null) compPriceAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP_PRICE", altkey);
				compPriceAudit.setSurrogateKeyColumn("ALT_KEY_PRICE");
				compPriceAudit.setState(STATE.MODIFY);
				for (DataRecordColumnModification columnMod : modRecord.getColumnModifications())
				{
					compPriceAudit.setModifiedColumn(columnMod.getColumnName().toUpperCase(), columnMod.getModifiedValue(), columnMod.getOriginalValue());
				}
				compPriceAudit.setRowKeyColumns(rowKeyColumns);
				auditMap.put(altkey, compPriceAudit);
				audit.addChildTable(compPriceAudit);
			}

			else if (modRecord.modifiableRecord instanceof QualTXComponentDataExtension)
			{
				HashMap<String, Object> rowKeyColumns = new HashMap<String, Object>();
				rowKeyColumns.put("TX_ID", ((QualTXComponentDataExtension) modRecord.modifiableRecord).tx_id);
				rowKeyColumns.put("ORG_CODE", ((QualTXComponentDataExtension) modRecord.modifiableRecord).org_code);
				rowKeyColumns.put("COMP_ID", ((QualTXComponentDataExtension) modRecord.modifiableRecord).comp_id);
				rowKeyColumns.put("SEQ_NUM", ((QualTXComponentDataExtension) modRecord.modifiableRecord).seq_num);

				Long altkey = ((QualTXComponentDataExtension) modRecord.modifiableRecord).seq_num;
				BOMQualAuditEntity compDeAudit = auditMap.get(altkey);
				if (compDeAudit == null) {
					String thegroupName = ((QualTXComponentDataExtension) modRecord.modifiableRecord).group_name;
					compDeAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP", altkey, thegroupName.replaceAll(":", "_"), STATE.MODIFY);
				}
				compDeAudit.setSurrogateKeyColumn("SEQ_NUM");
				compDeAudit.setState(STATE.MODIFY);
				for (DataRecordColumnModification columnMod : modRecord.getColumnModifications())
				{
					compDeAudit.setModifiedColumn(columnMod.getColumnName().toUpperCase(), columnMod.getModifiedValue(), columnMod.getOriginalValue());
				}
				compDeAudit.setRowKeyColumns(rowKeyColumns);
				auditMap.put(altkey, compDeAudit);
				audit.addChildTable(compDeAudit);
			}

		}

		for (DataRecordModificationTracker<?> newRecord : theEntityManager.getTracker().getNewRecords())
		{

			if (newRecord.modifiableRecord instanceof QualTXPrice)
			{
				QualTXPrice qualTXPrice = (QualTXPrice) newRecord.modifiableRecord;
				HashMap<String, Object> rowKeyColumns = new HashMap<String, Object>();
				rowKeyColumns.put("TX_ID", qualTXPrice.tx_id);
				rowKeyColumns.put("ORG_CODE", qualTXPrice.org_code);
				rowKeyColumns.put("PRICE_SEQ_NUM", qualTXPrice.price_seq_num);
				Long altkey = qualTXPrice.alt_key_price;
				BOMQualAuditEntity priceAudit = auditMap.get(altkey);
				if (priceAudit == null) priceAudit = new BOMQualAuditEntity("MDI_QUALTX_PRICE", altkey);
				priceAudit.setState(STATE.CREATE);
				priceAudit.setSurrogateKeyColumn("ALT_KEY_PRICE");
				priceAudit.setModifiedColumn("PRICE_TYPE", qualTXPrice.price_type, null);
				priceAudit.setModifiedColumn("PRICE", qualTXPrice.price, null);
				priceAudit.setModifiedColumn("CURRENCY_CODE", qualTXPrice.currency_code, null);
				priceAudit.setRowKeyColumns(rowKeyColumns);

				auditMap.put(altkey, priceAudit);
				audit.addChildTable(priceAudit);
			}
			else if (newRecord.modifiableRecord instanceof QualTXComponent)
			{
				QualTXComponent qualTXComponent = (QualTXComponent) newRecord.modifiableRecord;

				HashMap<String, Object> rowKeyColumns = new HashMap<String, Object>();
				rowKeyColumns.put("TX_ID", qualTXComponent.tx_id);
				rowKeyColumns.put("ORG_CODE", qualTXComponent.org_code);
				rowKeyColumns.put("COMP_ID", qualTXComponent.comp_id);

				Long altkey = qualTXComponent.alt_key_comp;
				BOMQualAuditEntity compAudit = auditMap.get(altkey);
				if (compAudit == null) compAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP", altkey);
				compAudit.setSurrogateKeyColumn("ALT_KEY_COMP");
				compAudit.setState(STATE.CREATE);
				compAudit.setModifiedColumn("AREA", qualTXComponent.area, null);
				compAudit.setModifiedColumn("AREA_UOM", qualTXComponent.area_uom, null);
				compAudit.setModifiedColumn("COMPONENT_TYPE", qualTXComponent.component_type, null);
				compAudit.setModifiedColumn("COST", qualTXComponent.cost, null);
				compAudit.setModifiedColumn("DESCRIPTION", qualTXComponent.description, null);
				compAudit.setModifiedColumn("ESSENTIAL_CHARACTER", qualTXComponent.essential_character, null);
				compAudit.setModifiedColumn("GROSS_WEIGHT", qualTXComponent.gross_weight, null);
				compAudit.setModifiedColumn("WEIGHT_UOM", qualTXComponent.weight_uom, null);
				compAudit.setModifiedColumn("PROD_KEY", qualTXComponent.prod_key, null);
				compAudit.setModifiedColumn("PROD_SRC_KEY", qualTXComponent.prod_src_key, null);
				compAudit.setModifiedColumn("MANUFACTURER_KEY", qualTXComponent.manufacturer_key, null);
				compAudit.setModifiedColumn("SELLER_KEY", qualTXComponent.seller_key, null);
				compAudit.setModifiedColumn("NET_WEIGHT", qualTXComponent.net_weight, null);
				compAudit.setModifiedColumn("UNIT_WEIGHT", qualTXComponent.unit_weight, null);
				compAudit.setModifiedColumn("QTY_PER", qualTXComponent.qty_per, null);
				compAudit.setModifiedColumn("UNIT_COST", qualTXComponent.unit_cost, null);
				compAudit.setRowKeyColumns(rowKeyColumns);
				auditMap.put(altkey, compAudit);
				audit.addChildTable(compAudit);
			}

			else if (newRecord.modifiableRecord instanceof QualTXComponentPrice)
			{
				QualTXComponentPrice qualTXCompPrice = (QualTXComponentPrice) newRecord.modifiableRecord;
				HashMap<String, Object> rowKeyColumns = new HashMap<String, Object>();
				rowKeyColumns.put("TX_ID", qualTXCompPrice.tx_id);
				rowKeyColumns.put("ORG_CODE", qualTXCompPrice.org_code);
				rowKeyColumns.put("COMP_ID", qualTXCompPrice.comp_id);
				rowKeyColumns.put("PRICE_SEQ_NUM", qualTXCompPrice.price_seq_num);

				Long altkey = qualTXCompPrice.alt_key_price;
				BOMQualAuditEntity compPriceAudit = auditMap.get(altkey);
				if (compPriceAudit == null) compPriceAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP_PRICE", altkey);
				compPriceAudit.setSurrogateKeyColumn("ALT_KEY_PRICE");
				compPriceAudit.setModifiedColumn("PRICE_TYPE", qualTXCompPrice.price_type, null);
				compPriceAudit.setModifiedColumn("PRICE", qualTXCompPrice.price, null);
				compPriceAudit.setModifiedColumn("CURRENCY_CODE", qualTXCompPrice.currency_code, null);
				compPriceAudit.setRowKeyColumns(rowKeyColumns);

				compPriceAudit.setState(STATE.CREATE);
				auditMap.put(altkey, compPriceAudit);
				audit.addChildTable(compPriceAudit);
			}

			else if (newRecord.modifiableRecord instanceof QualTXComponentDataExtension)
			{
				QualTXComponentDataExtension qualTXCompDe = (QualTXComponentDataExtension) newRecord.modifiableRecord;
				HashMap<String, Object> rowKeyColumns = new HashMap<String, Object>();
				rowKeyColumns.put("TX_ID", qualTXCompDe.tx_id);
				rowKeyColumns.put("ORG_CODE", qualTXCompDe.org_code);
				rowKeyColumns.put("COMP_ID", qualTXCompDe.comp_id);
				rowKeyColumns.put("SEQ_NUM", qualTXCompDe.seq_num);

				Long altkey = qualTXCompDe.seq_num;
				BOMQualAuditEntity compDeAudit = auditMap.get(altkey);
				String thegroupName = qualTXCompDe.group_name;
				if (compDeAudit == null) compDeAudit = new BOMQualAuditEntity("MDI_QUALTX_COMP", altkey, thegroupName.replaceAll(":", "_"), STATE.CREATE);
				compDeAudit.setSurrogateKeyColumn("SEQ_NUM");
				compDeAudit.setModifiedColumn("FLEXFIELD_VAR1", qualTXCompDe.getValue("FLEXFIELD_VAR1"), null);
				compDeAudit.setModifiedColumn("FLEXFIELD_VAR2", qualTXCompDe.getValue("FLEXFIELD_VAR2"), null);
				compDeAudit.setModifiedColumn("FLEXFIELD_VAR3", qualTXCompDe.getValue("FLEXFIELD_VAR3"), null);
				compDeAudit.setModifiedColumn("FLEXFIELD_VAR4", qualTXCompDe.getValue("FLEXFIELD_VAR4"), null);
				compDeAudit.setModifiedColumn("FLEXFIELD_VAR5", qualTXCompDe.getValue("FLEXFIELD_VAR5"), null);
				compDeAudit.setModifiedColumn("FLEXFIELD_VAR6", qualTXCompDe.getValue("FLEXFIELD_VAR6"), null);
				compDeAudit.setModifiedColumn("FLEXFIELD_VAR7", qualTXCompDe.getValue("FLEXFIELD_VAR7"), null);
				if ("IMPL_BOM_PROD_FAMILY:TEXTILES".equalsIgnoreCase(thegroupName))
				{
					compDeAudit.setModifiedColumn("FLEXFIELD_NUM1", qualTXCompDe.getValue("FLEXFIELD_NUM1"), null);
				}
				if ("QUALTX:COMP_DTLS".equalsIgnoreCase(thegroupName))
				{
					compDeAudit.setModifiedColumn("FLEXFIELD_VAR8", qualTXCompDe.getValue("FLEXFIELD_VAR8"), null);
					compDeAudit.setModifiedColumn("FLEXFIELD_VAR9", qualTXCompDe.getValue("FLEXFIELD_VAR9"), null);
					compDeAudit.setModifiedColumn("FLEXFIELD_VAR10", qualTXCompDe.getValue("FLEXFIELD_VAR10"), null);
					compDeAudit.setModifiedColumn("FLEXFIELD_VAR11", qualTXCompDe.getValue("FLEXFIELD_VAR11"), null);
					compDeAudit.setModifiedColumn("FLEXFIELD_VAR12", qualTXCompDe.getValue("FLEXFIELD_VAR12"), null);
					compDeAudit.setModifiedColumn("FLEXFIELD_VAR13", qualTXCompDe.getValue("FLEXFIELD_VAR13"), null);
					compDeAudit.setModifiedColumn("FLEXFIELD_VAR14", qualTXCompDe.getValue("FLEXFIELD_VAR14"), null);
					compDeAudit.setModifiedColumn("FLEXFIELD_VAR15", qualTXCompDe.getValue("FLEXFIELD_VAR15"), null);
					compDeAudit.setModifiedColumn("FLEXFIELD_VAR16", qualTXCompDe.getValue("FLEXFIELD_VAR16"), null);
					compDeAudit.setModifiedColumn("FLEXFIELD_VAR17", qualTXCompDe.getValue("FLEXFIELD_VAR17"), null);
					compDeAudit.setModifiedColumn("FLEXFIELD_NOTE1", qualTXCompDe.getValue("FLEXFIELD_NOTE1"), null);
				}
				compDeAudit.setRowKeyColumns(rowKeyColumns);
				compDeAudit.setState(STATE.CREATE);
				auditMap.put(altkey, compDeAudit);
				audit.addChildTable(compDeAudit);
			}

		}

		return audit;
	}
}
	
	
	
