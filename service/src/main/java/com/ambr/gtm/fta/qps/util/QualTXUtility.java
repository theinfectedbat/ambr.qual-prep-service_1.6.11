package com.ambr.gtm.fta.qps.util;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVA;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAProductSourceContainer;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTX;
import com.ambr.gtm.fta.qps.qualtx.engine.result.TradeLaneStatusTracker;
import com.ambr.gtm.fta.qts.QtxStatusUpdateRequest;
import com.ambr.gtm.fta.qts.api.TrackerClientAPI;
import com.ambr.gtm.utils.legacy.sps.SimplePropertySheet;
import com.ambr.gtm.utils.legacy.sps.SimplePropertySheetManager;
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
}
	
	
	
