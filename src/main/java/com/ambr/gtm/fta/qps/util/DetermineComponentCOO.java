package com.ambr.gtm.fta.qps.util;

import java.sql.SQLException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.qps.bom.BOMComponent;
import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationProductContainer;
import com.ambr.gtm.fta.qps.gpmclass.GPMCountry;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVA;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAProductSourceContainer;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponent;
import com.ambr.gtm.utils.legacy.sps.SimplePropertySheet;
import com.ambr.gtm.utils.legacy.sps.SimplePropertySheetManager;
import com.ambr.gtm.utils.legacy.sps.exception.PropertyDoesNotExistException;
import com.ambr.platform.utils.log.MessageFormatter;

public  class DetermineComponentCOO 
{
	static Logger		logger = LogManager.getLogger(DetermineComponentCOO.class);
	
	public String determineCOOForComponentSource(QualTXComponent qualtxComp,BOMComponent bomComponent, GPMSourceIVAProductSourceContainer prodSourceContainer, GPMClassificationProductContainer gpmClassContainer, SimplePropertySheetManager propertySheetManager) throws Exception
	{
		String aCOO = "";

		SimplePropertySheet aSPS = null;
		try
		{
			aSPS = propertySheetManager.getPropertySheet(qualtxComp.org_code, "BOM_SCREENING_CONFIG");
		}
		catch (Exception theException)
		{
			MessageFormatter.error(logger, "determineCOOForComponentSource", theException, "Org Code [{0}]: Error while reading the property sheet [{1}]", qualtxComp.org_code, "BOM_SCREENING_CONFIG");
		}
		if (aSPS == null) return null;
		
		GPMSourceIVA gpmSourceIVA = QualTXUtility.getGPMIVARecord(prodSourceContainer,qualtxComp.qualTX.fta_code, qualtxComp.qualTX.ctry_of_import,qualtxComp.qualTX.effective_from, qualtxComp.qualTX.effective_to); 
		
		String aCOOHierchyOrder= null;
		
		try
		{
			aCOOHierchyOrder = aSPS.getStringValue("COO_DETERMINATION_HIERARCHY");
		}
		catch (PropertyDoesNotExistException p)
		{
			aCOOHierchyOrder = null;
		}
		
		if (aCOOHierchyOrder == null || aCOOHierchyOrder.isEmpty()) return aCOO;
		String [] aCOOOrder = aCOOHierchyOrder.split(",");
		for (String cooOrder: aCOOOrder )
		{
			if ("STP_COO".equals(cooOrder))
			{
				aCOO=getSTPCOO(qualtxComp,gpmSourceIVA);
				if (aCOO!= null && !aCOO.isEmpty())
				{
					qualtxComp.coo_source = 0;
					break;	
				}
			}
			
			else if ("BOM_COMP_COO".equals(cooOrder))
			{
				aCOO=getBOMCompCOO(bomComponent);
				if ( aCOO!= null && !"".equals(aCOO))
				{
					qualtxComp.coo_source = 2;
					break;	
				}
							
			}
			else if ("GPM_COO".equals(cooOrder))
			{
				aCOO = getGPMCOO(qualtxComp, prodSourceContainer, gpmClassContainer);
				if (!"".equals(aCOO) && aCOO!= null)
				{
					qualtxComp.coo_source = 1;
					break;
				}
			}
			else if ("BOM_COMP_MANUFACTURER_COO".equals(cooOrder))
			{
				aCOO=getBOMCompManufacturerCOO(qualtxComp);
				if (!"".equals(aCOO) && aCOO!= null)
				{
					qualtxComp.coo_source = 3;
					break;
				}
			}
			
	      }
		if (aCOO == null || "".equals(aCOO))
		{
			aCOO=getBOMCompCOO(bomComponent);
			qualtxComp.coo_source = 2;
		}
		return aCOO;
	}

	public String getSTPCOO(QualTXComponent qualtxComp,GPMSourceIVA gpmSourceIVA) throws SQLException
	{
		if (gpmSourceIVA != null)
		{
			String aCtryOfOrigin = gpmSourceIVA.ctryOfOrigin;
			if (aCtryOfOrigin != null)
				return aCtryOfOrigin;
		}
		return null;
	}
	public String getBOMCompCOO(BOMComponent bomComponent) throws Exception
	{
		return bomComponent.ctry_of_origin;
		
	}
	public String getBOMCompManufacturerCOO(QualTXComponent theBOMComponent) throws Exception
	{
		return theBOMComponent.ctry_of_manufacture;
	}
	public String getGPMCOO(QualTXComponent qualtxComp, GPMSourceIVAProductSourceContainer prodSourceContainer, GPMClassificationProductContainer gpmClassContainer) throws Exception
	{
		String aGPMCOO = null;
		//Get COO from Source level
		if (prodSourceContainer != null)aGPMCOO = prodSourceContainer.ctryOfOrigin;

		if (aGPMCOO == null && gpmClassContainer != null)
		{
			//Get COO from country level
			GPMCountry aGPMCountry = getGPMCountry(gpmClassContainer.ctryList, qualtxComp.qualTX.ctry_of_import);
			aGPMCOO = aGPMCountry != null ? aGPMCountry.ctryOfOrigin : null;
			
			//Get COO at GPM header level
			if (aGPMCOO == null) aGPMCOO = gpmClassContainer.ctryOfOrigin;
		}
		return aGPMCOO;

	}
	
	public GPMCountry getGPMCountry(List<GPMCountry> aCtryList, String aCOI)
	{
		GPMCountry aGPMCountry = null;
		if (aCtryList != null && aCOI != null)
		{
			for (GPMCountry gpmCountry : aCtryList)
			{
				if (gpmCountry.ctryCode.equals(aCOI))
				{
					aGPMCountry = gpmCountry;
					break;
				}
			}
		}
		return aGPMCountry;
	}
}
