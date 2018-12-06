package com.ambr.gtm.fta.qps.util;

import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.qps.bom.BOMComponent;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVA;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAProductSourceContainer;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponent;
import com.ambr.gtm.fta.qts.util.Env;
import com.ambr.gtm.utils.legacy.sps.SimplePropertySheet;
import com.ambr.gtm.utils.legacy.sps.SimplePropertySheetManager;
import com.ambr.gtm.utils.legacy.sps.exception.PropertyDoesNotExistException;
import com.ambr.platform.utils.log.MessageFormatter;

public  class DetermineComponentCOO 
{
	static Logger		logger = LogManager.getLogger(DetermineComponentCOO.class);
	
	public String determineCOOForComponentSource(QualTXComponent qualtxComp,BOMComponent bomComponent, GPMSourceIVAProductSourceContainer prodSourceContainer, SimplePropertySheetManager propertySheetManager) throws Exception
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
					break;			
			}
			
			else if ("BOM_COMP_COO".equals(cooOrder))
			{
				aCOO=getBOMCompCOO(bomComponent);
				if ( aCOO!= null && !"".equals(aCOO))
					break;			
			}
			else if ("GPM_COO".equals(cooOrder))
			{
				aCOO=getGPMCOO(qualtxComp);
				if (!"".equals(aCOO) && aCOO!= null)
					break;			
			}
			else if ("BOM_COMP_MANUFACTURER_COO".equals(cooOrder))
			{
				aCOO=getBOMCompManufacturerCOO(qualtxComp);
				if (!"".equals(aCOO) && aCOO!= null)
					break;			
			}
			
	      }
		if (aCOO == null || "".equals(aCOO))
		{
			aCOO=getBOMCompCOO(bomComponent);
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
	public String getGPMCOO(QualTXComponent qualtxComp) throws Exception
	{
			// Bug Fix - 64834 - Performance - BOM Qualification with 10K
			// components.
			// TA-46676 - CLONE - COO determination of the component is not
			// taken up as per hierarchy when BOM goes thru raw material
			// approach
			if (qualtxComp.ctry_of_origin == null || qualtxComp.ctry_of_origin .isEmpty())
			{
					return getGPMCOO(qualtxComp.org_code,qualtxComp.prod_key,qualtxComp.prod_src_key,qualtxComp.qualTX.ctry_of_import);
			}
			else
				return qualtxComp.ctry_of_origin;
	}
	
	public  String getGPMCOO(String org_code,Long prod_key, Long prod_src_key, String ctryOfImport) throws Exception
	{
		
		//TODO Binaya web call is not acceptable in this case.
		return  Env.getSingleton().getTradeQualtxClient().getGPMCOO(org_code,prod_key, prod_src_key, ctryOfImport);
	}
}
