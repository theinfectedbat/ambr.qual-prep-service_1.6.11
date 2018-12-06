package com.ambr.gtm.fta.qts.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

public class CumulationConfigContainer 
{
	
	private Map<String,List<CumulationRule>> cumulationRuleMap = new HashMap<>();
	private Map<String,ComponentAgreement> ComponetAgreementMap = new HashMap<>();
	
	public List<CumulationRule> getCumulationRuleConfigByFTA(String ftaCode)
	{
		if(cumulationRuleMap.isEmpty())
			return null;
		return cumulationRuleMap.get(ftaCode);
	}
	
	public ComponentAgreement getComponentAgreementConfigByFTACOI(String ftaCode,String coi)
	{
		if(ftaCode == null || ftaCode.isEmpty() || coi == null || coi.isEmpty())
			return null;
		
		if(ComponetAgreementMap.isEmpty())
			return null;
		
		return ComponetAgreementMap.get(ftaCode+"#"+coi);
	}
	
	private void addCumulationRule(String srcFTACode, CumulationRule cumulationRule) {
		List<CumulationRule> cumulationList = this.cumulationRuleMap.get(srcFTACode);
		if(cumulationList == null)
		{
			cumulationList = new ArrayList<>();
			this.cumulationRuleMap.put(srcFTACode,cumulationList);
		}
		cumulationList.add(cumulationRule);
	}
	
	private void addComponentAgreement(String ftaCodeAndCoi, ComponentAgreement componentAgreement) {
		this.ComponetAgreementMap.put(ftaCodeAndCoi,componentAgreement);
	}
	
	public void setCumulationConfiguration(JsonNode cumulationConfigJson) 
	{
		if(cumulationConfigJson != null && cumulationConfigJson.isArray())
		{
			for(JsonNode cumuationRule : cumulationConfigJson)
			{
				CumulationRule	cumulationRule =  new CumulationRule();	
				
				String srcFTACode = "";
				if(cumuationRule.has("SRC_FTA_CODE"))
					srcFTACode = cumuationRule.get("SRC_FTA_CODE").asText("");
				cumulationRule.setSourceFTACode(srcFTACode);
				
				String coiListStr = "";
				if(cumuationRule.has("COI_LIST"))
					coiListStr = cumuationRule.get("COI_LIST").asText("");
				
				List<String> coiList = new ArrayList<>();
				if(!"".equals(coiListStr))
				{
					String[] ctryArray = coiListStr.split(";");
					for(String ctry : ctryArray)
						coiList.add(ctry.trim());
				}
				cumulationRule.setCountryOfImportList(coiList);
				
				String comListStr = "";
				if(cumuationRule.has("COM_LIST"))
					comListStr = cumuationRule.get("COM_LIST").asText("");
				
				List<String> comList = new ArrayList<>();
				if(!"".equals(comListStr))
				{
					String[] ctryArray = comListStr.split(";");
					for(String ctry : ctryArray)
						comList.add(ctry.trim());
				}
				cumulationRule.setCountryOfManufactureList(comList);
				
				String destFTACodeLstStr = "";
				if(cumuationRule.has("DEST_FTA_CODE"))
					destFTACodeLstStr =  cumuationRule.get("DEST_FTA_CODE").asText("");
				
				List<String> destFTAList = new ArrayList<>();
				if(!"".equals(destFTACodeLstStr))
				{
					String[] ftaArray = destFTACodeLstStr.split(";");
					for(String fta : ftaArray)
						destFTAList.add(fta.trim());
				}
				cumulationRule.setDestinationFTAList(destFTAList);
				
				boolean useCOOList = false;
				if(cumuationRule.has("USE_COO_LIST"))
				{
					String useCOO =  cumuationRule.get("USE_COO_LIST").asText("");
					if(!"".equals(useCOO) && ("Y".equalsIgnoreCase(useCOO) || "YES".equalsIgnoreCase(useCOO)))
							useCOOList = true;
					else if(!"".equals(useCOO) && ("N".equalsIgnoreCase(useCOO) || "NO".equalsIgnoreCase(useCOO)))
						useCOOList = false;
					
				}
				
				cumulationRule.setUseCOOList(useCOOList);
				
				String cooListStr = "";
				if(cumuationRule.has("COO_LIST"))
					cooListStr = cumuationRule.get("COO_LIST").asText("");
				
				List<String> cooList = new ArrayList<>();
				if(!"".equals(cooListStr))
				{
					String[] ctryArray = cooListStr.split(";");
					for(String ctry : ctryArray)
						cooList.add(ctry.trim());
				}
				cumulationRule.setCountryOfOriginList(cooList);
				
				String hsExcpListStr = "";
				if(cumuationRule.has("HS_EXCP_LIST"))
					hsExcpListStr = cumuationRule.get("HS_EXCP_LIST").asText("");
				
				List<String> hsExcpList = new ArrayList<>();
				if(!"".equals(hsExcpListStr))
				{
					String[] hsExcpArr = hsExcpListStr.split(";");
					for(String hsExp : hsExcpArr)
						hsExcpList.add(hsExp.trim());
				}
				cumulationRule.setHsExceptionList(hsExcpList);
				
				String populateCumCOO = "";
				if(cumuationRule.has("POPULATE_CUM_COO"))
					populateCumCOO = cumuationRule.get("POPULATE_CUM_COO").asText("");	
				cumulationRule.setPopulateCumulationCountry(populateCumCOO);
				
				addCumulationRule(srcFTACode,cumulationRule);
			}
		}
	}
	public void setComponentAgreementConfig(JsonNode componentAgreementConfigJson) 
	{
		if(componentAgreementConfigJson != null && componentAgreementConfigJson.isArray())
		{
			for(JsonNode componentAgrConfig : componentAgreementConfigJson)
			{
				ComponentAgreement componentAgreement = new ComponentAgreement();
				
				String ftaCode = "";
				if(componentAgrConfig.has("FTA_CODE"))
					ftaCode = componentAgrConfig.get("FTA_CODE").asText("");
				componentAgreement.setFtaCode(ftaCode);
				
				String ftaCoi = "";
				if(componentAgrConfig.has("FTA_COI"))
					ftaCoi = componentAgrConfig.get("FTA_COI").asText("");
				componentAgreement.setFtaCoi(ftaCoi);
				
				String ctryOfImport = "";
				if(componentAgrConfig.has("CTRY_OF_IMPORT"))
					ctryOfImport = componentAgrConfig.get("CTRY_OF_IMPORT").asText("");
				componentAgreement.setCtryOfImport(ctryOfImport);
				
				addComponentAgreement(ftaCode+"#"+ftaCoi,componentAgreement);
			}
		}
	}

	public class ComponentAgreement
	{
		private String ftaCode;
		private String ftaCoi;
		private String ctryOfImport;
		
		public String getFtaCode() {
			return ftaCode;
		}
		public void setFtaCode(String ftaCode) {
			this.ftaCode = ftaCode;
		}
		public String getFtaCoi() {
			return ftaCoi;
		}
		public void setFtaCoi(String ftaCoi) {
			this.ftaCoi = ftaCoi;
		}
		public String getCtryOfImport() {
			return ctryOfImport;
		}
		public void setCtryOfImport(String ctryOfImport) {
			this.ctryOfImport = ctryOfImport;
		}
	}
	public class CumulationRule
	{
		String srcFTACode = "";
		List<String> coiList = new ArrayList<>();
		List<String> comList = new ArrayList<>();
		List<String> destFTAList = new ArrayList<>();
		boolean useCOOList;
		List<String> cooList = new ArrayList<>();
		List<String> hsExcpList = new ArrayList<>();
		String populateCumCOO = "";
		
		public String getSoureFTACode() {
			return srcFTACode;
		}
		public void setSourceFTACode(String srcFTACode) {
			this.srcFTACode = srcFTACode;
		}
		public List<String> getCountryOfImportList() {
			return coiList;
		}
		public void setCountryOfImportList(List<String> coiList) {
			this.coiList = coiList;
		}
		public List<String> getCountryOfManufactureList() {
			return comList;
		}
		public void setCountryOfManufactureList(List<String> comList) {
			this.comList = comList;
		}
		public List<String> getDestinationFTAList() {
			return destFTAList;
		}
		public void setDestinationFTAList(List<String> destFTAList) {
			this.destFTAList = destFTAList;
		}
		public boolean useCOOList() {
			return useCOOList;
		}
		public void setUseCOOList(boolean useCOOList) {
			this.useCOOList = useCOOList;
		}
		public List<String> getCountryOfOriginList() {
			return cooList;
		}
		public void setCountryOfOriginList(List<String> cooList) {
			this.cooList = cooList;
		}
		public List<String> getHsExceptionList() {
			return hsExcpList;
		}
		public void setHsExceptionList(List<String> hsExcpList) {
			this.hsExcpList = hsExcpList;
		}
		public String getPopulateCumulationCountry() {
			return populateCumCOO;
		}
		public void setPopulateCumulationCountry(String populateCumCOO) {
			this.populateCumCOO = populateCumCOO;
		}
	}
}
