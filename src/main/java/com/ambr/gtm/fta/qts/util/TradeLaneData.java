package com.ambr.gtm.fta.qts.util;

public class TradeLaneData
{
	private boolean requalification;
	private boolean annulaqualification;
	private String annualQualificationDate;
	private boolean userPriviousYearQualification;
	private String usePrevYearQualUpto;
	private boolean rawMaterialAnalysisMethod;
	private boolean intermediateAnalysisMethod;
	private boolean populateMaterialAttributes;
	private boolean useNonOriginatingMaterials;
	private boolean applyRvcRestriction;
	private boolean rollupComponents;
	private String ftaCode;
	private String ctryOfImport;

	public String getFtaCode()
	{
		return ftaCode;
	}

	public String getCtryOfImport()
	{
		return ctryOfImport;
	}

	
	public boolean isRequalification() {
		return requalification;
	}

	public void setRequalification(boolean requalification) {
		this.requalification = requalification;
	}

	public boolean isAnnulaqualification() {
		return annulaqualification;
	}

	public void setAnnulaqualification(boolean annulaqualification) {
		this.annulaqualification = annulaqualification;
	}

	public String getAnnualQualificationDate() {
		return annualQualificationDate;
	}

	public void setAnnualQualificationDate(String annualQualificationDate) {
		this.annualQualificationDate = annualQualificationDate;
	}

	public boolean isUserPriviousYearQualification() {
		return userPriviousYearQualification;
	}

	public void setUserPriviousYearQualification(boolean userPriviousYearQualification) {
		this.userPriviousYearQualification = userPriviousYearQualification;
	}

	public String getUsePrevYearQualUpto() {
		return usePrevYearQualUpto;
	}

	public void setUsePrevYearQualUpto(String usePrevYearQualUpto) {
		this.usePrevYearQualUpto = usePrevYearQualUpto;
	}

	public boolean isRawMaterialAnalysisMethod() {
		return rawMaterialAnalysisMethod;
	}

	public void setRawMaterialAnalysisMethod(boolean rawMaterialAnalysisMethod) {
		this.rawMaterialAnalysisMethod = rawMaterialAnalysisMethod;
	}

	public boolean isIntermediateAnalysisMethod() {
		return intermediateAnalysisMethod;
	}

	public void setIntermediateAnalysisMethod(boolean intermediateAnalysisMethod) {
		this.intermediateAnalysisMethod = intermediateAnalysisMethod;
	}

	public boolean isPopulateMaterialAttributes() {
		return populateMaterialAttributes;
	}

	public void setPopulateMaterialAttributes(boolean populateMaterialAttributes) {
		this.populateMaterialAttributes = populateMaterialAttributes;
	}

	public boolean isUseNonOriginatingMaterials() {
		return useNonOriginatingMaterials;
	}

	public void setUseNonOriginatingMaterials(boolean useNonOriginatingMaterials) {
		this.useNonOriginatingMaterials = useNonOriginatingMaterials;
	}

	public boolean isApplyRvcRestriction() {
		return applyRvcRestriction;
	}

	public void setApplyRvcRestriction(boolean applyRvcRestriction) {
		this.applyRvcRestriction = applyRvcRestriction;
	}

	public boolean isRollupComponents() {
		return rollupComponents;
	}

	public void setRollupComponents(boolean rollupComponents) {
		this.rollupComponents = rollupComponents;
	}

	public void setFtaCode(String ftaCode) {
		this.ftaCode = ftaCode;
	}

	public void setCtryOfImport(String ctryOfImport) {
		this.ctryOfImport = ctryOfImport;
	}
}
