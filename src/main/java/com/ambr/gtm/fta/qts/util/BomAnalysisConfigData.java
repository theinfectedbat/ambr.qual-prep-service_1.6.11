package com.ambr.gtm.fta.qts.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BomAnalysisConfigData {
	
	private String cooFromCom;
	private double rvcMaxSalePrice;
	private double rvcLimitSafFactor;
	private String partialPeriod;
	private double rvcThreshHoldSaffactor;
	private String analysisMethod;
	private String interMediateParts;
	private String bomHeader;
	private String aggCooFromCom;
	private String rvcmethod;
	private String rvcminSalePrice;
	private String mandatoryCoo;
	private Map<String, String> ftaanalysis = new HashMap<String, String>();
	

	public String getCooFromCom() {
		return cooFromCom;
	}
	public void setCooFromCom(String cooFromCom) {
		this.cooFromCom = cooFromCom;
	}
	public double getRvcMaxSalePrice() {
		return rvcMaxSalePrice;
	}
	public void setRvcMaxSalePrice(double rvcMaxSalePrice) {
		this.rvcMaxSalePrice = rvcMaxSalePrice;
	}
	public double getRvcLimitSafFactor() {
		return rvcLimitSafFactor;
	}
	public void setRvcLimitSafFactor(double rvcLimitSafFactor) {
		this.rvcLimitSafFactor = rvcLimitSafFactor;
	}
	public String getPartialPeriod() {
		return partialPeriod;
	}
	public void setPartialPeriod(String partialPeriod) {
		this.partialPeriod = partialPeriod;
	}
	public double getRvcThreshHoldSaffactor() {
		return rvcThreshHoldSaffactor;
	}
	public void setRvcThreshHoldSaffactor(double rvcThreshHoldSaffactor) {
		this.rvcThreshHoldSaffactor = rvcThreshHoldSaffactor;
	}
	public String getAnalysisMethod() {
		return analysisMethod;
	}
	public void setAnalysisMethod(String analysisMethod) {
		this.analysisMethod = analysisMethod;
	}
	public String getInterMediateParts() {
		return interMediateParts;
	}
	public void setInterMediateParts(String interMediateParts) {
		this.interMediateParts = interMediateParts;
	}
	public String getBomHeader() {
		return bomHeader;
	}
	public void setBomHeader(String bomHeader) {
		this.bomHeader = bomHeader;
	}
	public String getAggCooFromCom() {
		return aggCooFromCom;
	}
	public void setAggCooFromCom(String aggCooFromCom) {
		this.aggCooFromCom = aggCooFromCom;
	}
	public String getRvcmethod() {
		return rvcmethod;
	}
	public void setRvcmethod(String rvcmethod) {
		this.rvcmethod = rvcmethod;
	}
	public String getRvcMinSalePrice() {
		return rvcminSalePrice;
	}
	public void setRvcMinSalePrice(String minSalePrice) {
		this.rvcminSalePrice = rvcminSalePrice;
	}
	public String getMandatoryCoo() {
		return mandatoryCoo;
	}
	public void setMandatoryCoo(String mandatoryCoo) {
		this.mandatoryCoo = mandatoryCoo;
	}
	public Map<String, String> getFtaanalysis() {
		return ftaanalysis;
	}
	public void setFtaanalysis(Map<String, String> ftaanalysis) {
		this.ftaanalysis = ftaanalysis;
	}
	
}
