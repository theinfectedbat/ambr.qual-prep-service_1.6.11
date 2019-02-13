package com.ambr.gtm.fta.qts.requal;

public class RequalificationWorkCodes
{
	// AR_QTX_WORK REASON CODES
	public static final long	BOM_HDR_CHG						= 0b1;

	public static final long	BOM_PROD_TXT_DE					= 0b10;

	public static final long	BOM_PROD_AUTO_DE				= 0b100;

	public static final long	BOM_QUAL_MPQ_CHG				= 0b1000;

	public static final long	BOM_PRC_CHG						= 0b10000;

	public static final long	GPM_IVA_AND_CLAIM_DTLS_CHANGE	= 0b100000;

	public static final long	GPM_NEW_HEADER_IVA_IDENTIFED	= 0b1000000;

	public static final long	CONTENT_ROO_CHANGE				= 0b10000000;

	public static final long	BOM_TXREF_CHG					= 0b100000000;
	
	public static final long	GPM_IVA_CHANGE_M_I				= 0b1000000000;

	// AR_QTX_COMP_WORK REASON CODES
	public static final long	BOM_COMP_ADDED					= 0b1;

	public static final long	BOM_COMP_DELETED				= 0b10;

	public static final long	BOM_COMP_MODIFIED				= 0b100;

	public static final long	BOM_COMP_YARN_DTLS_CHG			= 0b1000;

	public static final long	GPM_NEW_IVA_IDENTIFED			= 0b10000;
	
	public static final long	COMP_PRC_CHG					= 0b1000000;

	// AR_QTX_COMP_WORK_IVA REASON CODES
	public static final long	BOM_COMP_SRC_CHG				= 0b1;

	public static final long	GPM_SRC_IVA_DELETED				= 0b10;

	public static final long	GPM_SRC_CHANGE					= 0b100;

	public static final long	GPM_SRC_DELETED					= 0b1000;

	public static final long	GPM_SRC_ADDED					= 0b10000;
	
	public static final long	GPM_COMP_FINAL_DECISION_CHANGE	= 0b100000;

	// AR_QTX_COMP_WORK_HS REASON CODES

	public static final long	GPM_CTRY_CMPL_CHANGE			= 0b1;

	public static final long	GPM_CTRY_CMPL_DELETED			= 0b10;
	
	public static final long	GPM_CTRY_CMPL_ADDED			    = 0b100;
	
	
	private long[] qtxWorkArray = {BOM_HDR_CHG, BOM_PROD_TXT_DE, BOM_PROD_AUTO_DE, BOM_QUAL_MPQ_CHG, BOM_PRC_CHG, GPM_IVA_AND_CLAIM_DTLS_CHANGE,GPM_NEW_HEADER_IVA_IDENTIFED, CONTENT_ROO_CHANGE, BOM_TXREF_CHG};

}
