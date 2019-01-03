package com.ambr.gtm.fta.qps.gpmclaimdetail;

import java.text.MessageFormat;
import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GPMClaimDetailsSourceIVAContainer 
{
	public long 						prodSrcIVAKey;
	public ArrayList<GPMClaimDetails>	claimDetailList;
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theProdSrcIVAKey
     *************************************************************************************
     */
	public GPMClaimDetailsSourceIVAContainer(long theProdSrcIVAKey)
		throws Exception
	{
		this.prodSrcIVAKey = theProdSrcIVAKey;
		this.claimDetailList = new ArrayList<>();
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theClaimDetails
     *************************************************************************************
     */
	void addClaimDetails(GPMClaimDetails theClaimDetails)
		throws Exception
	{
		this.claimDetailList.add(theClaimDetails);
	}
	
    /**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theParentClaimDetails
	 *************************************************************************************
	 */
	@JsonIgnore
	public ArrayList<GPMClaimDetails> getChildRecords(GPMClaimDetails theParentClaimDetails)
		throws Exception
	{
		ArrayList<GPMClaimDetails>	aList = new ArrayList<>();
		
		if (theParentClaimDetails == null) {
			throw new IllegalArgumentException(MessageFormat.format("Prod Src IVA [{0}]: Parent Claim Details must be specified", this.prodSrcIVAKey));
		}
		
		for (GPMClaimDetails aClaimDetails : this.claimDetailList) {
			if (theParentClaimDetails.seq_num == aClaimDetails.parent_seq_num) {
				aList.add(aClaimDetails);
			}
		}
		
		return aList;
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	@JsonIgnore
	public String getFTACodeGroup()
		throws Exception
	{
		if (this.claimDetailList.size() == 0) {
			return null;
		}
		
		return this.claimDetailList.get(0).fta_code_group;
	}
	
    /**
     *************************************************************************************
     * <P>
     * Returns the "root" claim details record for this GPM Source IVA instance.
     * </P>
     *************************************************************************************
     */
	@JsonIgnore
	public GPMClaimDetails getPrimaryClaimDetails()
		throws Exception
	{
		if (this.claimDetailList.size() == 0) {
			return null;
		}

		for (GPMClaimDetails aClaimDetails : this.claimDetailList) {
			if (aClaimDetails.parent_seq_num == 0) {
				return aClaimDetails;
			}
		}
		
		// This should never happen, but if it does, we will simply return the first
		// entry in the list
		return this.claimDetailList.get(0);
	}
}
