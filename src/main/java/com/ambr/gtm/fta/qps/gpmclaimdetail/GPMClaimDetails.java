package com.ambr.gtm.fta.qps.gpmclaimdetail;

import java.io.Serializable;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Table;

import com.ambr.platform.rdbms.util.ResultSetUtility;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@Table(name = "mdi_ivainst_join_mdi_ivainst_de")
public class GPMClaimDetails 
	implements Serializable
{
	@Column(name = "alt_key_ivainst") 			public long 		alt_key_ivainst;
	@Column(name = "record_key") 				public long			prodSrcIVAKey;
	@Column(name = "fta_code_group")			public String 		fta_code_group;
	@Column(name = "seq_num")					public long 		seq_num;
	@Column(name = "parent_seq_num")			public long 		parent_seq_num;

	private GPMClaimDetailsUniversePartition	partition;
	public Map<String,Object> 					claimDetailsValue = new HashMap<>();
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GPMClaimDetails()
		throws Exception
	{
		this.claimDetailsValue = new HashMap<>();
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	thePartition
     *************************************************************************************
     */
	public GPMClaimDetails(GPMClaimDetailsUniversePartition thePartition)
		throws Exception
	{
		this.partition = thePartition;
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public ArrayList<String> getColumnNames()
		throws Exception
	{
		return new ArrayList<>(this.claimDetailsValue.keySet());
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theColumnName
     *************************************************************************************
     */
	public Object getValue(String theColumnName) 
		throws Exception 
	{
		if (theColumnName == null) {
			throw new IllegalArgumentException("Columm name must be specified");
		}
		
		return this.claimDetailsValue.get(theColumnName.toLowerCase());
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theResultSet
     *************************************************************************************
     */
	public void loadData(ResultSet theResultSet)
		throws Exception
	{
		Object	aValue;
		String	aColumnName;
		
		for (int aColIndex = 1; aColIndex <= theResultSet.getMetaData().getColumnCount(); aColIndex++) {
			aColumnName = theResultSet.getMetaData().getColumnLabel(aColIndex);
			aValue = theResultSet.getObject(aColIndex);
			if (aValue == null) {
				continue;
			}
			
			if ((aValue instanceof String) || (aValue instanceof Number)) {
				// no need to do any value conversion
			}
			else {
				if (ResultSetUtility.IsDateColumn(theResultSet, aColIndex)) {
					// since it's a data value, we need to ensure it is properly converted to a date value
					aValue = theResultSet.getTimestamp(aColIndex);
				}
			}
			
			this.claimDetailsValue.put(aColumnName.toLowerCase(), aValue);
		}
	}
}
