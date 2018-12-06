package com.ambr.gtm.fta.qps.bom;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import javax.persistence.Column;
import javax.persistence.Table;

import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfiguration;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfigurationRepository;
import com.ambr.platform.rdbms.util.DataRecordUtility;
import com.ambr.platform.rdbms.util.bdrp.DataRecordValueMapInterface;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@Table(name = "mdi_bom_de")
public class BOMDataExtension 
	implements Serializable, DataRecordValueMapInterface
{
	private static final long serialVersionUID = 1L;

	@Column(name = "alt_key_bom") 		public long		alt_key_bom;
	@Column(name = "group_name") 		public String	group_name;
	@Column(name = "parent_seq_num") 	public long		parent_seq_num;
	
	public HashMap<String, Object>					deFieldMap;
	public HashMap<String, Timestamp>				deDateFieldMap;
	private DataExtensionConfigurationRepository	repos;
	private transient BOM 							bom;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public BOMDataExtension()
		throws Exception
	{
		this.deFieldMap = new HashMap<>();
		this.deDateFieldMap = new HashMap<>();
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theGroupName
     * @param	theRepos
     *************************************************************************************
     */
	public BOMDataExtension(String theGroupName, DataExtensionConfigurationRepository theRepos)
		throws Exception
	{
		this();
		this.group_name = theGroupName;
		this.repos = theRepos;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * Returns the physical column names for the target DE
	 * </P>
	 *************************************************************************************
	 */
	@JsonIgnore
	@Override
	public ArrayList<String> getColumnNames() 
		throws Exception 
	{
		DataExtensionConfiguration	aCfg;
		ArrayList<String>			aList;
		
		if (this.repos == null) {
			aList = new ArrayList<>(new DataRecordUtility<>(BOMDataExtension.class, true).getColumnNames());
			this.deFieldMap.keySet().forEach(aKey->{aList.add(aKey);});
			this.deDateFieldMap.keySet().forEach(aKey->{aList.add(aKey);});
		}
		else {
			aCfg = this.repos.getDataExtensionConfiguration(this.group_name);
			aList = aCfg.getPhysicalColumnNames();
			aList.addAll(new DataRecordUtility<BOMDataExtension>(BOMDataExtension.class, true).getColumnNames());
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
	@Override
	public String getDescription() {
		return "BOM." + this.alt_key_bom;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * Returns the value for the specified physical column name
	 * </P>
	 *************************************************************************************
	 */
	@JsonIgnore
	@Override
	public Object getValue(String theColumnName) 
		throws Exception 
	{
		if (theColumnName == null) {
			throw new IllegalArgumentException(MessageFormat.format("BOM [{0}]: column name must be specified", this.alt_key_bom));
		}
		
		theColumnName = theColumnName.toUpperCase();
		
		if (theColumnName.equalsIgnoreCase("alt_key_bom")) {
			return this.alt_key_bom;
		}
		else if (theColumnName.equalsIgnoreCase("group_name")) {
			return this.group_name;
		}
		else if (theColumnName.equalsIgnoreCase("parent_seq_num")) {
			return this.parent_seq_num;
		}
		else {
			Object aValue;
			
			aValue = this.deFieldMap.get(theColumnName);
			if (aValue == null) {
				aValue = this.deDateFieldMap.get(theColumnName);
			}
			
			return aValue;
		}
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	@JsonIgnore
	void setBOM(BOM theBOM)
		throws Exception
	{
		this.bom = theBOM;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theRepos
	 *************************************************************************************
	 */
	@JsonIgnore
	void setRepository(DataExtensionConfigurationRepository theRepos)
		throws Exception
	{
		this.repos = theRepos;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theColumnName
	 * @param	theValue
	 *************************************************************************************
	 */
	@JsonIgnore
	@Override
	public void setValue(String theColumnName, Object theValue) 
		throws Exception 
	{
		theColumnName = theColumnName.toUpperCase();
		
		if (theValue == null) {
			this.deFieldMap.remove(theColumnName);
			this.deDateFieldMap.remove(theColumnName);
		}
		else {
			if (theValue instanceof Date) {
				if (theValue instanceof Timestamp) {
					this.deDateFieldMap.put(theColumnName, (Timestamp)theValue);
				}
				else {
					this.deDateFieldMap.put(theColumnName, new Timestamp(((Date)theValue).getTime()));
				}
			}
			else {
				this.deFieldMap.put(theColumnName, theValue);
			}
		}
		
		if ("alt_key_bom".equalsIgnoreCase(theColumnName)) {
			this.alt_key_bom = ((Number)theValue).longValue();
		}
		else if ("parent_seq_num".equalsIgnoreCase(theColumnName)) {
			this.parent_seq_num = ((Number)theValue).longValue();
		}
	}
}
