package com.ambr.gtm.fta.qps.qualtx.engine;

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
import com.ambr.platform.uoid.UniversalObjectIdentifier;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@Table(name = "mdi_qualtx_de")
public class QualTXDataExtension 
	implements Serializable, DataRecordValueMapInterface
{
	private static final long serialVersionUID = 1L;

	@Column(name = "alt_key_qualtx")		public long		alt_key_qualtx;
	@Column(name = "created_by") 			public String	created_by;
	@Column(name = "created_date") 			public Date		created_date;
	@Column(name = "group_name") 			public String	group_name;
	@Column(name = "last_modified_by") 		public String	last_modified_by;
	@Column(name = "last_modified_date")	public Date		last_modified_date;
	@Column(name = "org_code") 				public String	org_code;
	@Column(name = "parent_seq_num") 		public long		parent_seq_num;
	@Column(name = "tx_id") 				public String	tx_id;
	@Column(name = "seq_num") 				public long		seq_num;
	
	private HashMap<String, Object>					deFieldMap;
	private HashMap<String, Timestamp>				deDateFieldMap;
	private DataExtensionConfigurationRepository	repos;
	private transient QualTX						qualTX;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public QualTXDataExtension()
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
	public QualTXDataExtension(String theGroupName, DataExtensionConfigurationRepository theRepos)
		throws Exception
	{
		this.group_name = theGroupName;
		this.repos = theRepos;
		this.deFieldMap = new HashMap<>();
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theMSBFlag		Indicates whether to use the MSB portion of the UOID.  If false,
	 * 							the LSB portion will be used.
	 *************************************************************************************
	 */
	void generateNewKeys(boolean theMSBFlag)
		throws Exception
	{
		UniversalObjectIdentifier	aID;
		
		if (this.qualTX == null) {
			throw new IllegalStateException(MessageFormat.format("Qual TX DE [{0}]: QualTX object not initialized", this.seq_num));
		}

		if (this.qualTX.idGenerator == null) {
			throw new IllegalStateException(MessageFormat.format("Qual TX [{0}] DE [{1}]: ID generator not initialized", this.alt_key_qualtx, this.seq_num));
		}
		
		aID = this.qualTX.idGenerator.generate();
		this.alt_key_qualtx = this.qualTX.alt_key_qualtx;
		this.seq_num = theMSBFlag? aID.getMSB() : aID.getLSB();
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
			aList = new ArrayList<>(new DataRecordUtility<>(QualTXDataExtension.class, true).getColumnNames());
			this.deFieldMap.keySet().forEach(aKey->{aList.add(aKey);});
			this.deDateFieldMap.keySet().forEach(aKey->{aList.add(aKey);});
		}
		else {
			aCfg = this.repos.getDataExtensionConfiguration(this.group_name);
			aList = aCfg.getPhysicalColumnNames();
			aList.addAll(new DataRecordUtility<>(QualTXDataExtension.class, true).getColumnNames());
		}
		
		HashMap<String, String>	aColumnNameMap = new HashMap<>();
		aList.forEach(aColumnName->{aColumnNameMap.put(aColumnName.toUpperCase(), aColumnName.toUpperCase());});
		aList.clear();
		aList.addAll(aColumnNameMap.values());

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
		return "QUALTX." + this.alt_key_qualtx;
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
			throw new IllegalArgumentException(MessageFormat.format("QTX [{0}]: column name must be specified", this.alt_key_qualtx));
		}
		
		theColumnName = theColumnName.toUpperCase();
		
		if 		(theColumnName.equalsIgnoreCase("alt_key_qualtx")) 		{return this.alt_key_qualtx;}
		else if (theColumnName.equalsIgnoreCase("created_by")) 			{return this.created_by;}
		else if (theColumnName.equalsIgnoreCase("created_date")) 		{return this.created_date;}
		else if (theColumnName.equalsIgnoreCase("group_name")) 			{return this.group_name;}
		else if (theColumnName.equalsIgnoreCase("last_modified_by"))	{return this.last_modified_by;}
		else if (theColumnName.equalsIgnoreCase("last_modified_date")) 	{return this.last_modified_date;}
		else if (theColumnName.equalsIgnoreCase("parent_seq_num")) 		{return this.parent_seq_num;}
		else if (theColumnName.equalsIgnoreCase("tx_id")) 				{return this.tx_id;}
		else if (theColumnName.equalsIgnoreCase("org_code")) 			{return this.org_code;}
		else if (theColumnName.equalsIgnoreCase("seq_num")) 			{return this.seq_num;}
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
	void setQualTX(QualTX theQualTX)
		throws Exception
	{
		this.qualTX = theQualTX;
		this.org_code = this.qualTX.org_code;
		this.tx_id = this.qualTX.tx_id;
		this.seq_num = this.qualTX.idGenerator.generate().getLSB();
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
		if (theColumnName == null) {
			throw new IllegalArgumentException(MessageFormat.format("QTX [{0}]: column name must be specified", this.alt_key_qualtx));
		}
		
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
		
		if 		("alt_key_qualtx".equalsIgnoreCase(theColumnName)) 		{this.alt_key_qualtx = ((Number)theValue).longValue();}
		else if ("created_by".equalsIgnoreCase(theColumnName)) 			{this.created_by = (String)theValue;}
		else if ("created_date".equalsIgnoreCase(theColumnName)) 		{this.created_date = (Date)theValue;}
		else if ("last_modified_by".equalsIgnoreCase(theColumnName)) 	{this.last_modified_by = (String)theValue;}
		else if ("last_modified_date".equalsIgnoreCase(theColumnName)) 	{this.last_modified_date = (Date)theValue;}
		else if ("tx_id".equalsIgnoreCase(theColumnName)) 				{this.tx_id = (String)theValue;}
		else if ("org_code".equalsIgnoreCase(theColumnName)) 			{this.org_code = (String)theValue;}
		else if ("seq_num".equalsIgnoreCase(theColumnName)) 			{this.seq_num = ((Number)theValue).longValue();}
		else if ("group_name".equalsIgnoreCase(theColumnName)) 			{this.group_name = (String)theValue;}
		else if ("parent_seq_num".equalsIgnoreCase(theColumnName)) 		{this.parent_seq_num = ((Number)theValue).longValue();}
	}
}
