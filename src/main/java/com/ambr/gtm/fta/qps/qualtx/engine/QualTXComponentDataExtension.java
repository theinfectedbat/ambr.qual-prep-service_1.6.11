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
import com.ambr.platform.uoid.UniversalObjectIDGenerator;
import com.ambr.platform.uoid.UniversalObjectIdentifier;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@Table(name = "mdi_qualtx_comp_de")
public class QualTXComponentDataExtension 
	implements Serializable, DataRecordValueMapInterface
{
	private static final long serialVersionUID = 1L;

	@Column(name = "alt_key_comp")			public long		alt_key_comp;
	@Column(name = "alt_key_qualtx")		public long		alt_key_qualtx;
	@Column(name = "comp_id")				public String	comp_id;
	@Column(name = "created_by") 			public String	created_by;
	@Column(name = "created_date") 			public Date		created_date;
	@Column(name = "group_name") 			public String	group_name;
	@Column(name = "last_modified_by") 		public String	last_modified_by;
	@Column(name = "last_modified_date")	public Date		last_modified_date;
	@Column(name = "org_code") 				public String	org_code;
	@Column(name = "parent_seq_num") 		public long		parent_seq_num;
	@Column(name = "seq_num") 				public long		seq_num;
	@Column(name = "tx_id") 				public String	tx_id;
	
	public HashMap<String, Object>					deFieldMap;
	public HashMap<String, Timestamp>				deDateFieldMap;
	private DataExtensionConfigurationRepository	repos;
	private QualTXComponent							qualTXComp;
	private UniversalObjectIDGenerator				idGenerator;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public QualTXComponentDataExtension()
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
	 * @param 	theIDGenerator 
     *************************************************************************************
     */
	public QualTXComponentDataExtension(
		String 									theGroupName, 
		DataExtensionConfigurationRepository 	theRepos,
		UniversalObjectIDGenerator 				theIDGenerator)
		throws Exception
	{
		this();
		this.group_name = theGroupName;
		this.repos = theRepos;
		this.idGenerator = theIDGenerator;
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
		
		if (this.qualTXComp == null) {
			throw new IllegalStateException(MessageFormat.format("Qual TX DE [{0}]: QualTXComponentDE object not initialized", this.seq_num));
		}

		if (this.qualTXComp.qualTX.idGenerator == null) {
			throw new IllegalStateException(MessageFormat.format("Qual TX [{0}] Component [{1}] DE [{2}]: ID generator not initialized", this.alt_key_qualtx, this.alt_key_comp, this.seq_num));
		}
		
		aID = this.qualTXComp.qualTX.idGenerator.generate();
		this.alt_key_qualtx = this.qualTXComp.alt_key_qualtx;
		this.alt_key_comp = this.qualTXComp.alt_key_comp;
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
			aList = new ArrayList<>(new DataRecordUtility<>(QualTXComponentDataExtension.class, true).getColumnNames());
			this.deFieldMap.keySet().forEach(aKey->{aList.add(aKey);});
			this.deDateFieldMap.keySet().forEach(aKey->{aList.add(aKey);});
		}
		else {
			aCfg = this.repos.getDataExtensionConfiguration(this.group_name);
			aList = aCfg.getPhysicalColumnNames();
			aList.addAll(new DataRecordUtility<>(QualTXComponentDataExtension.class, true).getColumnNames());
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
	public String getDescription() 
	{
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
			throw new IllegalArgumentException(MessageFormat.format("QTX [{0,number,#}] Component [{1,number,#}]: column name must be specified", this.alt_key_qualtx, this.alt_key_comp));
		}
		
		theColumnName = theColumnName.toUpperCase();
		
		if 		(theColumnName.equalsIgnoreCase("alt_key_qualtx")) 		{return this.alt_key_qualtx;}
		else if (theColumnName.equalsIgnoreCase("alt_key_comp")) 		{return this.alt_key_comp;}
		else if (theColumnName.equalsIgnoreCase("created_by")) 			{return this.created_by;}
		else if (theColumnName.equalsIgnoreCase("created_date")) 		{return this.created_date;}
		else if (theColumnName.equalsIgnoreCase("comp_id")) 			{return this.comp_id;}
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
	 * 
	 * @param	theQualTXComp
	 *************************************************************************************
	 */
	@JsonIgnore
	void setQualTXComponent(QualTXComponent theQualTXComp)
		throws Exception
	{
		this.qualTXComp = theQualTXComp;
		this.org_code = this.qualTXComp.org_code;
		this.tx_id = this.qualTXComp.tx_id;
		this.alt_key_qualtx = this.qualTXComp.alt_key_qualtx;
		this.alt_key_comp = this.qualTXComp.alt_key_comp;
		this.comp_id = this.qualTXComp.comp_id;
		
		if (this.idGenerator != null) {
			this.seq_num = this.idGenerator.generate().getLSB();
		}
		else if (this.qualTXComp.qualTX != null) {
			if (this.qualTXComp.qualTX.idGenerator == null) {
				throw new IllegalStateException(MessageFormat.format("QTX [{0}] Component [{1}]: no ID generator available", this.alt_key_qualtx, this.alt_key_comp));
			}
			this.seq_num = this.qualTXComp.qualTX.idGenerator.generate().getLSB();
		}
		else {
			throw new IllegalStateException(MessageFormat.format("QTX [{0}] Component [{1}]: no ID generator available", this.alt_key_qualtx, this.alt_key_comp));
		}
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
			throw new IllegalArgumentException(MessageFormat.format("QTX [{0,number,#}] Component [{1,number,#}]: column name must be specified", this.alt_key_qualtx, this.alt_key_comp));
		}
		
		theColumnName = theColumnName.toUpperCase();
		
		if (theValue == null) {
			this.deFieldMap.remove(theColumnName);
			this.deDateFieldMap.remove(theColumnName);
			return;
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
		
		if ("alt_key_comp".equalsIgnoreCase(theColumnName)) 			{this.parent_seq_num = ((Number)theValue).longValue();}
		else if ("alt_key_qualtx".equalsIgnoreCase(theColumnName)) 		{this.alt_key_qualtx = ((Number)theValue).longValue();}
		else if ("created_by".equalsIgnoreCase(theColumnName)) 			{this.created_by = (String)theValue;}
		else if ("created_date".equalsIgnoreCase(theColumnName)) 		{this.created_date = (Date)theValue;}
		else if ("comp_id".equalsIgnoreCase(theColumnName)) 			{this.comp_id = (String)theValue;}
		else if ("group_name".equalsIgnoreCase(theColumnName)) 			{this.group_name = (String)theValue;}
		else if ("last_modified_by".equalsIgnoreCase(theColumnName)) 	{this.last_modified_by = (String)theValue;}
		else if ("last_modified_date".equalsIgnoreCase(theColumnName)) 	{this.last_modified_date = (Date)theValue;}
		else if ("org_code".equalsIgnoreCase(theColumnName)) 			{this.org_code = (String)theValue;}
		else if ("parent_seq_num".equalsIgnoreCase(theColumnName)) 		{this.parent_seq_num = ((Number)theValue).longValue();}
		else if ("seq_num".equalsIgnoreCase(theColumnName)) 			{this.seq_num = ((Number)theValue).longValue();}
		else if ("tx_id".equalsIgnoreCase(theColumnName)) 				{this.tx_id = (String)theValue;}
	}
}
