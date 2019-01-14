package com.ambr.gtm.fta.qps.qualtx.engine;

import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetails;
import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetailsSourceIVAContainer;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfiguration;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfigurationRepository;
import com.ambr.gtm.utils.legacy.rdbms.de.GroupNameSpecification;
import com.ambr.platform.rdbms.orm.EntityKeyInterface;
import com.ambr.platform.rdbms.util.bdrp.DataRecordInterface;
import com.ambr.platform.uoid.UniversalObjectIDGenerator;
import com.ambr.platform.uoid.UniversalObjectIdentifier;
import com.ambr.platform.utils.queue.TaskProgressInterface;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@Table(name = "mdi_qualtx_comp")
public class QualTXComponent 
	implements DataRecordInterface, TaskProgressInterface, EntityKeyInterface
{
	public QualTX		qualTX;
	
	@Column(name = "alt_key_comp") 				public long		alt_key_comp;
	@Column(name = "alt_key_qualtx") 			public long 	alt_key_qualtx;
	@Column(name = "area") 						public Double	area;
	@Column(name = "area_uom") 					public String	area_uom;
	@Column(name = "comp_id") 					public String	comp_id;
	@Column(name = "coo_source") 				public int		coo_source;
	@Column(name = "cost") 						public Double	cost;
	@Column(name = "component_type") 			public String	component_type;
	@Column(name = "created_by") 				public String	created_by;
	@Column(name = "created_date") 				public Date		created_date;
	@Column(name = "critical_indicator") 		public String	critical_indicator;
	@Column(name = "ctry_of_manufacture")		public String	ctry_of_manufacture;
	@Column(name = "ctry_of_origin") 			public String	ctry_of_origin;
	@Column(name = "cumulation_currency") 		public String	cumulation_currency;
	@Column(name = "cumulation_rule_applied")	public String 	cumulation_rule_applied;
	@Column(name = "cumulation_rule_fta_used")	public String 	cumulation_rule_fta_used;
	@Column(name = "cumulation_value") 			public Double	cumulation_value;
	@Column(name = "description") 				public String 	description;
	@Column(name = "essential_character") 		public String 	essential_character;
	@Column(name = "gross_weight") 				public Double	gross_weight;
	@Column(name = "hs_num") 					public String	hs_num;
	@Column(name = "include_for_trace_value") 	public String	include_for_trace_value;
	@Column(name = "in_cost") 					public Double	in_cost;
	@Column(name = "in_cumulation_value") 		public Double	in_cumulation_value;
	@Column(name = "in_qty_per") 				public Double	in_qty_per;
	@Column(name = "in_traced_value") 			public Double	in_traced_value;
	@Column(name = "intermediate_ind") 			public String	intermediate_ind;
	@Column(name = "is_active") 				public String	is_active;
	@Column(name = "last_modified_by") 			public String	last_modified_by;
	@Column(name = "last_modified_date")		public Date		last_modified_date;
	@Column(name = "make_buy_flg") 				public String	make_buy_flg;
	@Column(name = "manufacturer_key") 			public Long		manufacturer_key;
	@Column(name = "net_weight")				public Double	net_weight;
	@Column(name = "org_code") 					public String	org_code;
	@Column(name = "prod_ctry_cmpl_key") 		public Long		prod_ctry_cmpl_key;
	@Column(name = "prod_key") 					public Long		prod_key;
	@Column(name = "prod_src_iva_key") 			public Long		prod_src_iva_key;
	@Column(name = "prod_src_key") 				public Long		prod_src_key;
	@Column(name = "qty_per") 					public Double	qty_per;
	@Column(name = "qualified_flg") 			public String	qualified_flg;
	@Column(name = "qualified_from") 			public Date		qualified_from;
	@Column(name = "qualified_to") 				public Date		qualified_to;
	@Column(name = "raw_material_ind") 			public String	raw_material_ind;
	@Column(name = "rm_cost") 					public Double	rm_cost;
	@Column(name = "rm_cumulation_value") 		public Double	rm_cumulation_value;
	@Column(name = "rm_qty_per") 				public Double	rm_qty_per;
	@Column(name = "rm_traced_value") 			public Double	rm_traced_value;
	@Column(name = "seller_key") 				public Long		seller_key;
	@Column(name = "source_of_data")			public String	source_of_data;
	@Column(name = "src_id") 					public String	src_id;
	@Column(name = "src_key") 					public Long		src_key;
	@Column(name = "supplier_key") 				public Long		supplier_key;
	@Column(name = "sub_bom_id") 				public String	sub_bom_id;
	@Column(name = "sub_bom_key") 				public Long		sub_bom_key;
	@Column(name = "sub_bom_org_code") 			public String	sub_bom_org_code;
	@Column(name = "sub_pull_ctry")				public String   sub_pull_ctry;
	@Column(name = "top_down_ind") 				public String	top_down_ind;
	@Column(name = "traced_value") 				public Double	traced_value;
	@Column(name = "traced_value_currency") 	public String	traced_value_currency;
	@Column(name = "tx_id") 					public String	tx_id;
	@Column(name = "unit_cost")					public Double	unit_cost;
	@Column(name = "unit_weight")				public Double	unit_weight;
	@Column(name = "weight") 					public Double	weight;
	@Column(name = "weight_uom") 				public String	weight_uom;
	@Column(name = "prev_year_qual_applied") 	public String	prev_year_qual_applied;
	@Column(name = "td_traced_value") 			public Double	td_traced_value;
	@Column(name = "td_cumulation_value") 		public Double	td_cumulation_value;
	@Column(name = "rvc") 						public String	rvc;

	
	@OneToMany(targetEntity = QualTXComponentDataExtension.class) public ArrayList<QualTXComponentDataExtension>	deList;
	@OneToMany(targetEntity = QualTXComponentPrice.class) public ArrayList<QualTXComponentPrice>			priceList;
	
	private String taskDesc;

    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public QualTXComponent()
		throws Exception
	{
		this.qualTX = null;
		this.deList = new ArrayList<>();
		this.priceList = new ArrayList<>();
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theQualTX
     *************************************************************************************
     */
	QualTXComponent(QualTX theQualTX)
		throws Exception
	{
		this();
		this.qualTX = theQualTX;
		if (this.qualTX.idGenerator == null) {
			this.alt_key_comp = System.currentTimeMillis();
		}
		else {
			this.alt_key_comp = this.qualTX.idGenerator.generate().getLSB();
		}
		this.comp_id = String.valueOf(this.alt_key_comp);
		this.created_by = this.qualTX.created_by;
		this.created_date = this.qualTX.created_date;
		this.last_modified_by = this.qualTX.last_modified_by;
		this.last_modified_date = this.qualTX.last_modified_date;
		this.org_code = this.qualTX.org_code;
		this.alt_key_qualtx = this.qualTX.alt_key_qualtx;
		this.source_of_data = this.qualTX.source_of_data;
		this.tx_id = this.qualTX.tx_id;
		this.qualified_from = this.qualTX.effective_from;
		this.qualified_to = this.qualTX.effective_to;

		this.taskDesc = "QTX." + this.alt_key_qualtx + ".COMP" + this.alt_key_comp;
	}
	
    /**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	thePrice
	 * @param	thePriceType
	 *************************************************************************************
	 */
	public QualTXComponentPrice createPrice(Number thePrice, String thePriceType)
		throws Exception
	{
		QualTXComponentPrice	aQualTXCompPrice;

		aQualTXCompPrice = this.createPrice();
		if (thePrice != null) {
			aQualTXCompPrice.price = thePrice.doubleValue();
		}
		aQualTXCompPrice.price_type = thePriceType;
		return aQualTXCompPrice;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public QualTXComponentPrice createPrice()
		throws Exception
	{
		QualTXComponentPrice	aQualTXCompPrice;
		
		aQualTXCompPrice = new QualTXComponentPrice(this);
		this.priceList.add(aQualTXCompPrice);
		
		return aQualTXCompPrice;
	}
	
    /**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theGroupName
	 * @param	theRepos
	 * @param	theIDGenerator
	 *************************************************************************************
	 */
	public QualTXComponentDataExtension createDataExtension(
		String 									theGroupName, 
		DataExtensionConfigurationRepository 	theRepos,
		UniversalObjectIDGenerator				theIDGenerator)
		throws Exception
	{
		QualTXComponentDataExtension	aQualTXCompDE;
		
		aQualTXCompDE = new QualTXComponentDataExtension(theGroupName, theRepos, theIDGenerator);
		aQualTXCompDE.setQualTXComponent(this);
		
		this.deList.add(aQualTXCompDE);
		
		return aQualTXCompDE;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theWorkIdentifier
	 * @param	theFilter
	 *************************************************************************************
	 */
	@Override
	public boolean filterWorkIdentifier(String theWorkIdentifier, String theFilter) 
		throws Exception 
	{
		return StandardWorkIdentifierFilterLogic.execute(theWorkIdentifier, theFilter);
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
			throw new IllegalStateException(MessageFormat.format("Qual TX Component [{0}]: QualTX object not initialized", this.alt_key_comp));
		}

		if (this.qualTX.idGenerator == null) {
			throw new IllegalStateException(MessageFormat.format("Qual TX [{0}] Component [{1}]: ID generator not initialized", this.alt_key_qualtx, this.alt_key_comp));
		}
		
		aID = this.qualTX.idGenerator.generate();
		this.alt_key_qualtx = this.qualTX.alt_key_qualtx;
		this.alt_key_comp = theMSBFlag? aID.getMSB() : aID.getLSB();

		for (QualTXComponentPrice aPrice : this.priceList) {
			aPrice.generateNewKeys(theMSBFlag);
		}

		for (QualTXComponentDataExtension aDE : this.deList) {
			aDE.generateNewKeys(theMSBFlag);
		}
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	@Override
	public String getDescription() 
	{
		return this.taskDesc;
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	@Override
	public ArrayList<String> getWorkIdentifiersForTask() 
		throws Exception 
	{
		ArrayList<String>	aList;
		
		aList = new ArrayList<>();
		aList.add(StandardBOMRelatedWorkIdentifierGenerator.execute(this.qualTX.alt_key_bom, this.alt_key_comp, "QTXCOMP"));
		return aList;
	}

	@Override
	public String getTableName() throws Exception
	{
		return "mdi_qualtx_comp";
	}

	@Override
	public ArrayList<String> getKeyColumnNames() throws Exception
	{
		ArrayList<String>	aList = new ArrayList<>();
		aList.add("alt_key_comp");
		return aList;
	}

	@Override
	public Object getKeyValue(String theColumnName) throws Exception
	{
		if (theColumnName == null) {
			throw new IllegalArgumentException("Column name must be specified");
		}
		else if (!"alt_key_comp".equalsIgnoreCase(theColumnName)) {
			throw new IllegalArgumentException(MessageFormat.format("Column [{0}]: only [alt_key_comp] is a valid specification", theColumnName));
		}
		
		return this.alt_key_comp;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theClaimDetailsContainer
	 * @param	theRepos
	 *************************************************************************************
	 */
	public void setClaimDetails(
		GPMClaimDetailsSourceIVAContainer		theClaimDetailsContainer, 
		DataExtensionConfigurationRepository 	theRepos)
		throws Exception
	{
		Object						aValue;
		String						aGroupName;
		GPMClaimDetails				aClaimDetails;
		GroupNameSpecification		aGroupNameSpec;
		
		if (theClaimDetailsContainer == null) {
			return;
		}
		
		aClaimDetails = theClaimDetailsContainer.getPrimaryClaimDetails();
		if (aClaimDetails == null) {
			return;
		}
		
		aGroupName = (String)aClaimDetails.getValue("group_name");
		String aFtaCode = (String)aClaimDetails.getValue("fta_code_group");
		aGroupNameSpec = new GroupNameSpecification("STP", this.qualTX.fta_code_group);
		if (!aGroupNameSpec.groupName.equalsIgnoreCase(aGroupName)) {
			return;
		}
		
		ArrayList<QualTXComponentDataExtension> aQualTXDataExts = this.deList;
		String groupName = MessageFormat.format("{0}{1}{2}", "STP", GroupNameSpecification.SEPARATOR, aFtaCode);
		DataExtensionConfiguration	aCfg = theRepos.getDataExtensionConfiguration(groupName);
		Map<String, String> flexFieldMap = aCfg.getFlexColumnMapping();
		
		String aCampId = flexFieldMap.get("CAMPAIGN_ID");
		String aresponseId = flexFieldMap.get("RESPONSE_ID");
		QualTXComponentDataExtension qualTXCompDetails = null;
		
		if (aQualTXDataExts != null && !aQualTXDataExts.isEmpty())
		{
			for (QualTXComponentDataExtension qualTXCompDe : aQualTXDataExts)
			{
				if (qualTXCompDe.group_name.contains("QUALTX:COMP_DTLS"))
				{
					qualTXCompDetails = qualTXCompDe;
					break;
				}
			}
		}
		if(aClaimDetails.getValue(aCampId) != null || aClaimDetails.getValue(aresponseId) != null)
		{
			Timestamp now = new Timestamp(System.currentTimeMillis());
			if (qualTXCompDetails == null)
			{
				qualTXCompDetails = this.createDataExtension("QUALTX:COMP_DTLS", theRepos, null);
				qualTXCompDetails.setValue("CREATED_DATE", now);
			}
			else
			{
	
				qualTXCompDetails.setValue("LAST_MODIFIED_DATE", now);
	
			}
			
			qualTXCompDetails.setValue("LAST_MODIFIED_BY", this.last_modified_by);
			qualTXCompDetails.setValue("FLEXFIELD_VAR12", aClaimDetails.getValue(aCampId));
			qualTXCompDetails.setValue("FLEXFIELD_VAR13", aClaimDetails.getValue(aresponseId));

		}

		String aLogicalColumnName = flexFieldMap.get("TRACED_VALUE");
		if(aLogicalColumnName != null)
		{
			aValue = aClaimDetails.getValue(aLogicalColumnName);
			if (aValue != null)
				this.traced_value = ((Number)aValue).doubleValue();
		}
		
		aLogicalColumnName = flexFieldMap.get("TRACED_VALUE_CURRENCY");
		if(aLogicalColumnName != null)
		{
			aValue = aClaimDetails.getValue(aLogicalColumnName);
			if(aValue != null)
				this.traced_value_currency = (String)aValue;
		}
		
		aLogicalColumnName = flexFieldMap.get("CUMULATION_VALUE");
		if(aLogicalColumnName != null)
		{
			aValue = aClaimDetails.getValue(aLogicalColumnName);
			if (aValue != null)
				this.cumulation_value = ((Number)aValue).doubleValue();
		}
		aLogicalColumnName = flexFieldMap.get("CUMULATION_CURRENCY");
		if(aLogicalColumnName != null)
		{
			aValue = aClaimDetails.getValue(aLogicalColumnName);
			if (aValue != null)
				this.cumulation_currency = (String)aValue;
		}
		
		
		aLogicalColumnName = flexFieldMap.get("RVC");
		if(aLogicalColumnName != null)
		{
			aValue = aClaimDetails.getValue(aLogicalColumnName);
			if (aValue != null)
				this.rvc = (String)aValue;
		}
	}
}
