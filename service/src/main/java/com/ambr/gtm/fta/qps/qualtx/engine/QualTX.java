package com.ambr.gtm.fta.qps.qualtx.engine;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import com.ambr.gtm.fta.qps.qualtx.engine.result.BOMStatusTracker;
import com.ambr.gtm.fta.qps.qualtx.engine.result.TradeLaneStatusTracker;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfigurationRepository;
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
@Table(name = "mdi_qualtx")
public class QualTX 
	implements DataRecordInterface, TaskProgressInterface, EntityKeyInterface
{
	@Column(name = "alt_key_qualtx") 				public long 	alt_key_qualtx;
	@Column(name = "analysis_method") 				public String	analysis_method;
	@Column(name = "area") 							public Double	area;
	@Column(name = "area_uom") 						public String	area_uom;	
	@Column(name = "assembly_type") 				public String	assembly_type;
	@Column(name = "bom_type") 						public String	bom_type;	
	@Column(name = "cost") 							public Double	cost;
	@Column(name = "ctry_of_import") 				public String	ctry_of_import;	
	@Column(name = "ctry_of_manufacture") 			public String	ctry_of_manufacture;
	@Column(name = "ctry_of_origin") 				public String	ctry_of_origin;
	@Column(name = "created_by") 					public String	created_by;
	@Column(name = "created_date") 					public Date		created_date;
	@Column(name = "currency_code") 				public String	currency_code;
	@Column(name = "direct_processing_cost")		public Double 	direct_processing_cost;
	@Column(name = "effective_from") 				public Date		effective_from;
	@Column(name = "effective_to") 					public Date		effective_to;
	@Column(name = "fta_code") 						public String	fta_code;
	@Column(name = "fta_code_group")				public String	fta_code_group;
	@Column(name = "gross_weight") 					public Double	gross_weight;
	@Column(name = "hs_num") 						public String	hs_num;
	@Column(name = "in_construction_status") 		public Integer	in_construction_status;
	@Column(name = "include_for_trace_value") 		public String	include_for_trace_value;
	@Column(name = "is_active") 			 	    public String	is_active;
	@Column(name = "iva_code") 						public String	iva_code;
	@Column(name = "knit_to_shape") 				public String	knit_to_shape;
	@Column(name = "last_modified_by") 				public String	last_modified_by;
	@Column(name = "last_modified_date") 			public Date		last_modified_date;
	@Column(name = "listed_material")				public String	listed_material;
	@Column(name = "manufacturer_key") 				public Long		manufacturer_key;
	@Column(name = "org_code") 						public String	org_code;
	@Column(name = "prod_ctry_cmpl_key")			public Long		prod_ctry_cmpl_key;
	@Column(name = "prod_family") 					public String	prod_family;
	@Column(name = "prod_key") 						public Long		prod_key;
	@Column(name = "prod_src_key") 					public Long		prod_src_key;
	@Column(name = "prod_src_iva_key")				public Long		prod_src_iva_key;
	@Column(name = "prod_sub_family") 				public String	prod_sub_family;
	@Column(name = "qualified_flg") 				public String	qualified_flg;
	@Column(name = "rm_construction_status") 		public Integer	rm_construction_status;
	@Column(name = "seller_key") 					public Long		seller_key;
	@Column(name = "source_of_data") 				public String	source_of_data;
	@Column(name = "src_id") 						public String	src_id;
	@Column(name = "src_key") 						public Long		src_key;
	@Column(name = "supplier_key") 					public Long		supplier_key;
	@Column(name = "sub_pull_ctry") 				public String	sub_pull_ctry;
	@Column(name = "td_construction_status") 		public Integer	td_construction_status;
	@Column(name = "tx_id") 						public String	tx_id;
	@Column(name = "uom") 							public String	uom;
	@Column(name = "value") 						public Double	value;
	@Column(name = "rvc_limit_safety_factor")		public Double	rvc_limit_safety_factor;
	@Column(name = "rvc_threshold_safety_factor")	public Double	rvc_threshold_safety_factor;
	@Column(name = "rvc_restricted")				public String	rvc_restricted;
	@Column(name = "target_roo_id")					public String	target_roo_id;
	
	@OneToMany(targetEntity = QualTXComponent.class) 		public ArrayList<QualTXComponent>		compList;
	@OneToMany(targetEntity = QualTXPrice.class) 			public ArrayList<QualTXPrice>			priceList;
	@OneToMany(targetEntity = QualTXDataExtension.class) 	public ArrayList<QualTXDataExtension>	deList;
	
	public long								alt_key_bom;
	public UniversalObjectIDGenerator		idGenerator;
	private String							taskDesc;
	private boolean							persistFailedFlag;
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public QualTX()
		throws Exception
	{
		this.compList = new ArrayList<>();
		this.priceList = new ArrayList<>();
		this.deList = new ArrayList<>();
		this.is_active = "Y";
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theAltKeyQualTX
     *************************************************************************************
     */
	public QualTX(long theAltKeyQualTX)
		throws Exception
	{
		this();
		this.alt_key_qualtx = theAltKeyQualTX;
		this.taskDesc = "QTX." + this.alt_key_qualtx;
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param 	theIDGenerator 
     * @param 	theNewQualTXKey 
     *************************************************************************************
     */
	public QualTX(
		UniversalObjectIDGenerator 	theIDGenerator, 
		long 						theNewQualTXKey)
		throws Exception
	{
		this();
		this.idGenerator = theIDGenerator;
		if (theNewQualTXKey > 0) {
			this.alt_key_qualtx = theNewQualTXKey;
		}
		else {
			this.alt_key_qualtx = this.idGenerator.generate().getLSB();
		}
		this.tx_id = String.valueOf(this.alt_key_qualtx);
		this.taskDesc = "QTX." + this.alt_key_qualtx;
	}
	
    public void addComponent(QualTXComponent qualtxComp)
	{
		synchronized (this) 
		{
			this.compList.add(qualtxComp);
		}
		qualtxComp.qualTX = this;
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public QualTXComponent createComponent()
		throws Exception
	{
		QualTXComponent		aComponent;
		
		aComponent = new QualTXComponent(this);
		
		synchronized (this) {
			this.compList.add(aComponent);
		}
		
		return aComponent;
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
	public QualTXDataExtension createDataExtension(String theGroupName, DataExtensionConfigurationRepository theRepos)
		throws Exception
	{
		QualTXDataExtension		aQualTXDE;
		
		aQualTXDE = new QualTXDataExtension(theGroupName, theRepos);
		aQualTXDE.setQualTX(this);
		
		this.deList.add(aQualTXDE);
		
		return aQualTXDE;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public QualTXPrice createPrice()
		throws Exception
	{
		QualTXPrice	aPrice = new QualTXPrice(this);
		
		this.priceList.add(aPrice);
		return aPrice;
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
	public void generateNewKeys(boolean theMSBFlag)
		throws Exception
	{
		UniversalObjectIdentifier	aID;
		
		if (this.idGenerator == null) {
			throw new IllegalStateException(MessageFormat.format("Qual TX [{0}]: ID generator not initialized", this.alt_key_qualtx));
		}
		
		aID = this.idGenerator.generate();
		this.alt_key_qualtx = theMSBFlag? aID.getMSB() : aID.getLSB();
		
		for (QualTXComponent aComp : this.compList) {
			aComp.generateNewKeys(theMSBFlag);
		}
		
		for (QualTXPrice aPrice : this.priceList) {
			aPrice.generateNewKeys(theMSBFlag);
		}
		
		for (QualTXDataExtension aDE : this.deList) {
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
	public ArrayList<QualTXComponent> getIntermediateComponentList() 
			throws Exception
	{
		ArrayList<QualTXComponent> intermediateComponentList = new ArrayList<QualTXComponent>();
		for(QualTXComponent aQualTXComp : this.compList)
		{
			
			if(!"Y".equalsIgnoreCase(aQualTXComp.intermediate_ind))
				continue;
			
			intermediateComponentList.add(aQualTXComp);
		}
		
		return intermediateComponentList;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	@Override
	public ArrayList<String> getKeyColumnNames() 
		throws Exception 
	{
		ArrayList<String>	aList = new ArrayList<>();
		aList.add("alt_key_qualtx");
		return aList;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theColumnName
	 *************************************************************************************
	 */
	@Override
	public Object getKeyValue(String theColumnName) 
		throws Exception 
	{
		if (theColumnName == null) {
			throw new IllegalArgumentException("Column name must be specified");
		}
		else if (!"alt_key_qualtx".equalsIgnoreCase(theColumnName)) {
			throw new IllegalArgumentException(MessageFormat.format("Column [{0}]: only [alt_key_qualtx] is a valid specification", theColumnName));
		}
		
		return this.alt_key_qualtx;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public boolean getPersistFailedFlag()
		throws Exception
	{
		return this.persistFailedFlag;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public ArrayList<QualTXComponent> getRawMaterialComponentList() 
		throws Exception
	{
		ArrayList<QualTXComponent> rawMaterialComponentList = new ArrayList<QualTXComponent>();
		for(QualTXComponent aQualTXComp : this.compList)
		{
			
			if(!"Y".equalsIgnoreCase(aQualTXComp.raw_material_ind))
				continue;
			
			rawMaterialComponentList.add(aQualTXComp);
		}
		
		return rawMaterialComponentList;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	@Override
	public String getTableName() 
		throws Exception 
	{
		return "mdi_qualtx";
	}

	public ArrayList<QualTXComponent> getTopDownComponentList() 
			throws Exception
		{
			ArrayList<QualTXComponent> topDownComponentList = new ArrayList<QualTXComponent>();
			for(QualTXComponent aQualTXComp : this.compList)
			{
				
				if(!"Y".equalsIgnoreCase(aQualTXComp.top_down_ind))
					continue;
				
				topDownComponentList.add(aQualTXComp);
			}
			
			return topDownComponentList;
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
		aList.add(StandardBOMRelatedWorkIdentifierGenerator.execute(this.alt_key_bom, this.alt_key_qualtx, "QTX"));
		return aList;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public boolean hasMakeComponents() 
		throws Exception
	{
		for(QualTXComponent aQualTXComp : this.compList)
		{
			
			if(!"Y".equalsIgnoreCase(aQualTXComp.top_down_ind))
				continue;
			
			if(aQualTXComp.sub_bom_key != null && aQualTXComp.sub_bom_key != 0)
				return true;
		}
		
		return false;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public boolean removeComponent(QualTXComponent qualtxComp)
	{
		synchronized (this) {
			return this.compList.remove(qualtxComp);
		}
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public void setPersistFailed()
		throws Exception
	{
		this.persistFailedFlag = true;
	}
	
	public QualTXPrice getQualtxPrice(String priceType)
	{
		if (priceType == null) return null;
		QualTXPrice qtxPrice = null;
		if (this.priceList != null)
		{
			for (QualTXPrice qualTXPrice : priceList)
			{
				if (priceType.equalsIgnoreCase(qualTXPrice.price_type))
				{
					qtxPrice = qualTXPrice;
					break;
				}
			}
		}
		return qtxPrice;
	}
	
	
}
