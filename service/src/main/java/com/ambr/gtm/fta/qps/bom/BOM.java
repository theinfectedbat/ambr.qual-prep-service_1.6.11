package com.ambr.gtm.fta.qps.bom;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Optional;

import javax.persistence.Column;
import javax.persistence.Table;

import com.ambr.gtm.fta.qts.TrackerCodes;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfigurationRepository;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@Table(name = "mdi_bom")
public class BOM
	implements Serializable
{
	private static final long serialVersionUID = 1L;

	@Column(name = "alt_key_bom") 				public long		alt_key_bom;
	@Column(name = "area") 						public Double	area;
	@Column(name = "area_uom") 					public String	area_uom;	
	@Column(name = "assembly_type") 			public String	assembly_type;
	@Column(name = "bom_id") 					public String	bom_id;
	@Column(name = "bom_type") 					public String	bom_type;
	@Column(name = "cost") 						public Double	cost;
	@Column(name = "created_by") 				public String	created_by;
	@Column(name = "ctry_of_manufacture") 		public String	ctry_of_manufacture;
	@Column(name = "ctry_of_origin") 			public String	ctry_of_origin;
	@Column(name = "currency_code") 			public String	currency_code;
	@Column(name = "direct_processing_cost") 	public Double	direct_processing_cost;
	@Column(name = "effective_from") 			public Date		effective_from;
	@Column(name = "effective_to") 				public Date		effective_to;
	@Column(name = "gross_weight") 				public Double	gross_weight;
	@Column(name = "include_for_trace_value") 	public String	include_for_trace_value;
	@Column(name = "manufacturer_key") 			public Long		manufacturer_key;
	@Column(name = "org_code") 					public String	org_code;
	@Column(name = "price") 					public Double	price;
	@Column(name = "priority") 					public Integer	priority;
	@Column(name = "prod_family") 				public String	prod_family;
	@Column(name = "prod_key") 					public long		prod_key;
	@Column(name = "prod_src_key") 				public Long		prod_src_key;
	@Column(name = "prod_sub_family") 			public String	prod_sub_family;
	@Column(name = "seller_key") 				public Long		seller_key;
	@Column(name = "supplier_key") 				public Long		supplier_key;
	@Column(name = "uom") 						public String	uom;
	@Column(name = "weight_uom") 				public String	weight_uom;
	@Column(name = "is_active") 				public String	is_active;
	@Column(name = "net_weight") 				public Double	net_weight;
	
	public int									componentCount;
	public int									referenceCount; // number of bom components that reference this BOM
	public ArrayList<BOMComponent>				compList;
	public ArrayList<BOMComponent>				subCompList;
	public ArrayList<BOMPrice>					priceList;
	public ArrayList<BOMDataExtension>			deList;
	public int									depth;
	private transient BOMUniversePartition		bomUniverse;
	public ArrayList<BOMQual>					qualList;
	
	@JsonIgnore
	private HashMap<Long, BOMComponent> 		altKeyBOMComponentIndex;
	public 	boolean								passedViaRVC;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	class DepthCalculator
	{
		//TODO does this need to be ignored by json serialization?
		private HashMap<Long, Boolean>		circularReferenceValidationTable;
		
		/**
	     *************************************************************************************
	     * <P>
	     * </P>
	     *************************************************************************************
	     */
		public DepthCalculator()
			throws Exception
		{
			this.circularReferenceValidationTable = new HashMap<>();
		}
		
		/**
	     *************************************************************************************
	     * <P>
	     * </P>
	     *************************************************************************************
	     */
		public void calculate()
			throws Exception
		{
			BOM.this.depth = this.calculate(BOM.this, 0);
		}
		
		/**
	     *************************************************************************************
	     * <P>
	     * </P>
	     * 
	     * @param	theBOM
	     * @param	theCurrentDepth
	     *************************************************************************************
	     */
		private int calculate(BOM theBOM, int theCurrentDepth)
			throws Exception
		{
			int		aMaxDepth = 0;
			int		aSubBomDepth;
			BOM		aSubBOM;
			
			if (theBOM == null) {
				return theCurrentDepth;
			}
			
			if (theBOM.subCompList.size() == 0) {
				return theCurrentDepth;
			}
			
			if (this.circularReferenceValidationTable.containsKey(theBOM.alt_key_bom)) {
				return theCurrentDepth;
			}
			
			this.circularReferenceValidationTable.put(theBOM.alt_key_bom, true);
			
			for (BOMComponent aSubBOMComponent : theBOM.subCompList) {
				aSubBOM = BOM.this.bomUniverse.getBOM(aSubBOMComponent.sub_bom_key);
				
				aSubBomDepth = this.calculate(aSubBOM, theCurrentDepth+1);
				if (aSubBomDepth > aMaxDepth) {
					aMaxDepth = aSubBomDepth;
				}
			}
			
			return aMaxDepth;
		}
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public BOM()
		throws Exception
	{
		this.compList = new ArrayList<>();
		this.subCompList = new ArrayList<>();
		this.priceList = new ArrayList<>();
		this.deList = new ArrayList<>();
		this.qualList = new ArrayList<>();
		this.depth = -1;
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBOMUniverse
     *************************************************************************************
     */
	public BOM(BOMUniversePartition theBOMUniverse)
		throws Exception
	{
		this();
		this.bomUniverse = theBOMUniverse;
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theComponent
     *************************************************************************************
     */
	public void addComponent(BOMComponent theComponent)
		throws Exception
	{
		theComponent.setBOM(this);
		this.compList.add(theComponent);
		this.componentCount++;
		
		if ((theComponent.sub_bom_key != null) && (theComponent.sub_bom_key != 0)) {
			this.subCompList.add(theComponent);
		}
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theBOMDataExtension
	 *************************************************************************************
	 */
	void addDataExtension(BOMDataExtension theBOMDataExtension)
		throws Exception
	{
		this.deList.add(theBOMDataExtension);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theGroupName
	 *************************************************************************************
	 */
	public ArrayList<BOMDataExtension> getDataExtensionByGroupName(String theGroupName)
	{
		ArrayList<BOMDataExtension> bomDataExtensions = new ArrayList<BOMDataExtension>();
		
		if (this.deList != null)
		{
			for (BOMDataExtension bomDataExtension : this.deList)
				if (bomDataExtension.group_name.equalsIgnoreCase(theGroupName)) bomDataExtensions.add(bomDataExtension);
		}

		return bomDataExtensions;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	thePrice
	 *************************************************************************************
	 */
	public void addPrice(BOMPrice thePrice)
		throws Exception
	{
		thePrice.setBOM(this);
		this.priceList.add(thePrice);
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public int getComponentCount()
		throws Exception
	{
		return this.componentCount;
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public int getDepth()
		throws Exception
	{
		if (this.depth == -1) {
			new DepthCalculator().calculate();
		}
		
		return this.depth;
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public int getReferenceCount()
		throws Exception
	{
		return this.referenceCount;
	}
	
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	void initializeComponentBOMReferences()
		throws Exception
	{
		for (BOMComponent aBomComp : this.compList) {
			aBomComp.setBOM(this);
		}
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theDataExtensionRepos
     *************************************************************************************
     */
	void setDataExtensionRepository(DataExtensionConfigurationRepository theDataExtensionRepos)
		throws Exception
	{
		for (BOMDataExtension aBOMDataExt : this.deList) {
			aBOMDataExt.setRepository(theDataExtensionRepos);
		}
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBOMCompDE
     *************************************************************************************
     */
	public void addComponentDataExtension(BOMComponentDataExtension theBOMCompDE)
		throws Exception
	{
		Optional<BOMComponent>	aOptionalBOMComp;
		
		aOptionalBOMComp = this.compList.stream().filter(
			aBOMComp->aBOMComp.alt_key_comp == theBOMCompDE.alt_key_comp
		).findFirst();
		
		if (aOptionalBOMComp.isPresent()) {
			aOptionalBOMComp.get().addDataExtension(theBOMCompDE);
		}
	}

	public double getBOMPrice(String priceType) {
		if(this.priceList == null)
			return 0;
		
		Optional<BOMPrice> bomPriceOptional = this.priceList.stream().filter(p-> p.price_type.equals(priceType)).findFirst();
		if(bomPriceOptional.isPresent())
		{
			BOMPrice bomPrice = bomPriceOptional.get();
			return bomPrice.price;
		}
		return 0;
	}
	
	public BOMComponent getBOMComponentByAltKey(long theAltKeyComp)
	{
		return this.altKeyBOMComponentIndex.get(theAltKeyComp);
	}
	
	public void indexByAltKeyBOMComponent()
	{
		this.altKeyBOMComponentIndex = new HashMap<Long, BOMComponent>();
		
		if (this.compList != null)
		{
			for (BOMComponent bomComponent : this.compList)
			{
				this.altKeyBOMComponentIndex.put(bomComponent.alt_key_comp, bomComponent);
			}
		}
	}
	
	public void addQual(BOMQual theQual)
			throws Exception
	{
		theQual.setBOM(this);
		this.qualList.add(theQual);
	}
	
	public String getBOMQualifiedFlag(String theFTACode, String theIVACode, String theCOI, Date theEffectiveFrom, Date theEffectiveTo) throws Exception 
	{
		if(this.qualList == null || this.qualList.isEmpty())
			return null;
		
		Optional<BOMQual> bomQualOptional = this.qualList.stream().filter(p -> p.fta_code.equalsIgnoreCase(theFTACode)
																			&& p.iva_code.equalsIgnoreCase(theIVACode)
																			&& p.ctry_of_import.equalsIgnoreCase(theCOI)
																			&& p.effective_from.equals(theEffectiveFrom)
																			&& p.effective_to.equals(theEffectiveTo)
																		).findFirst();
		if(bomQualOptional.isPresent())
		{
			BOMQual bomQual = bomQualOptional.get();
			if(this.isIntermediateBOM() && "QUALIFIED".equalsIgnoreCase(bomQual.qualified_flag))
			{
				passedViaRVC = true;
			}
			return bomQual.qualified_flag;
		}
		return null;
	}
	
	public boolean isIntermediateBOM() throws Exception
	{
		return TrackerCodes.AssemblyType.INTERMEDIATE.name().equals(this.assembly_type);
	}

}
