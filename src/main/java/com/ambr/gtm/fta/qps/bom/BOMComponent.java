package com.ambr.gtm.fta.qps.bom;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@Table(name = "mdi_bom_comp")
public class BOMComponent
	implements Serializable
{
	private static final long serialVersionUID = 1L;

	@Column(name = "alt_key_bom") 			public long		alt_key_bom;
	@Column(name = "alt_key_comp") 			public long		alt_key_comp;
	@Column(name = "area") 					public Double	area;
	@Column(name = "area_uom") 				public String	area_uom;
	@Column(name = "extended_cost") 		public Double	extended_cost;
	@Column(name = "comp_num") 				public Double	comp_num;
	@Column(name = "component_type") 		public String	component_type;
	@Column(name = "critical_indicator") 	public String	critical_indicator;
	@Column(name = "ctry_of_origin") 		public String	ctry_of_origin;
	@Column(name = "ctry_of_manufacture")	public String	ctry_of_manufacture;
	@Column(name = "description")			public String	description;
	@Column(name = "effective_from") 		public Date		effective_from;
	@Column(name = "effective_to") 			public Date		effective_to;
	@Column(name = "essential_character") 	public String	essential_character;
	@Column(name = "manufacturer_key") 		public Long		manufacturer_key;
	@Column(name = "net_weight") 			public Double	net_weight;
	@Column(name = "org_code") 				public String	org_code;
	@Column(name = "prod_key") 				public Long		prod_key;
	@Column(name = "prod_src_key") 			public Long		prod_src_key;
	@Column(name = "qty_per") 				public Double	qty_per;
	@Column(name = "seller_key") 			public Long		seller_key;
	@Column(name = "sub_bom_id") 			public String	sub_bom_id;
	@Column(name = "sub_bom_key") 			public Long		sub_bom_key;
	@Column(name = "sub_bom_org_code") 		public String	sub_bom_org_code;
	@Column(name = "supplier_key") 			public Long		supplier_key;
	@Column(name = "unit_cost")				public Double	unit_cost;
	@Column(name = "unit_weight") 			public Double	unit_weight;
	@Column(name = "weight_uom") 			public String	weight_uom;
	
	public ArrayList<BOMComponentDataExtension>		deList;
	
	@JsonIgnore
	private transient BOM bom;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public BOMComponent()
		throws Exception
	{
		this.deList = new ArrayList<>();
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public BOM getBOM()
		throws Exception
	{
		return this.bom;
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public boolean isSubAssembly()
		throws Exception
	{
		return  (this.sub_bom_key != null) && (this.sub_bom_key > 0);
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public void setBOM(BOM theBOM)
		throws Exception
	{
		this.bom = theBOM;
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBOMCompDE
     *************************************************************************************
     */
	public void addDataExtension(BOMComponentDataExtension theBOMCompDE)
		throws Exception
	{
		this.deList.add(theBOMCompDE);
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theGroupName
	 *************************************************************************************
	 */
	public ArrayList<BOMComponentDataExtension> getDataExtensionByGroupName(String theGroupName)
	{
		ArrayList<BOMComponentDataExtension> bomComponentDataExtensions = new ArrayList<BOMComponentDataExtension>();
		
		if (this.deList != null)
		{
			for (BOMComponentDataExtension bomComponentDataExtension : this.deList)
				if (bomComponentDataExtension.group_name.equalsIgnoreCase(theGroupName)) bomComponentDataExtensions.add(bomComponentDataExtension);
		}

		return bomComponentDataExtensions;
	}
}
