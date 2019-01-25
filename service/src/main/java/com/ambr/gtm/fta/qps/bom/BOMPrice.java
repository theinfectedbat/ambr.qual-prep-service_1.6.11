package com.ambr.gtm.fta.qps.bom;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@Table(name = "mdi_bom_price")
public class BOMPrice 
	implements Serializable
{
	private static final long serialVersionUID = 1L;

	public static final String TRANSACTION_VALUE = "TRANSACTION_VALUE";
	public static final String NET_COST = "NET_COST";

	@Column(name = "alt_key_bom") 		public long		alt_key_bom;
	@Column(name = "alt_key_price") 	public long		alt_key_price;
	@Column(name = "price_seq_num") 	public long		price_seq_num;
	@Column(name = "currency_code") 	public String	currency_code;
	@Column(name = "price") 			public double	price;
	@Column(name = "price_type") 		public String	price_type;
	@Column(name = "source_of_data") 	public String	source_of_data;

	@JsonIgnore
	private transient BOM bom;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public BOMPrice()
		throws Exception
	{
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	void setBOM(BOM theBOM)
		throws Exception
	{
		this.bom = theBOM;
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
}
