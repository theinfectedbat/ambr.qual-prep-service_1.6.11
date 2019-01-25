package com.ambr.gtm.fta.qps.bom;

import java.io.Serializable;
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
@Table(name = "mdi_bom_qual")
public class BOMQual 
	implements Serializable
{
	private static final long serialVersionUID = 1L;

	
	@Column(name = "alt_key_bom") 		public long		alt_key_bom;
	@Column(name = "alt_key_qual") 		public long		alt_key_qual;
	@Column(name = "qualification_key") public long		qualification_key;
	@Column(name = "analysis_method") 	public String	analysis_method;
	@Column(name = "qualified_flag") 	public String	qualified_flag;
	@Column(name = "fta_code") 			public String	fta_code;
	@Column(name = "iva_code") 			public String	iva_code;
	@Column(name = "ctry_of_import") 	public String	ctry_of_import;
	@Column(name = "effective_from") 	public Date		effective_from;
	@Column(name = "effective_to") 		public Date		effective_to;


	@JsonIgnore
	private transient BOM bom;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public BOMQual()
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
