package com.ambr.gtm.fta.qps.qualtx.universe;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Table;

import com.ambr.gtm.fta.qts.TrackerCodes;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@Table(name = "mdi_qualtx")
public class QualTXDetail 
{
	@Column(name = "alt_key_qualtx")			public long									alt_key_qualtx;
	@Column(name = "src_key") 					public long									src_key;
	@Column(name = "created_date") 				public Date									created_date;
	@Column(name = "created_by") 				public String								created_by;
	@Column(name = "last_modified_date") 		public Date									last_modified_date;
	@Column(name = "last_modified_by") 			public String								last_modified_by;
	@Column(name = "fta_code") 					public String								fta_code;
	@Column(name = "iva_code") 					public String								iva_code;
	@Column(name = "ctry_of_import") 			public String								ctry_of_import;
	@Column(name = "effective_from") 			public Date									effective_from;
	@Column(name = "effective_to") 				public Date									effective_to;
	@Column(name = "qualified_flg") 			public String								qualified_flg;
	@Column(name = "org_code") 					public String								org_code;
	@Column(name = "source_of_data") 			public String								source_of_data;
	@Column(name = "tx_id") 					public String								tx_id;
	@Column(name = "td_construction_status") 	public Short								td_construction_status;
	@Column(name = "rm_construction_status") 	public Short								rm_construction_status;
	@Column(name = "in_construction_status") 	public Short								in_construction_status;
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public QualTXDetail()
		throws Exception
	{
	}
}
