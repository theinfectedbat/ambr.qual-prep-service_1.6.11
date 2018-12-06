package com.ambr.gtm.fta.qts.trade;

import java.sql.Timestamp;
import java.sql.Date;

//TODO is this class needed anymore (other than the table name constant)?
public class MDIQualTx
{
	public static final String TABLE_NAME = "mdi_qualtx";
	
	public String tx_id;
	public String org_code;
	public long alt_key_qualtx;

	public String analysis_method;
	public Double area;
	public String area_uom;
	public Double cost;
	public String ctry_of_import;
	public String ctry_of_manufacture;
	public String ctry_origin;
	public String currency_code;
	public Double deminimis_weight;
	public String deminimis_weight_orig_check;
	public String description;
	public Double direct_processing_cost;
	public Date effective_from;
	public Date effective_to;
	public String fta_code;
	public String fta_code_group;
	public Double gross_weight;
	public String hs_num;
	public String qualified_flg;
	public Long result_id;
	public String roo_qualifier;
	public Double rvc_limit_safety_factor;
	public String rvc_restricted;
	public Double rvc_threshold_safety_factor;
	public String source_of_data;
	public String src_id;
	public Long src_key;
	public String status;
	public String target_roo_id;
	public Double trace_value;
	public String uom;
	public Double value;

	public String created_by;
	public Timestamp created_date;
	public String last_modified_by;
	public Timestamp last_modified_date;
	
	public long _test_comp_count;
	public long _test_wid;

	public MDIQualTx()
	{
	}
}
