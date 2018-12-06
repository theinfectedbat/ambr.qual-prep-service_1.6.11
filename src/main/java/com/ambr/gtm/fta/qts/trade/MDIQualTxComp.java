package com.ambr.gtm.fta.qts.trade;

import java.sql.Timestamp;
import java.sql.Date;

//TODO is this class needed anymore (other than the table name constant)?
public class MDIQualTxComp
{
	public static final String TABLE_NAME = "mdi_qualtx_comp";

	public String org_code;
	public String tx_id;
	public String comp_id;
	public long alt_key_comp;
	public long alt_key_qualtx;

	public Double area;
	public String area_uom;
	public String component_type;
	public Double cost;
	public String critical_indicator;
	public String ctry_of_manufacture;
	public String ctry_of_origin;
	public String cumulation_currency;
	public Double cumulation_value;
	public String description;
	public String essential_character;
	public String hs_num;
	public String make_buy_flg;
	public Long prod_key;
	public Long prod_src_key;
	public String qualified_flg;
	public Date qualified_from;
	public Date qualified_to;
	public String source_of_data;
	public Long src_id;
	public Long src_key;
	public Double traced_value;
	public Double weight;
	public String weight_uom;
	
	public Integer top_down_ind;
	public Integer raw_materia_ind;
	public Integer intermediate_ind;

	public String created_by;
	public Timestamp created_date;
	public String last_modified_by;
	public Timestamp last_modified_date;
	
	public long _test_wid;

	public MDIQualTxComp()
	{
	}
}
