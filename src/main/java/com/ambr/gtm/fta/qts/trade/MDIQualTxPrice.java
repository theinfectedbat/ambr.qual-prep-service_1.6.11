package com.ambr.gtm.fta.qts.trade;

import java.sql.Timestamp;

//TODO is this class needed anymore (other than the table name constant)?
public class MDIQualTxPrice
{
	public static final String TABLE_NAME = "mdi_qualtx_price";
	
	public String tx_id;
	public String org_code;
	public long alt_key_qualtx;
	public long alt_key_price;
	public long price_seq_num;
	public long price_type;
	public long price;
	public String currency_code;
	
	public String created_by;
	public Timestamp created_date;
	public String last_modified_by;
	public Timestamp last_modified_date;
	
	public MDIQualTxPrice()
	{
	}
}
