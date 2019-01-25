package com.ambr.gtm.fta.qps.ptnr;

import javax.persistence.Column;
import javax.persistence.Table;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@Table(name = "mdi_ptnr")
public class PartnerDetail 
{
	@Column(name = "alt_key_ptnr")		public long		alt_key_ptnr;
	@Column(name = "country_code")		public String	country_code;
}
