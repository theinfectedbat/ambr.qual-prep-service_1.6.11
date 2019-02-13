package com.ambr.gtm.fta.qps.qualtx.engine.result;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Table;

import com.ambr.platform.rdbms.util.bdrp.DataRecordInterface;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@Table(name = "ar_qtxprep_log_dtl")
public class QualTXPrepLogDtlEntry
	implements DataRecordInterface
{
	@Column(name = "prep_instance_id") 		public String 	prep_instance_id;
	@Column(name = "detail_instance_id") 	public String 	detail_instance_id;
	@Column(name = "alt_key_bom") 			public long 	alt_key_bom;
	@Column(name = "alt_key_qualtx") 		public long 	alt_key_qualtx;
	@Column(name = "record_id") 			public long 	record_id;
	@Column(name = "record_type") 			public int 		record_type;
	@Column(name = "operation") 			public int 		operation;
	@Column(name = "message") 				public String 	message;
	@Column(name = "created_date") 			public Date 	created_date;

    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public QualTXPrepLogDtlEntry()
		throws Exception
	{
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
		return this.prep_instance_id + "." + this.detail_instance_id;
	}
}
