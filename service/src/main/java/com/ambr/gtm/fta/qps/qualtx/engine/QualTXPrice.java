package com.ambr.gtm.fta.qps.qualtx.engine;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Table;

import com.ambr.platform.rdbms.util.bdrp.DataRecordInterface;
import com.ambr.platform.uoid.UniversalObjectIdentifier;
import com.ambr.platform.utils.queue.TaskProgressInterface;
import com.fasterxml.jackson.annotation.JsonIgnore;

@Table(name = "mdi_qualtx_price")
public class QualTXPrice
	implements DataRecordInterface, TaskProgressInterface
{
	private static final long serialVersionUID = 1L;

	@Column(name = "alt_key_qualtx") 		public long		alt_key_qualtx;
	@Column(name = "alt_key_price") 		public long		alt_key_price;
	@Column(name = "created_by") 			public String	created_by;
	@Column(name = "created_date") 			public Date		created_date;
	@Column(name = "last_modified_by") 		public String	last_modified_by;
	@Column(name = "last_modified_date")	public Date		last_modified_date;
	@Column(name = "org_code") 				public String	org_code;
	@Column(name = "price") 				public double	price;
	@Column(name = "price_type") 			public String	price_type;
	@Column(name = "price_seq_num") 		public long		price_seq_num;
	@Column(name = "source_of_data") 		public String	source_of_data;
	@Column(name = "tx_id") 				public String	tx_id;
	@Column(name = "currency_code") 		public String	currency_code;

	@JsonIgnore
	private transient QualTX qualTX;

	@JsonIgnore
	private String taskDesc;
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public QualTXPrice()
		throws Exception
	{
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQualTX
	 *************************************************************************************
	 */
	QualTXPrice(QualTX theQualTX)
		throws Exception
	{
		this.qualTX = theQualTX;

		this.alt_key_qualtx = this.qualTX.alt_key_qualtx;
		this.alt_key_price = this.qualTX.idGenerator.generate().getLSB();
		this.created_by = this.qualTX.created_by;
		this.created_date = this.qualTX.created_date;
		this.last_modified_by = this.qualTX.last_modified_by;
		this.last_modified_date = this.qualTX.last_modified_date;
		this.org_code = this.qualTX.org_code;
		this.price_seq_num = this.alt_key_price;
		this.source_of_data = this.qualTX.source_of_data;
		this.tx_id = this.qualTX.tx_id;

		this.taskDesc = "QTX." + this.alt_key_qualtx + "PRICE." + this.alt_key_price;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theWorkIdentifier
	 * @param	theFilter
	 *************************************************************************************
	 */
	@Override
	public boolean filterWorkIdentifier(String theWorkIdentifier, String theFilter) 
		throws Exception 
	{
		return StandardWorkIdentifierFilterLogic.execute(theWorkIdentifier, theFilter);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theMSBFlag		Indicates whether to use the MSB portion of the UOID.  If false,
	 * 							the LSB portion will be used.
	 *************************************************************************************
	 */
	void generateNewKeys(boolean theMSBFlag)
		throws Exception
	{
		UniversalObjectIdentifier	aID;
		
		if (this.qualTX == null) {
			throw new IllegalStateException(MessageFormat.format("Qual TX Price [{0}]: QualTX object not initialized", this.alt_key_price));
		}

		if (this.qualTX.idGenerator == null) {
			throw new IllegalStateException(MessageFormat.format("Qual TX [{0}] Price [{1}]: ID generator not initialized", this.alt_key_qualtx, this.alt_key_price));
		}
		
		aID = this.qualTX.idGenerator.generate();
		this.alt_key_qualtx = this.qualTX.alt_key_qualtx;
		this.alt_key_price = theMSBFlag? aID.getMSB() : aID.getLSB();
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
		return this.taskDesc;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	@Override
	public ArrayList<String> getWorkIdentifiersForTask() 
		throws Exception 
	{
		ArrayList<String>	aList;
		
		aList = new ArrayList<>();
		aList.add(StandardBOMRelatedWorkIdentifierGenerator.execute(this.qualTX.alt_key_bom, this.alt_key_price, "QTXPRICE"));
		return aList;
	}
}
