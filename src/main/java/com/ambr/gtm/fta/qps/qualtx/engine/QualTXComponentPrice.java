package com.ambr.gtm.fta.qps.qualtx.engine;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.persistence.Column;
import javax.persistence.Table;

import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetails;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfigurationRepository;
import com.ambr.platform.rdbms.util.bdrp.DataRecordInterface;
import com.ambr.platform.uoid.UniversalObjectIdentifier;
import com.ambr.platform.utils.queue.TaskProgressInterface;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@Table(name = "mdi_qualtx_comp_price")
public class QualTXComponentPrice 
	implements DataRecordInterface, TaskProgressInterface
{
	public QualTXComponent		qualTXComp;
	
	@Column(name = "alt_key_comp") 			public long		alt_key_comp;
	@Column(name = "alt_key_qualtx") 		public long 	alt_key_qualtx;
	@Column(name = "alt_key_price") 		public long 	alt_key_price;
	@Column(name = "comp_id") 				public String 	comp_id;
	@Column(name = "created_by") 			public String	created_by;
	@Column(name = "created_date") 			public Date		created_date;

	@Column(name = "last_modified_by") 		public String	last_modified_by;
	@Column(name = "last_modified_date")	public Date		last_modified_date;
	@Column(name = "org_code") 				public String	org_code;
	@Column(name = "price") 				public Double	price;
	@Column(name = "price_seq_num") 		public long		price_seq_num;
	@Column(name = "price_type") 			public String	price_type;
	@Column(name = "tx_id") 				public String	tx_id;
	@Column(name = "currency_code") 		public String	currency_code;
	
	private String taskDesc;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public QualTXComponentPrice()
		throws Exception
	{
		this.qualTXComp = null;
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theQualTXComponent
     *************************************************************************************
     */
	QualTXComponentPrice(QualTXComponent theQualTXComponent)
		throws Exception
	{
		this();
		this.qualTXComp = theQualTXComponent;
		this.comp_id = this.qualTXComp.comp_id;
		this.org_code = this.qualTXComp.org_code;
		this.alt_key_qualtx = this.qualTXComp.alt_key_qualtx;
		this.alt_key_comp = this.qualTXComp.alt_key_comp;
		this.alt_key_price = this.qualTXComp.qualTX.idGenerator.generate().getLSB();
		this.price_seq_num = this.alt_key_price;
		this.tx_id = this.qualTXComp.tx_id;
		this.created_by = this.qualTXComp.created_by;
		this.created_date = this.qualTXComp.created_date;
		this.last_modified_by = this.qualTXComp.last_modified_by;
		this.last_modified_date = this.qualTXComp.last_modified_date;

		this.taskDesc = 
			"QTX." + this.alt_key_qualtx +
			"COMP." + this.alt_key_comp +
			"PRICE." + this.alt_key_price
		;
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
	 *************************************************************************************
	 */
	@Override
	public ArrayList<String> getWorkIdentifiersForTask() 
		throws Exception 
	{
		ArrayList<String>	aList;
		
		aList = new ArrayList<>();
		aList.add(StandardBOMRelatedWorkIdentifierGenerator.execute(this.qualTXComp.qualTX.alt_key_bom, this.alt_key_price, "QTXCOMPPRICE"));
		return aList;
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
		
		if (this.qualTXComp == null) {
			throw new IllegalStateException(MessageFormat.format("Qual TX Comp [{0}]: QualTXComponent object not initialized", this.alt_key_price));
		}

		if (this.qualTXComp.qualTX.idGenerator == null) {
			throw new IllegalStateException(MessageFormat.format("Qual TX [{0}] Component [{1}] Price [{2}]: ID generator not initialized", this.alt_key_qualtx, this.alt_key_comp, this.alt_key_price));
		}
		
		aID = this.qualTXComp.qualTX.idGenerator.generate();
		this.alt_key_qualtx = this.qualTXComp.alt_key_qualtx;
		this.alt_key_comp = this.qualTXComp.alt_key_comp;
		this.alt_key_price = theMSBFlag? aID.getMSB() : aID.getLSB();
	}
}
