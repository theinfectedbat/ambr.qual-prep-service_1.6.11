package com.ambr.gtm.fta.qps.ptnr;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.util.StringUtil;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ambr.gtm.fta.qps.exception.MaxRowsReachedException;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.platform.utils.log.PerformanceTracker;
import com.ambr.platform.utils.misc.ParameterizedMessageUtility;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class PartnerDetailUniversePartition 
{
	static Logger		logger = LogManager.getLogger(PartnerDetailUniversePartition.class);

	int										rowCount;
	private HashMap<Long, PartnerDetail>	ptnrDetailTable;
	private String							loadHeaderSQLText;
	private int								partitionNum;
	private int								fetchSize;
	int										maxCursorDepth;
	private int								partitionCount;
	private String							targetSchema;				
	private String							filterOrgCode;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public PartnerDetailUniversePartition()
		throws Exception
	{
		this(0, 0, null);
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	thePartitionCount
     * @param	thePartitionNum
     *************************************************************************************
     */
	public PartnerDetailUniversePartition(
		int 	thePartitionCount, 
		int 	thePartitionNum,
		String	theFilterOrgCode)
		throws Exception
	{
		ArrayList<String>	aSQLLines = new ArrayList<>();
		ArrayList<String>	aWhereClauseSQLLines = new ArrayList<>();

		this.partitionNum = thePartitionNum;
		this.partitionCount = thePartitionCount;
		this.filterOrgCode = theFilterOrgCode;
		this.ptnrDetailTable = new HashMap<>();
		
		aSQLLines.add("select alt_key_ptnr, country_code"); 
		aSQLLines.add("from mdi_ptnr");
		aSQLLines.add("where alt_key_ptnr in (select manufacturer_key from mdi_bom_comp)");

		if (this.partitionCount > 1) {
			aWhereClauseSQLLines.add("mod(alt_key_ptnr, ?) = ?");
		}

		if (this.filterOrgCode != null) {
			aWhereClauseSQLLines.add("org_code = ?");
		}

		this.loadHeaderSQLText = StringUtil.join(aSQLLines.toArray(), " ");
		if (aWhereClauseSQLLines.size() > 0) {
			this.loadHeaderSQLText += " where " + StringUtil.join(aWhereClauseSQLLines.toArray(), " and ");
		}		
		
		MessageFormatter.info(logger, "constructor", "GPM Classification Universe Partition: Count [{0}] Partition Number [{1}]", this.partitionCount, this.partitionNum);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theComponent
	 *************************************************************************************
	 */
	void addPartnerDetail(PartnerDetail thePtnrDetail)
		throws Exception
	{
		this.ptnrDetailTable.put(thePtnrDetail.alt_key_ptnr, thePtnrDetail);
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	thePtnrKey
     *************************************************************************************
     */
	public PartnerDetail getPartnerDetail(long thePtnrKey)
		throws Exception
	{
		PartnerDetail	aPtnrDetail;
		
		aPtnrDetail = this.ptnrDetailTable.get(thePtnrKey);
		return aPtnrDetail;
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public int getPtnrDetailCount()
		throws Exception
	{
		return this.ptnrDetailTable.size();
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public int getPartitionNum()
		throws Exception
	{
		return this.partitionNum;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	thePaddingLength
	 *************************************************************************************
	 */
	public String getStatus(int thePaddingLength)
		throws Exception
	{
		ParameterizedMessageUtility		aMsgUtil;
	
		aMsgUtil = new ParameterizedMessageUtility(thePaddingLength);
		aMsgUtil.format("Partner Detail Universe Partition [{0}] of [{1}]", false, true, this.partitionNum, this.partitionCount);
		aMsgUtil.format("   Partner Count [{0}]", 
			false, true, 
			this.ptnrDetailTable.size()
		);
		
		System.gc();
	
		aMsgUtil.format("   JVM Memory: Total [{0}] Free [{1}] Max [{2}] Used [{3}]", 
			false, true, 
			Runtime.getRuntime().totalMemory(),
			Runtime.getRuntime().freeMemory(),
			Runtime.getRuntime().maxMemory(),
			Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory()
		);
		
		return aMsgUtil.getMessage();
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theJdbcTemplate
     *************************************************************************************
     */
	public void load(JdbcTemplate theJdbcTemplate)
		throws Exception
	{
		Object[]			aInputList = null;
		PerformanceTracker	aPerfTracker = new PerformanceTracker(logger, Level.INFO, "load");

		this.ptnrDetailTable.clear();
		this.ptnrDetailTable = new HashMap<>();
		
		aPerfTracker.start();
		try {
			try {
				theJdbcTemplate.setFetchSize(this.fetchSize);
				if (this.targetSchema != null) {
					theJdbcTemplate.execute(MessageFormat.format("alter session set current_schema={0}", this.targetSchema));
				}
	
				if (this.partitionCount == 1) {
					aInputList = (this.filterOrgCode == null)? null : new Object[]{this.filterOrgCode};
				}
				else if (this.filterOrgCode == null) {
					aInputList = new Object[]{new Integer(this.partitionCount), new Integer(this.partitionNum-1)};
				}
				else {
					aInputList = new Object[]{new Integer(this.partitionCount), new Integer(this.partitionNum-1), this.filterOrgCode};
				}
				
				if (aInputList == null) {
					theJdbcTemplate.query(this.loadHeaderSQLText, new PartnerDetailRowCallbackHandler(this));
				}
				else {
					theJdbcTemplate.query(this.loadHeaderSQLText, aInputList,new PartnerDetailRowCallbackHandler(this));
				}
			}
			catch (DataAccessException e) {
				if (!(e.getCause() instanceof MaxRowsReachedException)) {
					throw e;
				}
			}
		}
		finally {
			aPerfTracker.stop("Partners [{0}]", new Object[]{this.getPtnrDetailCount()});
		}
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theFetchSize
	 *************************************************************************************
	 */
	public PartnerDetailUniversePartition setFetchSize(int theFetchSize)
		throws Exception
	{
		this.fetchSize = theFetchSize;
		MessageFormatter.info(logger, "setFetchSize", "Fetch size [{0}]", this.fetchSize);
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theMaxCursorDepth
	 *************************************************************************************
	 */
	public PartnerDetailUniversePartition setMaxCursorDepth(int theMaxCursorDepth)
		throws Exception
	{
		this.maxCursorDepth = theMaxCursorDepth;
		MessageFormatter.info(logger, "setMaxCursorDepth", "Max Cursor Depth: [{0}]", this.maxCursorDepth);
		return this;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theTargetSchema
	 *************************************************************************************
	 */
	public PartnerDetailUniversePartition setTargetSchema(String theTargetSchema)
		throws Exception
	{
		this.targetSchema = theTargetSchema;
		MessageFormatter.info(logger, "setTargetSchema", "Target schema: [{0}]", this.targetSchema);
		return this;
	}
}
