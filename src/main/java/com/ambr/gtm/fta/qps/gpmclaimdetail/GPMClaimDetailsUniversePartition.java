package com.ambr.gtm.fta.qps.gpmclaimdetail;

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
public class GPMClaimDetailsUniversePartition 
{
	static Logger		logger = LogManager.getLogger(GPMClaimDetailsUniversePartition.class);

	int															rowCount;
	private HashMap<Long, GPMClaimDetailsSourceIVAContainer>	claimDetailByProdSrcIVAKeyTable;
	private String												loadSQLText;
	private int													partitionNum;
	private int													fetchSize;
	int															maxCursorDepth;
	private int													partitionCount;
	private String												targetSchema;
	private String												filterOrgCode;
	private int													claimDetailCount;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GPMClaimDetailsUniversePartition()
		throws Exception
	{
		this(1, 1, null);
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	thePartitionCount
     * @param	thePartitionNum
     * @param	theFilterOrgCode
     *************************************************************************************
     */
	public GPMClaimDetailsUniversePartition(
		int 	thePartitionCount, 
		int 	thePartitionNum,
		String	theFilterOrgCode)
		throws Exception
	{
		this.partitionNum = thePartitionNum;
		this.partitionCount = thePartitionCount;
		this.filterOrgCode = theFilterOrgCode;
		this.claimDetailByProdSrcIVAKeyTable = new HashMap<>();
		
		ArrayList<String> aSQLLines = new ArrayList<>();
		
		aSQLLines.add("select"); 
		aSQLLines.add("d.*, ii.fta_code_group, ii.record_key, si.fta_code");
		aSQLLines.add("from (select prod_src_key from mdi_bom union select prod_src_key from mdi_bom_comp) b");
		aSQLLines.add("inner join mdi_prod_src_iva si on (si.alt_key_src = b.prod_src_key)");
		aSQLLines.add("inner join mdi_ivainst ii on (si.alt_key_iva = ii.record_key)");
		aSQLLines.add("inner join mdi_ivainst_de d on (d.alt_key_ivainst = ii.alt_key_ivainst)");
		if (this.partitionCount > 1) {
			aSQLLines.add("where mod(d.alt_key_ivainst, ?) = ?");

			if (this.filterOrgCode != null) {
				aSQLLines.add("and d.org_code = ?");
			}
		}
		else {
			if (this.filterOrgCode != null) {
				aSQLLines.add("where d.org_code = ?");
			}
		}

		this.loadSQLText = StringUtil.join(aSQLLines.toArray(), " ");
		
		MessageFormatter.info(logger, "constructor", "GPM Claim Detail Universe Partition: Count [{0}] Partition Number [{1}]", this.partitionCount, this.partitionNum);
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theGPMClaimDetail
     *************************************************************************************
     */
	void addClaimDetail(GPMClaimDetails theGPMClaimDetail)
		throws Exception
	{
		GPMClaimDetailsSourceIVAContainer	aContainer;
		
		aContainer = this.claimDetailByProdSrcIVAKeyTable.get(theGPMClaimDetail.prodSrcIVAKey);
		if (aContainer == null) {
			aContainer = new GPMClaimDetailsSourceIVAContainer(theGPMClaimDetail.prodSrcIVAKey);
			this.claimDetailByProdSrcIVAKeyTable.put(aContainer.prodSrcIVAKey, aContainer);
		}
		
		aContainer.addClaimDetails(theGPMClaimDetail);
		this.claimDetailCount++;
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public int getClaimDetailCount()
		throws Exception
	{
		return this.claimDetailCount;
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
     * @param	theProdSrcIVAKey
     *************************************************************************************
     */
	public GPMClaimDetailsSourceIVAContainer getClaimDetails(long theProdSrcIVAKey)
		throws Exception
	{
		GPMClaimDetailsSourceIVAContainer	aContainer;
		
		aContainer = this.claimDetailByProdSrcIVAKeyTable.get(theProdSrcIVAKey);
		
		return aContainer;
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
		aMsgUtil.format("GPM Claim Detail Universe Partition [{0}] of [{1}]", false, true, this.partitionNum, this.partitionCount);
		aMsgUtil.format("   Claim Details: Count [{0}]", 
			false, true, 
			this.getClaimDetailCount()
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

		this.claimDetailByProdSrcIVAKeyTable.clear();
		this.claimDetailByProdSrcIVAKeyTable = new HashMap<>();
		
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
					theJdbcTemplate.query(this.loadSQLText, new GPMClaimDetailsRowCallbackHandler(this));
				}
				else {
					theJdbcTemplate.query(this.loadSQLText,	aInputList,	new GPMClaimDetailsRowCallbackHandler(this));
				}
			}
			catch (DataAccessException e) {
				if (!(e.getCause() instanceof MaxRowsReachedException)) {
					throw e;
				}
			}
		}
		finally {
			aPerfTracker.stop("GPM Claim Details [{0}]", new Object[]{this.claimDetailByProdSrcIVAKeyTable.size()});
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
	public GPMClaimDetailsUniversePartition setFetchSize(int theFetchSize)
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
	 * @param	theSqlText
	 *************************************************************************************
	 */
	public void setLoadQuery(String theSqlText)
		throws Exception
	{
		if (theSqlText != null) {
			this.loadSQLText = theSqlText;
		}
		
		MessageFormatter.info(logger, "setLoadQuery", "Query [{0}]", this.loadSQLText);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theMaxCursorDepth
	 *************************************************************************************
	 */
	public GPMClaimDetailsUniversePartition setMaxCursorDepth(int theMaxCursorDepth)
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
	public GPMClaimDetailsUniversePartition setTargetSchema(String theTargetSchema)
		throws Exception
	{
		this.targetSchema = theTargetSchema;
		MessageFormatter.info(logger, "setTargetSchema", "Target schema: [{0}]", this.targetSchema);
		return this;
	}
}
