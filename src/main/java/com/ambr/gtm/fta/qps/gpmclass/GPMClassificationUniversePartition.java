package com.ambr.gtm.fta.qps.gpmclass;

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
public class GPMClassificationUniversePartition 
{
	static Logger		logger = LogManager.getLogger(GPMClassificationUniversePartition.class);

	int															rowCount;
	private ArrayList<GPMClassification>						gpmClassList;
	private HashMap<Long, GPMClassificationProductContainer>	classByProdTable;
	private String												loadSQLText;
	private int													partitionNum;
	private int													fetchSize;
	int															maxCursorDepth;
	private int													partitionCount;
	private String												targetSchema;				
	private String												filterOrgCode;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GPMClassificationUniversePartition()
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
	public GPMClassificationUniversePartition(
		int 	thePartitionCount, 
		int 	thePartitionNum,
		String	theFilterOrgCode)
		throws Exception
	{
		this.partitionNum = thePartitionNum;
		this.partitionCount = thePartitionCount;
		this.filterOrgCode = theFilterOrgCode;
		this.gpmClassList = new ArrayList<>();
		this.classByProdTable = new HashMap<>();
		
		ArrayList<String>	aSQLLines = new ArrayList<>();
		
		aSQLLines.add("select alt_key_cmpl, alt_key_prod, alt_key_ctry, ctry_code, effective_from, effective_to, im_hs1, is_active"); 
		aSQLLines.add("from mdi_prod_ctry_cmpl");
		if (this.partitionCount > 1) {
			aSQLLines.add("where mod(alt_key_cmpl, ?) = ?");

			if (this.filterOrgCode != null) {
				aSQLLines.add("and org_code = ?");
			}
		}
		else {
			if (this.filterOrgCode != null) {
				aSQLLines.add("where org_code = ?");
			}
		}

		this.loadSQLText = StringUtil.join(aSQLLines.toArray(), " ");

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
	void addClassification(GPMClassification theGPMClass)
		throws Exception
	{
		GPMClassificationProductContainer	aContainer;
		
		this.gpmClassList.add(theGPMClass);
		
		aContainer = this.classByProdTable.get(theGPMClass.prodKey);
		if (aContainer == null) {
			aContainer = new GPMClassificationProductContainer();
			aContainer.prodKey = theGPMClass.prodKey;
			this.classByProdTable.put(aContainer.prodKey, aContainer);
		}
		
		aContainer.add(theGPMClass);
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public ArrayList<GPMClassification> getGPMClassifications()
		throws Exception
	{
		return this.gpmClassList;
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theProdKey
     *************************************************************************************
     */
	public GPMClassificationProductContainer getGPMClassificationsByProduct(long theProdKey)
		throws Exception
	{
		GPMClassificationProductContainer	aContainer;
		
		aContainer = this.classByProdTable.get(theProdKey);
		return aContainer;
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public Object getClassificationCount()
		throws Exception
	{
		return this.gpmClassList.size();
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
	 * @param	thePadding
	 *************************************************************************************
	 */
	public String getStatus(int thePaddingLength)
		throws Exception
	{
		ParameterizedMessageUtility		aMsgUtil;
	
		aMsgUtil = new ParameterizedMessageUtility(thePaddingLength);
		aMsgUtil.format("GPM Classification Universe Partition [{0}] of [{1}]", false, true, this.partitionNum, this.partitionCount);
		aMsgUtil.format("   Classification Details: GPMs [{0}] Classifications [{1}]", 
			false, true, 
			this.classByProdTable.size(), 
			this.getClassificationCount()
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

		this.gpmClassList.clear();
		this.gpmClassList = new ArrayList<>();
		
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
					theJdbcTemplate.query(this.loadSQLText, new GPMClassificationsRowCallbackHandler(this));
				}
				else {
					theJdbcTemplate.query(this.loadSQLText, aInputList,new GPMClassificationsRowCallbackHandler(this));
				}
			}
			catch (DataAccessException e) {
				if (!(e.getCause() instanceof MaxRowsReachedException)) {
					throw e;
				}
			}
		}
		finally {
			aPerfTracker.stop("GPM Classifications [{0}]", new Object[]{this.getClassificationCount()});
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
	public GPMClassificationUniversePartition setFetchSize(int theFetchSize)
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
	public GPMClassificationUniversePartition setMaxCursorDepth(int theMaxCursorDepth)
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
	public GPMClassificationUniversePartition setTargetSchema(String theTargetSchema)
		throws Exception
	{
		this.targetSchema = theTargetSchema;
		MessageFormatter.info(logger, "setTargetSchema", "Target schema: [{0}]", this.targetSchema);
		return this;
	}
}
