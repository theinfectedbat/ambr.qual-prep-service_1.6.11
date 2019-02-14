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
import org.springframework.jdbc.support.JdbcUtils;

import com.ambr.gtm.fta.qps.exception.MaxRowsReachedException;
import com.ambr.platform.rdbms.schema.providers.RDBMSVendorNameEnum;
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
	private String												loadClassificationSQLText;
	private String												loadCtrySQLText;
	private String												loadHeaderSQLText;
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
		ArrayList<String>	aSQLLines = new ArrayList<>();
		ArrayList<String>	aWhereClauseSQLLines = new ArrayList<>();

		this.partitionNum = thePartitionNum;
		this.partitionCount = thePartitionCount;
		this.filterOrgCode = theFilterOrgCode;
		this.gpmClassList = new ArrayList<>();
		this.classByProdTable = new HashMap<>();
		
		aSQLLines.add("select alt_key_cmpl, alt_key_prod, alt_key_ctry, ctry_code, effective_from, effective_to, im_hs1, is_active"); 
		aSQLLines.add("from mdi_prod_ctry_cmpl");

		if (this.partitionCount > 1) {
			aWhereClauseSQLLines.add("mod(alt_key_prod, ?) = ?");
		}

		if (this.filterOrgCode != null) {
			aWhereClauseSQLLines.add("org_code = ?");
		}

		this.loadClassificationSQLText = StringUtil.join(aSQLLines.toArray(), " ");
		if (aWhereClauseSQLLines.size() > 0) {
			this.loadClassificationSQLText += " where " + StringUtil.join(aWhereClauseSQLLines.toArray(), " and ");
		}

		aSQLLines.clear();
		aSQLLines.add("select alt_key_prod, alt_key_ctry, ctry_code, ctry_of_origin"); 
		aSQLLines.add("from mdi_prod_ctry");

		this.loadCtrySQLText = StringUtil.join(aSQLLines.toArray(), " ");
		if (aWhereClauseSQLLines.size() > 0) {
			this.loadCtrySQLText += " where " + StringUtil.join(aWhereClauseSQLLines.toArray(), " and ");
		}

		aSQLLines.clear();
		aSQLLines.add("select alt_key_prod, ctry_of_origin"); 
		aSQLLines.add("from mdi_prod");

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
	 * 
	 * @param	theProdKey
	 * @param	theGPMCtry
	 *************************************************************************************
	 */
	public void addCountry(Long theProdKey, GPMCountry theGPMCtry)
		throws Exception
	{
		GPMClassificationProductContainer	aContainer;
		
		aContainer = this.classByProdTable.get(theProdKey);
		if (aContainer == null) {
			aContainer = new GPMClassificationProductContainer();
			aContainer.prodKey = theProdKey;
			this.classByProdTable.put(aContainer.prodKey, aContainer);
		}
		
		aContainer.add(theGPMCtry);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theProdKey
	 * @param	theCtryOfOrigin
	 *************************************************************************************
	 */
	public void addProdCtryOfOrigin(Long theProdKey, String theCtryOfOrigin)
		throws Exception
	{
		GPMClassificationProductContainer	aContainer;
		
		aContainer = this.classByProdTable.get(theProdKey);
		if (aContainer == null) {
			aContainer = new GPMClassificationProductContainer();
			aContainer.prodKey = theProdKey;
			this.classByProdTable.put(aContainer.prodKey, aContainer);
		}
		
		aContainer.ctryOfOrigin = theCtryOfOrigin;
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
	public int getClassificationCount()
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
		
		this.classByProdTable.clear();
		this.classByProdTable = new HashMap<>();
		
		this.rowCount = 0;
		
		aPerfTracker.start();
		try {
			try {
				theJdbcTemplate.setFetchSize(this.fetchSize);
			if (this.targetSchema != null) {
					String dbVendor = JdbcUtils.commonDatabaseName(JdbcUtils.extractDatabaseMetaData(theJdbcTemplate.getDataSource(), "getDatabaseProductName"));
					if (dbVendor!=null && RDBMSVendorNameEnum.valueOf(dbVendor.toUpperCase()) ==   RDBMSVendorNameEnum.POSTGRESQL)
					{
						theJdbcTemplate.execute(MessageFormat.format("SET search_path TO {0}", this.targetSchema));
					}
					else
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
					theJdbcTemplate.query(this.loadClassificationSQLText, new GPMClassificationRowCallbackHandler(this));
					theJdbcTemplate.query(this.loadCtrySQLText, new GPMCountryRowCallbackHandler(this));
					theJdbcTemplate.query(this.loadHeaderSQLText, new GPMHeaderRowCallbackHandler(this));
				}
				else {
					theJdbcTemplate.query(this.loadClassificationSQLText, aInputList,new GPMClassificationRowCallbackHandler(this));
					theJdbcTemplate.query(this.loadCtrySQLText, aInputList,new GPMCountryRowCallbackHandler(this));
					theJdbcTemplate.query(this.loadHeaderSQLText, aInputList, new GPMHeaderRowCallbackHandler(this));
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
