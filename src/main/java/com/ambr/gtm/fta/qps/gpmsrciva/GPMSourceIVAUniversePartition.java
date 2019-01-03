package com.ambr.gtm.fta.qps.gpmsrciva;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
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
public class GPMSourceIVAUniversePartition 
{
	static Logger		logger = LogManager.getLogger(GPMSourceIVAUniversePartition.class);

	int															rowCount;
	private HashMap<Long, GPMSourceIVAProductSourceContainer>	ivaByProdSrcTable;
	private HashMap<Long, GPMSourceIVAProductContainer>			ivaByProdTable;
	private ArrayList<GPMSourceIVA>								gpmSrcIVAList;
	private String												loadSrcIVASQLText;
	private String												loadSrcCOOSQLText;
	private String												loadSrcCampaignDetailsSQLText;
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
	public GPMSourceIVAUniversePartition()
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
	public GPMSourceIVAUniversePartition(
		int 	thePartitionCount, 
		int 	thePartitionNum,
		String	theFilterOrgCode)
		throws Exception
	{
		ArrayList<String>	aSQLLines = new ArrayList<>();
		ArrayList<String> aWhereClauseLines = new ArrayList<>();

		this.partitionNum = thePartitionNum;
		this.partitionCount = thePartitionCount;
		this.filterOrgCode = theFilterOrgCode;
		this.gpmSrcIVAList = new ArrayList<>();
		this.ivaByProdSrcTable = new HashMap<>();
		this.ivaByProdTable = new HashMap<>();

		aSQLLines.add("SELECT iva.alt_key_prod, iva.alt_key_src, alt_key_iva, fta_enabled_flag, fta_code, system_decision, final_decision, iva.ctry_of_import, iva.ctry_of_origin, effective_from, effective_to, iva_code"); 
		aSQLLines.add("FROM mdi_prod_src_iva iva");
		aSQLLines.add("left join mdi_prod_src src");
		aSQLLines.add("on (iva.alt_key_src = src.alt_key_src)");
		aSQLLines.add("WHERE ");
		aSQLLines.add("(");
		aSQLLines.add("   iva.alt_key_src in ");
		aSQLLines.add("   (");
		aSQLLines.add("     (select prod_src_key from mdi_bom)");
		aSQLLines.add("     union");
		aSQLLines.add("     (select prod_src_key from mdi_bom_comp)");
		aSQLLines.add("   )");
		aSQLLines.add("   or");
		aSQLLines.add("   iva.alt_key_prod in (select prod_key from mdi_bom_comp where prod_src_key = -1)");
		aSQLLines.add(")");
		aSQLLines.add("and src.is_active = 'Y'");

		if (this.partitionCount > 1) {
			aSQLLines.add("and mod(iva.alt_key_src, ?) = ?");
		}
		
		if (this.filterOrgCode != null) {
			aSQLLines.add("and iva.org_code = ?");
		}
				
		this.loadSrcIVASQLText = StringUtil.join(aSQLLines.toArray(), " ");

		aSQLLines = new ArrayList<>();
		aSQLLines.add("SELECT alt_key_prod, alt_key_src, ctry_of_origin"); 
		aSQLLines.add("FROM mdi_prod_src");
	
		if (this.partitionCount > 1) {
			aWhereClauseLines.add("mod(alt_key_src, ?) = ?");
		}
		
		if (this.filterOrgCode != null) {
			aWhereClauseLines.add("org_code = ?");
		}

		this.loadSrcCOOSQLText = StringUtil.join(aSQLLines.toArray(), " ");
		
		if (aWhereClauseLines.size() > 0) {
			this.loadSrcCOOSQLText += " where " + StringUtil.join(aWhereClauseLines.toArray(), " and ");
		}

		aSQLLines = new ArrayList<>();
		aSQLLines.add("SELECT alt_key_prod, alt_key_src, seq_num, group_name, flexfield_var1 as \"camp_id\", flexfield_var6 as \"fta_code\", flexfield_var9 as \"prev_year_qual_override\", flexfield_date1 as \"prev_year_qual_override_date\""); 
		aSQLLines.add("FROM mdi_prod_src_de");

		this.loadSrcCampaignDetailsSQLText = StringUtil.join(aSQLLines.toArray(), " ");
		
		if (aWhereClauseLines.size() > 0) {
			this.loadSrcCampaignDetailsSQLText += " where " + StringUtil.join(aWhereClauseLines.toArray(), " and ");
		}
		
		MessageFormatter.info(logger, "constructor", "GPM IVA Universe Partition: Count [{0}] Partition Number [{1}]", this.partitionCount, this.partitionNum);
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theGPMSrcIVA
     *************************************************************************************
     */
	public void addGPMSourceIVA(GPMSourceIVA theGPMSrcIVA)
		throws Exception
	{
		GPMSourceIVAProductSourceContainer		aSrcContainer;
		GPMSourceIVAProductContainer			aProdContainer;
		
		this.gpmSrcIVAList.add(theGPMSrcIVA);
		
		aSrcContainer = this.ivaByProdSrcTable.get(theGPMSrcIVA.srcKey);
		if (aSrcContainer == null) {
			aSrcContainer = new GPMSourceIVAProductSourceContainer();
			aSrcContainer.prodSrcKey = theGPMSrcIVA.srcKey;
			this.ivaByProdSrcTable.put(aSrcContainer.prodSrcKey, aSrcContainer);
		}
		
		aSrcContainer.ivaList.add(theGPMSrcIVA);
		
		aProdContainer = this.ivaByProdTable.get(theGPMSrcIVA.prodKey);
		if (aProdContainer == null) {
			aProdContainer = new GPMSourceIVAProductContainer(theGPMSrcIVA.prodKey);
			this.ivaByProdTable.put(aProdContainer.prodKey, aProdContainer);
		}
		
		aProdContainer.add(aSrcContainer.prodSrcKey);
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theProdKey
	 * @param	theProdSrcKey
	 * @param	theGPMSrcCampDetail
	 *************************************************************************************
	 */
	public void addGPMSourceCampaignDetails(
		Long 					theProdKey, 
		Long 					theProdSrcKey,
		GPMSourceCampaignDetail theGPMSrcCampDetail)
		throws Exception
	{
		GPMSourceIVAProductSourceContainer		aSrcContainer;
		GPMSourceIVAProductContainer			aProdContainer;
		
		aSrcContainer = this.ivaByProdSrcTable.get(theProdSrcKey);
		if (aSrcContainer == null) {
			aSrcContainer = new GPMSourceIVAProductSourceContainer();
			aSrcContainer.prodSrcKey = theProdSrcKey;
			this.ivaByProdSrcTable.put(aSrcContainer.prodSrcKey, aSrcContainer);
		}
		
		aSrcContainer.campDetailList.add(theGPMSrcCampDetail);

		aProdContainer = this.ivaByProdTable.get(theProdKey);
		if (aProdContainer == null) {
			aProdContainer = new GPMSourceIVAProductContainer(theProdKey);
			this.ivaByProdTable.put(aProdContainer.prodKey, aProdContainer);
		}
		
		aProdContainer.add(aSrcContainer.prodSrcKey);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theProdKey
	 * @param	theProdSrcKey
	 * @param	theCOO
	 *************************************************************************************
	 */
	public void addGPMSourceCOO(Long theProdKey, Long theProdSrcKey, String theCOO)
		throws Exception
	{
		GPMSourceIVAProductSourceContainer		aSrcContainer;
		GPMSourceIVAProductContainer			aProdContainer;
		
		aSrcContainer = this.ivaByProdSrcTable.get(theProdSrcKey);
		if (aSrcContainer == null) {
			aSrcContainer = new GPMSourceIVAProductSourceContainer();
			aSrcContainer.prodSrcKey = theProdSrcKey;
			this.ivaByProdSrcTable.put(aSrcContainer.prodSrcKey, aSrcContainer);
		}
		
		aSrcContainer.ctryOfOrigin = theCOO;

		aProdContainer = this.ivaByProdTable.get(theProdKey);
		if (aProdContainer == null) {
			aProdContainer = new GPMSourceIVAProductContainer(theProdKey);
			this.ivaByProdTable.put(aProdContainer.prodKey, aProdContainer);
		}
		
		aProdContainer.add(aSrcContainer.prodSrcKey);
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public int getIVACount()
		throws Exception
	{
		return this.gpmSrcIVAList.size();
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
	 * @param	theProdKey
	 *************************************************************************************
	 */
	public GPMSourceIVAProductContainer getSourceIVAByProduct(long theProdKey)
		throws Exception
	{
		GPMSourceIVAProductContainer	aContainer;
		
		aContainer = this.ivaByProdTable.get(theProdKey);
		return aContainer;
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theProdSrcKey
     *************************************************************************************
     */
	public GPMSourceIVAProductSourceContainer getSourceIVABySource(long theProdSrcKey)
		throws Exception
	{
		GPMSourceIVAProductSourceContainer	aContainer;
		
		aContainer = this.ivaByProdSrcTable.get(theProdSrcKey);
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
		aMsgUtil.format("GPM Source IVA Universe Partition [{0}] of [{1}]", false, true, this.partitionNum, this.partitionCount);
		aMsgUtil.format("   IVA Details: GPM Sources [{0}] IVAs [{1}]", 
			false, true, 
			this.ivaByProdSrcTable.size(),
			this.getIVACount()
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

		this.ivaByProdSrcTable.clear();
		this.ivaByProdSrcTable = new HashMap<>();
		
		this.gpmSrcIVAList.clear();
		this.gpmSrcIVAList = new ArrayList<>();
		
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
					theJdbcTemplate.query(this.loadSrcIVASQLText, new GPMSourceIVARowCallbackHandler(this));
					theJdbcTemplate.query(this.loadSrcCOOSQLText, new GPMSourceCOORowCallbackHandler(this));
					theJdbcTemplate.query(this.loadSrcCampaignDetailsSQLText, new GPMSourceCampaignDetailRowCallbackHandler(this));
				}
				else {
					theJdbcTemplate.query(this.loadSrcIVASQLText, aInputList, new GPMSourceIVARowCallbackHandler(this));
					theJdbcTemplate.query(this.loadSrcCOOSQLText, aInputList, new GPMSourceCOORowCallbackHandler(this));
					theJdbcTemplate.query(this.loadSrcCampaignDetailsSQLText, aInputList, new GPMSourceCampaignDetailRowCallbackHandler(this));
				}
			}
			catch (DataAccessException e) {
				if (!(e.getCause() instanceof MaxRowsReachedException)) {
					throw e;
				}
			}
		}
		finally {
			aPerfTracker.stop("GPM Sources [{0}] Source IVAs [{0}]", new Object[]{this.ivaByProdSrcTable.size(), this.getIVACount()});
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
	public GPMSourceIVAUniversePartition setFetchSize(int theFetchSize)
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
			this.loadSrcIVASQLText = theSqlText;
		}
		
		MessageFormatter.info(logger, "setLoadQuery", "Query [{0}]", this.loadSrcIVASQLText);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theMaxCursorDepth
	 *************************************************************************************
	 */
	public GPMSourceIVAUniversePartition setMaxCursorDepth(int theMaxCursorDepth)
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
	public GPMSourceIVAUniversePartition setTargetSchema(String theTargetSchema)
		throws Exception
	{
		this.targetSchema = theTargetSchema;
		MessageFormatter.info(logger, "setTargetSchema", "Target schema: [{0}]", this.targetSchema);
		return this;
	}
}
