package com.ambr.gtm.fta.qps.bom;

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
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfigurationRepository;
import com.ambr.platform.rdbms.schema.providers.RDBMSVendorNameEnum;
import com.ambr.platform.rdbms.util.DataRecordUtility;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.platform.utils.log.PerformanceTracker;
import com.ambr.platform.utils.misc.ParameterizedMessageUtility;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class BOMUniversePartition 
{
	static Logger		logger = LogManager.getLogger(BOMUniversePartition.class);

	int										rowCount;
	private HashMap<Long, BOM>				bomTable;
	private ArrayList<BOMComponent>			componentList;
	private int								maxBOMDepth;
	private String							loadBOMsSQLText;
	private String							loadComponentsSQLText;
	private String							loadPricesSQLText;
	private String							loadBOMDESQLText;
	private String 							loadBOMCompDESQLText;
	private int								partitionNum;
	private int								fetchSize;
	int										maxCursorDepth;
	private int								partitionCount;
	private String							targetSchema;
	private String							filterOrgCode;
	DataExtensionConfigurationRepository	dataExtensionRepos;

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public BOMUniversePartition()
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
	public BOMUniversePartition(
		int 	thePartitionCount, 
		int 	thePartitionNum,
		String	theFilterOrgCode)
		throws Exception
	{
		DataRecordUtility<?>	aUtil;
		String					aWhereClause;
		
		this.partitionNum = thePartitionNum;
		this.partitionCount = thePartitionCount;
		this.filterOrgCode = theFilterOrgCode;
		this.maxBOMDepth = -1;
		this.bomTable = new HashMap<>();
		this.componentList = new ArrayList<>();
		
		aWhereClause = (this.partitionCount == 1)? "" : "where mod(alt_key_bom, ?) = ?";
		if (this.filterOrgCode != null) {
			if (aWhereClause.length() == 0) {
				aWhereClause += "where org_code = ?";
			}
			else {
				aWhereClause += "and org_code = ?"; 
			}
		}

		aUtil = new DataRecordUtility<BOM>(BOM.class);
		this.loadBOMsSQLText = MessageFormat.format("select {0} from mdi_bom {1}", StringUtil.join(aUtil.getColumnNames().toArray(), ","), aWhereClause);

		aUtil = new DataRecordUtility<BOMPrice>(BOMPrice.class);
		this.loadPricesSQLText = MessageFormat.format("select {0} from mdi_bom_price {1}", StringUtil.join(aUtil.getColumnNames().toArray(), ","), aWhereClause);

		aUtil = new DataRecordUtility<BOMComponent>(BOMComponent.class);
		this.loadComponentsSQLText = MessageFormat.format("select {0} from mdi_bom_comp {1}", StringUtil.join(aUtil.getColumnNames().toArray(), ","), aWhereClause);

		this.loadBOMDESQLText = MessageFormat.format("select * from mdi_bom_de {0}", aWhereClause);
		this.loadBOMCompDESQLText = MessageFormat.format("select * from mdi_bom_comp_de {0}", aWhereClause);
		
		MessageFormatter.info(logger, "constructor", "Bom Universe Partition: Count [{0}] Partition Number [{1}]", this.partitionCount, this.partitionNum);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theBOM
	 *************************************************************************************
	 */
	void addBOM(BOM theBOM)
		throws Exception
	{
		this.bomTable.put(theBOM.alt_key_bom, theBOM);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theComponent
	 *************************************************************************************
	 */
	void addComponent(BOMComponent theComponent)
		throws Exception
	{
		BOM		aBOM;
		
		this.componentList.add(theComponent);
		
		aBOM = this.bomTable.get(theComponent.alt_key_bom);
		if (aBOM == null) {
			aBOM = new BOM(this);
			aBOM.alt_key_bom = theComponent.alt_key_bom;
			this.addBOM(aBOM);
		}
		
		aBOM.addComponent(theComponent);
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theBOMCompDE
	 *************************************************************************************
	 */
	public void addComponentDataExtension(BOMComponentDataExtension theBOMCompDE)
		throws Exception
	{
		BOM	aBOM;
		
		aBOM = this.bomTable.get(theBOMCompDE.alt_key_bom);
		if (aBOM == null) {
			return;
		}
		
		aBOM.addComponentDataExtension(theBOMCompDE);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theBOMDataExtension
	 *************************************************************************************
	 */
	public void addDataExtension(BOMDataExtension theBOMDataExtension)
		throws Exception
	{
		BOM		aBOM;
	
		aBOM = this.bomTable.get(theBOMDataExtension.alt_key_bom);
		if (aBOM != null) {
			aBOM.addDataExtension(theBOMDataExtension);
		}
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param thePrice
	 *************************************************************************************
	 */
	void addPrice(BOMPrice thePrice)
		throws Exception
	{
		BOM		aBOM;

		aBOM = this.bomTable.get(thePrice.alt_key_bom);
		if (aBOM != null) {
			aBOM.addPrice(thePrice);
		}
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	private void calculateBOMReferenceCounts()
		throws Exception
	{
		BOM		aBOM;
		
		for (BOMComponent aBOMComponent : this.componentList) {
			if (aBOMComponent.isSubAssembly()) {
				aBOM = this.getBOM(aBOMComponent.sub_bom_key);
				if (aBOM != null) {
					aBOM.referenceCount++;
				}
			}
		}
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public BOMMetricSetPartitionContainer getAllBOMMetrics()
		throws Exception
	{
		BOMMetricSetPartitionContainer aContainer = new BOMMetricSetPartitionContainer(this);
		return aContainer;
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBomID
     *************************************************************************************
     */
	public BOM getBOM(Long theBomID)
		throws Exception
	{
		BOM		aBOM;
		
		if (theBomID == null) {
			return null;
		}
		
		aBOM = this.bomTable.get(theBomID);
		return aBOM;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public int getBOMCount()
		throws Exception
	{
		return this.bomTable.size();
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public ArrayList<BOM> getBOMs()
		throws Exception
	{
		return new ArrayList<BOM>(this.bomTable.values());
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public Object getComponentCount()
		throws Exception
	{
		return this.componentList.size();
	}
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public int getMaxBOMDepth()
		throws Exception
	{
		int aCurrentDepth;
		
		if (this.maxBOMDepth == -1) {
			for (BOM aBOM : this.bomTable.values()) {
				aCurrentDepth = aBOM.getDepth();
				if (aCurrentDepth > this.maxBOMDepth) {
					this.maxBOMDepth = aCurrentDepth;
				}
			}
		}
		
		return this.maxBOMDepth;
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
		aMsgUtil.format("BOM Universe Partition [{0}] of [{1}]", false, true, this.partitionNum, this.partitionCount);
		aMsgUtil.format("  BOM Details: BOMs [{0}] Components [{1}] Max Depth [{2}]", false, true, 
			this.getBOMCount(), 
			this.getComponentCount(),
			this.getMaxBOMDepth()
		);
		
		System.gc();

		aMsgUtil.format("  JVM Memory: Total [{0}] Free [{1}] Max [{2}] Used [{3}]", 
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

		this.bomTable.clear();
		this.bomTable = new HashMap<>();
		
		this.componentList.clear();
		this.componentList = new ArrayList<>();
		this.rowCount = 0;
		this.maxBOMDepth = 0;
		
		aPerfTracker.start();
		try {
			try {
				theJdbcTemplate.setFetchSize(this.fetchSize);
				if (this.targetSchema != null) 
					{
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
					theJdbcTemplate.query(this.loadBOMsSQLText, new BOMRowCallbackHandler(this));
					theJdbcTemplate.query(this.loadPricesSQLText, new BOMPriceRowCallbackHandler(this));
					theJdbcTemplate.query(this.loadComponentsSQLText, new BOMComponentsRowCallbackHandler(this));
					theJdbcTemplate.query(this.loadBOMDESQLText, new BOMDataExtensionRowCallbackHandler(this));
					theJdbcTemplate.query(this.loadBOMCompDESQLText, new BOMComponentDataExtensionRowCallbackHandler(this));
				}
				else {
					theJdbcTemplate.query(this.loadBOMsSQLText,	aInputList,	new BOMRowCallbackHandler(this));
					theJdbcTemplate.query(this.loadPricesSQLText, aInputList, new BOMPriceRowCallbackHandler(this));
					theJdbcTemplate.query(this.loadComponentsSQLText, aInputList, new BOMComponentsRowCallbackHandler(this));
					theJdbcTemplate.query(this.loadBOMDESQLText, aInputList, new BOMDataExtensionRowCallbackHandler(this));
					theJdbcTemplate.query(this.loadBOMCompDESQLText, aInputList, new BOMComponentDataExtensionRowCallbackHandler(this));
				}
			}
			catch (DataAccessException e) {
				if (!(e.getCause() instanceof MaxRowsReachedException)) {
					throw e;
				}
			}
			
			this.calculateBOMReferenceCounts();
		}
		finally {
			aPerfTracker.stop("BOMs [{0}] Components [{1}] Max Depth [{2}]", new Object[]
				{
					this.getBOMCount(), 
					this.getComponentCount(),
					this.getMaxBOMDepth()
				}
			);
		}
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theRepos
	 *************************************************************************************
	 */
	public BOMUniversePartition setDataExtensionConfigRepos(DataExtensionConfigurationRepository theRepos)
		throws Exception
	{
		this.dataExtensionRepos = theRepos;
		return this;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theFetchSize
	 *************************************************************************************
	 */
	public BOMUniversePartition setFetchSize(int theFetchSize)
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
	public BOMUniversePartition setMaxCursorDepth(int theMaxCursorDepth)
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
	public BOMUniversePartition setTargetSchema(String theTargetSchema)
		throws Exception
	{
		this.targetSchema = theTargetSchema;
		MessageFormatter.info(logger, "setTargetSchema", "Target schema: [{0}]", this.targetSchema);
		return this;
	}
}
