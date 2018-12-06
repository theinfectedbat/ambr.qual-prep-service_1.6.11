package com.ambr.gtm.fta.qps.qualtx.universe;

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
public class QualTXDetailUniversePartition 
{
	static Logger		logger = LogManager.getLogger(QualTXDetailUniversePartition.class);

	int													rowCount;
	private HashMap<Long, QualTXDetailBOMContainer>		qualTXDetailTable;
	private String										loadSQLText;
	private int											partitionNum;
	private int											fetchSize;
	private int											qualTXCount;
	private int											partitionCount;
	int													maxCursorDepth;
	private String										targetSchema;
	private String 										filterOrgCode;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public QualTXDetailUniversePartition()
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
	public QualTXDetailUniversePartition(
		int 	thePartitionCount, 
		int 	thePartitionNum,
		String	theFilterOrgCode)
		throws Exception
	{
		DataRecordUtility<?>	aUtil;
		
		this.partitionNum = thePartitionNum;
		this.partitionCount = thePartitionCount;
		this.filterOrgCode = theFilterOrgCode;
		this.qualTXDetailTable = new HashMap<>();
		
		ArrayList<String>	aWhereClause = new ArrayList<>();
		
		if (this.partitionCount > 1) {
			aWhereClause.add("where mod(src_key, ?) = ?");

			if (this.filterOrgCode != null) {
				aWhereClause.add("and org_code = ?");
			}
		}
		else {
			if (this.filterOrgCode != null) {
				aWhereClause.add("where org_code = ?");
			}
		}

		aUtil = new DataRecordUtility<QualTXDetail>(QualTXDetail.class);
		this.loadSQLText = MessageFormat.format("select {0} from mdi_qualtx {1}", 
			StringUtil.join(aUtil.getColumnNames().toArray(), ","), 
			StringUtil.join(aWhereClause.toArray(), " ")
		);
		
		MessageFormatter.info(logger, "constructor", "QualTX Detail Universe Partition: Count [{0}] Partition Number [{1}]", this.partitionCount, this.partitionNum);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQualTXDetail
	 *************************************************************************************
	 */
	void addQualTXDetail(QualTXDetail theQualTXDetail)
		throws Exception
	{
		QualTXDetailBOMContainer	aContainer;
		
		this.qualTXCount++;
		
		aContainer = this.qualTXDetailTable.get(theQualTXDetail.src_key);
		if (aContainer == null) {
			aContainer = new QualTXDetailBOMContainer(theQualTXDetail.src_key);
			this.qualTXDetailTable.put(aContainer.bomKey, aContainer);
		}
		
		aContainer.add(theQualTXDetail);
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBomID
     *************************************************************************************
     */
	public QualTXDetailBOMContainer getQualTXDetailByBOM(long theBOMKey)
		throws Exception
	{
		QualTXDetailBOMContainer		aDetailContainer;
		
		aDetailContainer = this.qualTXDetailTable.get(theBOMKey);
		return aDetailContainer;
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
		return this.qualTXDetailTable.size();
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
	 *************************************************************************************
	 */
	public int getQualTXCount()
		throws Exception
	{
		return this.qualTXCount;
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
		aMsgUtil.format("Qual TX Universe Partition [{0}] of [{1}]", false, true, this.partitionNum, this.partitionCount);
		aMsgUtil.format("  Qual TX Details: BOMs [{0}] Qual TXs [{1}]", false, true, 
			this.getBOMCount(), 
			this.getQualTXCount()
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

		this.qualTXDetailTable.clear();
		this.qualTXDetailTable = new HashMap<>();
		
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
					theJdbcTemplate.query(this.loadSQLText, new QualTXDetailRowCallbackHandler(this));
				}
				else {
					theJdbcTemplate.query(this.loadSQLText,	aInputList,	new QualTXDetailRowCallbackHandler(this));
				}
			}
			catch (DataAccessException e) {
				if (!(e.getCause() instanceof MaxRowsReachedException)) {
					throw e;
				}
			}
		}
		finally {
			aPerfTracker.stop("BOMs [{0}] Qual TXs [{1}]", new Object[]{this.getBOMCount(),	this.getQualTXCount()});
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
	public QualTXDetailUniversePartition setFetchSize(int theFetchSize)
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
	public QualTXDetailUniversePartition setMaxCursorDepth(int theMaxCursorDepth)
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
	public QualTXDetailUniversePartition setTargetSchema(String theTargetSchema)
		throws Exception
	{
		this.targetSchema = theTargetSchema;
		MessageFormatter.info(logger, "setTargetSchema", "Target schema: [{0}]", this.targetSchema);
		return this;
	}
}
