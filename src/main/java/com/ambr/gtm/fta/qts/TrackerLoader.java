package com.ambr.gtm.fta.qts;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowCallbackHandler;

import com.ambr.gtm.fta.qts.container.TrackerContainer;
import com.ambr.gtm.fta.qts.observer.BOMTrackerStatusObserver;
import com.ambr.gtm.fta.qts.observer.QtxStatusObserver;
import com.ambr.gtm.fta.qts.observer.ReloadQtxWorkObserver;
import com.ambr.gtm.fta.qts.observer.TrackerGarbageCollector;
import com.ambr.gtm.fta.qts.work.QtxWorkInfo;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.platform.utils.propertyresolver.ConfigurationPropertyResolver;


public class TrackerLoader
{

	static Logger			logger				= LogManager.getLogger(TrackerLoader.class);

	private int								fetchSize;
	private String							targetSchema;
	private static boolean					isloadedFlag		= false;
	private ConfigurationPropertyResolver	propertyResolver;
	private DataSource dataSrc;
    
	TrackerContainer trackerContainer;
	
   
	/**
	 ************************************************************************************* <P>
	 * </P>
	 */
	public TrackerLoader(ConfigurationPropertyResolver propertyResolver, DataSource dataSrc) throws Exception
	{
		this.propertyResolver = propertyResolver;
		this.dataSrc = dataSrc;
		this.fetchSize = 10000;
	}

	public TrackerLoader() throws Exception
	{
		this.fetchSize = 10000;
	}
	
	/**
	 ************************************************************************************* <P>
	 * </P>
	 */
	class MaxRowsReachedException extends SQLException
	{
		private static final long serialVersionUID = 1L;
	}

	/**
	 ************************************************************************************* <P>
	 * </P>
	 */
	class QTXTrackerRowCallbackHandler implements RowCallbackHandler
	{
		/**
		 ************************************************************************************* <P>
		 * </P>
		 * 
		 * @param theResultSet
		 */
		@Override
		public void processRow(ResultSet theResultSet) throws SQLException
		{

			BOMTracker aBOMTracker;
			QtxTracker aQtxTracker;
			Long bomKey = theResultSet.getLong("BOM_KEY");
			Long qtxKey = theResultSet.getLong("QUALTX_KEY");
			Long qtxWid =  theResultSet.getLong("QTX_WID");
			try
			{
				aBOMTracker = trackerContainer.getBomTracker(bomKey);
				int newPriority = theResultSet.getInt("PRIORITY");
				int existingPriority = aBOMTracker.getPriority();
				if ( newPriority >= 0 && newPriority > existingPriority) aBOMTracker.setPriority(newPriority);
				
				QtxWorkInfo qtxWorkInfo = new QtxWorkInfo();
				qtxWorkInfo.setBomKey(bomKey);
				qtxWorkInfo.setQtxKey(qtxKey);
				qtxWorkInfo.setQtxWorkId(qtxWid);
				qtxWorkInfo.setTotalComponents(theResultSet.getLong("COMPONENTS"));
				qtxWorkInfo.setAnalysisMethod(TrackerCodes.AnalysisMethod.values()[theResultSet.getInt("ANALYSIS_METHOD")]);
				qtxWorkInfo.setQtxStatus(TrackerCodes.QualtxStatus.values()[theResultSet.getInt("STATUS")]);
				String aWaitFlg =theResultSet.getString("WAIT_FOR_NEXT_ANALYSIS_METHOD");
				if ("Y".equals(aWaitFlg)) qtxWorkInfo.setWaitForNextAnalysisMethodFlg(true);
				
				aQtxTracker =aBOMTracker.addQualtx(qtxWorkInfo);
				aQtxTracker.addQualtxWork(qtxWorkInfo);
			}
			catch (Exception e)
			{
				MessageFormatter.error(logger, "QTXTrackerRowCallbackHandler",e, "Exception while loading Qualtx work record into QTX Tracker, BOM key [{0}]  , QTX key: [{1}], QTX Work Id: [{2}] ",bomKey,qtxKey, qtxWid);
			}
		}
	}

	public void loadTracker(TrackerContainer trackerContainer) throws Exception
	{
		this.trackerContainer = trackerContainer;
		if (isloadedFlag)
		{
			MessageFormatter.debug(logger, "loadTracker", "Tracker is already loaded ");
			return;
		}
		synchronized (this)
		{
			if (isloadedFlag)
			{
				MessageFormatter.debug(logger, "loadTracker", "Tracker is already loaded ");
				return;
			}
			
			loadQtxTracker(new JdbcTemplate(this.dataSrc));
			isloadedFlag = true;
		}

		startObservers();
	}
	
	public void startObservers() throws Exception
	{
		
		int qtxObserverInterval = Integer.valueOf(this.propertyResolver.getPropertyValue(QTSProperties.QTX_OBSERVER_THREAD_INTERVAL, "30"));
		int bomObserverInterval = Integer.valueOf(this.propertyResolver.getPropertyValue(QTSProperties.BOM_OBSERVER_THREAD_INTERVAL, "45"));
		int qtxGarbageCollectorInterval = Integer.valueOf(this.propertyResolver.getPropertyValue(QTSProperties.TRACKER_GARBAGE_THREAD_INTERVAL, "180"));
		int qtxReloadInterval = Integer.valueOf(this.propertyResolver.getPropertyValue(QTSProperties.QTX_RELOAD_THREAD_INTERVAL, "3600"));

		Thread aQtxTrackerObserver = new Thread(new QtxStatusObserver(trackerContainer, qtxObserverInterval));
		aQtxTrackerObserver.setName("QtxTrackerObserver");
		aQtxTrackerObserver.start();
		MessageFormatter.debug(logger, "startObservers", "Qualtx Tracker Observer started with interval [{0}] seconds", qtxObserverInterval);
		
		
		Thread aBomTrackerObserver = new Thread(new BOMTrackerStatusObserver(trackerContainer, new JdbcTemplate(this.dataSrc), bomObserverInterval));
		aBomTrackerObserver.setName("BomTrackerObserver");
		aBomTrackerObserver.start();
		MessageFormatter.debug(logger, "startObservers", "BOM Status Tracker Observer started with interval [{0}] seconds", bomObserverInterval);
		
		
		Thread garbaseCollector = new Thread(new TrackerGarbageCollector(trackerContainer, qtxGarbageCollectorInterval));
		garbaseCollector.setName("TrackerGarbageCollector");
		garbaseCollector.start();
		MessageFormatter.debug(logger, "startObservers", "Tracker Garbage Collector started with interval [{0}] seconds", qtxGarbageCollectorInterval);
		
		
		Thread aReloadQtxWorkObserver = new Thread(new ReloadQtxWorkObserver(trackerContainer, new JdbcTemplate(this.dataSrc), this, qtxReloadInterval));
		aReloadQtxWorkObserver.setName("ReloadQtxWorkObserver");
		aReloadQtxWorkObserver.start();
		MessageFormatter.debug(logger, "startObservers", "Reload Qualtx Observer started with interval [{0}] seconds", qtxReloadInterval);
	}

	public void loadQtxTracker(JdbcTemplate theJdbcTemplate) throws Exception
	{
		MessageFormatter.debug(logger, "loadQtxTracker", "Tracker loading is started ");
		try
		{
			theJdbcTemplate.setFetchSize(this.fetchSize);
			if (this.targetSchema != null)
			{
				theJdbcTemplate.execute(MessageFormat.format("alter session set current_schema={0}", this.targetSchema));
			}

			theJdbcTemplate.query(TrackerLoaderQueries.qtxTrackerQuery, new PreparedStatementSetter()
			{
				public void setValues(PreparedStatement ps) throws SQLException
				{
					ps.setLong(1, TrackerCodes.QualtxStatus.BOM_POST_POLICY_WORK_POSTED.ordinal());
					ps.setLong(2, TrackerCodes.QualtxStatus.QUALTX_PREP_FAILED.ordinal());
					ps.setLong(3, TrackerCodes.QualtxStatus.QUALIFICATION_FAILED.ordinal());
				}
			}, new QTXTrackerRowCallbackHandler());
			MessageFormatter.debug(logger, "loadQtxTracker", "Tracker loading is finished ");
		}
		catch (DataAccessException e)
		{
			if (!(e.getCause() instanceof MaxRowsReachedException)) { throw e; }
		}
		
	}

	public TrackerLoader setFetchSize(int theFetchSize) throws Exception
	{
		this.fetchSize = theFetchSize;
		 MessageFormatter.debug(logger, "setFetchSize", "Fetch size [{0}]", this.fetchSize);
		return this;
	}

	/**
	 ************************************************************************************* <P>
	 * </P>
	 * 
	 * @param theTargetSchema
	 */
	public TrackerLoader setTargetSchema(String theTargetSchema) throws Exception
	{
		this.targetSchema = theTargetSchema;
		MessageFormatter.debug(logger, "setTargetSchema", "Target schema: [{0}]", this.targetSchema);
		return this;
	}

	public void reloadTracker(JdbcTemplate aTemplate,Set<QtxWorkTracker> reloadWorkTrackerList) throws Exception
	{
		Set<Long> aReloadWorkIdSet = new HashSet<>();
		Set<Long> aReloadQtxKeySet = new HashSet<>();
		for (QtxWorkTracker aQtxTracker : reloadWorkTrackerList)
		{
			aReloadWorkIdSet.add(aQtxTracker.getQualtxWorkId());
			aReloadQtxKeySet.add(aQtxTracker.getQualtxKey());
		}
		removeExistingQtxDetails(reloadWorkTrackerList, aReloadQtxKeySet);

		Set<Long> aReloadIdSet = new HashSet<>();
		Iterator<Long> aWorkIdIterator = aReloadWorkIdSet.iterator();
		for (int i = 0; aWorkIdIterator.hasNext(); i++)
		{
			aReloadIdSet.add(aWorkIdIterator.next());
			if (i > 0 && i % 10000 == 0)
			{
				reloadQtxTracker(aTemplate, aReloadIdSet);
				aReloadIdSet.clear();
			}
		}
		if (!aReloadIdSet.isEmpty())
		{
			reloadQtxTracker(aTemplate, aReloadIdSet);
		}
	}
	
	public void reloadQtxTracker(JdbcTemplate theJdbcTemplate,Set<Long> theReloadWorkIdSet) throws Exception
	{
		try
		{
			String aSql = replaceInWhereClause(TrackerLoaderQueries.qtxTrackerReloadQuery, theReloadWorkIdSet.size());
			if (aSql != null)
			{
				theJdbcTemplate.query(aSql, theReloadWorkIdSet.toArray(), new QTXTrackerRowCallbackHandler());
			}
		}
		catch (DataAccessException e)
		{
			if (!(e.getCause() instanceof MaxRowsReachedException)) { throw e; }
		}
	}

	private String replaceInWhereClause(String theSql ,int theReloadWorkListSize) throws Exception
	{
		String aFinalSql = null;
		if (theSql != null && theReloadWorkListSize > 0)
		{
			String aReplaceIdInCondition = CommonUtility.parametrizeSQLInClause("QWS.QTX_WID", theReloadWorkListSize, 1, false);
			aFinalSql = theSql.replaceAll("::TO_REPLACE::", aReplaceIdInCondition);
		}
		return aFinalSql;
	}

	public void removeExistingQtxDetails( Set<QtxWorkTracker> reloadWorkList ,Set<Long> theReloadQtxKeySet) throws Exception
	{

		synchronized (theReloadQtxKeySet)
		{
			for (Long aQtxKey : theReloadQtxKeySet)
			{
				QtxTracker aQtxTracker = this.trackerContainer.getQtxTracker(aQtxKey);
				for (QtxWorkTracker qtxWorkTracker : reloadWorkList)
				{
					if (qtxWorkTracker.getQualtxKey() == aQtxKey)
					{
						aQtxTracker.deleteQtxWorkTracker(qtxWorkTracker);
					}
				}
			}
		}
		synchronized (reloadWorkList)
		{
			this.trackerContainer.deleteQtxWorkTrackers(reloadWorkList);
		}
	}
	
}
