package com.ambr.gtm.fta.qts;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.ambr.gtm.fta.qts.util.JDBCUtility;
import com.ambr.platform.uoid.UniversalObjectIDGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;

//TODO review methods some of these may be obsolete
//TODO setlong/setint convenience routine for NULL values.
//TODO review and convert storage to use datasaver class (rdbms schema utils)
public class QTXWorkRepository
{
	private static final Logger logger = LogManager.getLogger(QTXWorkRepository.class);
	
	private UniversalObjectIDGenerator idGenerator;
	private JdbcTemplate template;
	
	private int commitSize;
	
	public QTXWorkRepository(UniversalObjectIDGenerator idGenerator, JdbcTemplate template, int commitSize)
	{
		this.idGenerator = idGenerator;
		this.template = template;
		this.commitSize = commitSize;
	}
	
	private long getNextSequence() throws Exception
	{
		return this.idGenerator.generate().getLSB();
	}
	
	public QTXWork createWork() throws RepositoryException
	{
		QTXWork work = new QTXWork();
		
		try
		{
			work.qtx_wid = this.getNextSequence();
			work.setWorkStatus(TrackerCodes.QualtxStatus.INIT);
			work.time_stamp = new Timestamp(System.currentTimeMillis());
		}
		catch (Exception s)
		{
			throw new RepositoryException("Failed to create new QTXWork record", s);
		}
		
		return work;
	}
	
	public QTXCompWork createCompWork(long wid) throws RepositoryException
	{
		QTXCompWork work = new QTXCompWork();
		
		try
		{
			work.qtx_wid = wid;
			work.qtx_comp_wid = this.getNextSequence();
			work.setWorkStatus(TrackerCodes.QualtxCompStatus.INIT);
			//work.qualifier = TrackerCodes.QualtxCompQualifier.CREATE_COMP;
			//work.time_stamp = new Timestamp(System.currentTimeMillis());
		}
		catch (Exception s)
		{
			throw new RepositoryException("Failed to create new QTXCompWork record", s);
		}
		
		return work;
	}
	
	public QTXWorkHS createWorkHS(long wid) throws RepositoryException
	{
		QTXWorkHS work = new QTXWorkHS();
		
		try
		{
			work.qtx_wid = wid;
			work.qtx_hspull_wid = this.getNextSequence();
			
			work.status = TrackerCodes.QualtxHSPullStatus.INIT;
			
			work.time_stamp = new Timestamp(System.currentTimeMillis());
		}
		catch (Exception s)
		{
			throw new RepositoryException("Failed to create new QTXWorkHS record", s);
		}
		
		return work;
	}
	
	public QTXCompWorkHS createCompWorkHS(long compWid, long wid) throws RepositoryException
	{
		QTXCompWorkHS work = new QTXCompWorkHS();
		
		try
		{
			work.qtx_comp_wid = compWid;
			work.qtx_wid = wid;
			work.qtx_comp_hspull_wid = this.getNextSequence();
			work.status = TrackerCodes.QualtxCompHSPullStatus.INIT;
			//work.time_stamp = new Timestamp(System.currentTimeMillis());
		}
		catch (Exception s)
		{
			throw new RepositoryException("Failed to create new QTXCompWorkHS record", s);
		}
		
		return work;
	}
	
	public QTXCompWorkIVA createCompWorkIVA(long compWid, long wid) throws RepositoryException
	{
		QTXCompWorkIVA work = new QTXCompWorkIVA();
		
		try
		{
			work.qtx_comp_wid = compWid;
			work.qtx_wid = wid;
			work.qtx_comp_iva_wid = this.getNextSequence();
			work.status = TrackerCodes.QualtxCompIVAPullStatus.INIT;
			//work.time_stamp = new Timestamp(System.currentTimeMillis());
		}
		catch (Exception s)
		{
			throw new RepositoryException("Failed to create new QTXCompWorkHS record", s);
		}
		
		return work;
	}
	
//	public QTXWorkDetails getWorkDetails(long wid, Connection connection) throws RepositoryException
//	{
//		QTXWorkDetails details = new QTXWorkDetails();
//		
//		try
//		{
//			PreparedStatement stmt = null;
//			
//			try
//			{
//				stmt = JDBCUtility.prepareStatement("select qualtx_key,analysis_method,components,ctry_of_import,reason_code,time_stamp from ar_qtx_work_details where qtx_wid=?", connection);
//				
//				stmt.setLong(1, wid);
//				
//				ResultSet results = null;
//				try
//				{
//					results = stmt.executeQuery();
//					
//					if (results.next())
//					{
//						details.qtx_wid = wid;
//						details.qualtx_key = results.getLong(1);
//						
//						if (results.getObject(2) != null)
//							details.analysis_method = TrackerCodes.AnalysisMethod.values()[results.getInt(2)];
//						
//						details.components = results.getLong(3);
//						details.ctry_of_import = results.getString(4);
//						details.reason_code = results.getLong(5);
//						details.time_stamp = results.getTimestamp(6);
//					}
//					else
//					{
//						throw new RepositoryException("work details record does not exist, wid of " + wid);
//					}
//				}
//				finally
//				{
//					JDBCUtility.closeResultSet(results);
//				}
//			}
//			finally
//			{
//				JDBCUtility.closeStatement(stmt);
//			}
//		}
//		catch (SQLException s)
//		{
//			throw new RepositoryException("Failed to load work details wid of " + wid, s);
//		}
//		
//		return details;
//	}

	public QTXWorkDetails createWorkDetails(long wid) throws RepositoryException
	{
		QTXWorkDetails details = new QTXWorkDetails();
		
		details.qtx_wid = wid;
		
		return details;
	}
	
	public void storeWork(QTXWork work) throws RepositoryException
	{
		Collection<QTXWork> workList = new ArrayList<QTXWork>();
		
		workList.add(work);
		
		this.storeWork(workList);
	}
	
	public void storeWork(Collection<QTXWork> workList) throws RepositoryException
	{
		logger.debug("Storing work records " + workList.size());
		
		if (workList == null || workList.size() == 0) return;
		
		ArrayList<QTXWorkStatus> statusList = new ArrayList<QTXWorkStatus>();
		ArrayList<QTXWorkDetails> detailsList = new ArrayList<QTXWorkDetails>();
		ArrayList<QTXWorkHS> hsList = new ArrayList<QTXWorkHS>();
		ArrayList<QTXCompWork> compList = new ArrayList<QTXCompWork>();
		
		try
		{
			String sql = "insert into ar_qtx_work (qtx_wid,priority,company_code,bom_key,iva_key,entity_key,entity_type,time_stamp,user_id) values (?,?,?,?,?,?,?,?,?)";
			template.batchUpdate(sql, workList, this.commitSize, new ParameterizedPreparedStatementSetter<QTXWork>() {
				   public void setValues(PreparedStatement ps, QTXWork work) throws SQLException {
					   ps.setLong(1, work.qtx_wid);
					   ps.setInt(2, work.priority);
					   ps.setString(3, work.company_code);
					   ps.setLong(4, work.bom_key);
					   if (work.iva_key != null)
						ps.setLong(5, work.iva_key);
					   else
						ps.setLong(5, 0);
					  // ps.setLong(5, work.iva_key);
						
					   if (work.entity_key != null)
						ps.setLong(6, work.entity_key);
					   else
						ps.setNull(6, java.sql.Types.NUMERIC);
						
						//ps.setLong(6, work.entity_key);
						if (work.entity_type != null)
							ps.setInt(7, work.entity_type);
						else
							ps.setNull(7, java.sql.Types.NUMERIC);
						
						//ps.setInt(7, work.entity_type);
						ps.setTimestamp(8, work.time_stamp);
						ps.setString(9, work.userId);
				   }
				});
			
			for (QTXWork work : workList)
			{
				if (work.details != null)
					detailsList.add(work.details);
				
				if (work.status != null)
					statusList.add(work.status);
				
				hsList.addAll(work.workHSList);
				compList.addAll(work.compWorkList);
			}
			
			template.batchUpdate("insert into ar_qtx_work_status (qtx_wid,status,time_stamp) values (?,?,?)", workList, this.commitSize, new ParameterizedPreparedStatementSetter<QTXWork>() {
				   public void setValues(PreparedStatement ps, QTXWork work) throws SQLException {
					   ps.setLong(1, work.qtx_wid);
					   ps.setInt(2, work.status.status.ordinal());
					   ps.setTimestamp(3,  work.time_stamp);
				   }
				});
			
			this.storeWorkDetails(detailsList);
			
			this.storeWorkHS(hsList);
			this.storeCompWork(compList);
			
			ArrayList<QTXCompWorkHS> compHSList = new ArrayList<QTXCompWorkHS>();
			ArrayList<QTXCompWorkIVA> compIVAList = new ArrayList<QTXCompWorkIVA>();
			for (QTXCompWork compWork : compList)
			{
				compHSList.addAll(compWork.compWorkHSList);
				compIVAList.addAll(compWork.compWorkIVAList);
			}
			this.storeCompWorkHS(compHSList);
			this.storeCompWorkIVA(compIVAList);
		}
		catch (Exception e)
		{
			throw new RepositoryException("Failed to batch insert work", e);
		}
	}
	
	public void storeCompWork(QTXCompWork work) throws RepositoryException
	{
		List<QTXCompWork> compWorkList = new ArrayList<QTXCompWork>();
		
		compWorkList.add(work);
		
		this.storeCompWork(compWorkList);
	}
	
	public void storeCompWork(List<QTXCompWork> compWorkList) throws RepositoryException
	{
		if (compWorkList == null || compWorkList.size() == 0) return;
		
		try
		{
			String sql = "insert into ar_qtx_comp_work (qtx_wid,qtx_comp_wid,priority,qualifier,bom_key,bom_comp_key,entity_key,entity_src_key,qualtx_comp_key,time_stamp,qualtx_key,reason_code) values (?,?,?,?,?,?,?,?,?,?,?,?)";
			template.batchUpdate(sql, compWorkList, this.commitSize, new ParameterizedPreparedStatementSetter<QTXCompWork>() {
				   public void setValues(PreparedStatement ps, QTXCompWork compWork) throws SQLException {
					   ps.setLong(1, compWork.qtx_wid);
					   ps.setLong(2, compWork.qtx_comp_wid);
					   ps.setInt(3, compWork.priority);
						
						if (compWork.qualifier != null)
							ps.setInt(4, compWork.qualifier.ordinal());
						else
							ps.setNull(4, java.sql.Types.NUMERIC);
						
						ps.setLong(5, compWork.bom_key);
						
						ps.setLong(6, compWork.bom_comp_key);
						
						if (compWork.entity_key != null)
							ps.setLong(7, compWork.entity_key);
						else
							ps.setNull(7,  java.sql.Types.NUMERIC);
						
						if (compWork.entity_src_key != null)
							ps.setLong(8, compWork.entity_src_key);
						else
							ps.setNull(8, java.sql.Types.NUMERIC);
						
						if (compWork.qualtx_comp_key != null)
							ps.setLong(9, compWork.qualtx_comp_key);
						else
							ps.setNull(9, java.sql.Types.NUMERIC);
						
						ps.setTimestamp(10, compWork.time_stamp);
						
						if (compWork.qualtx_key != null)
							ps.setLong(11, compWork.qualtx_key);
						else
							ps.setLong(11,  java.sql.Types.NUMERIC);
						
						ps.setLong(12, compWork.reason_code);
				   }
			});
			
			sql = "insert into ar_qtx_comp_work_status (qtx_wid,qtx_comp_wid,status,time_stamp) values (?,?,?,?)";
			template.batchUpdate(sql, compWorkList, this.commitSize, new ParameterizedPreparedStatementSetter<QTXCompWork>() {
				   public void setValues(PreparedStatement ps, QTXCompWork compWork) throws SQLException {
						ps.setLong(1, compWork.qtx_wid);
						ps.setLong(2, compWork.qtx_comp_wid);
						ps.setInt(3, compWork.status.status.ordinal());
						ps.setTimestamp(4,  compWork.time_stamp);
				   }
			});
		}
		catch (Exception e)
		{
			throw new RepositoryException("Failed to batch insert comp work", e);
		}
	}
	
	public void storeWorkDetails(QTXWorkDetails details) throws RepositoryException
	{
		List<QTXWorkDetails> detailsList = new ArrayList<QTXWorkDetails>();
		
		detailsList.add(details);
		
		this.storeWorkDetails(detailsList);
	}
	
	public void storeWorkDetails(List<QTXWorkDetails> detailsList) throws RepositoryException
	{
		if (detailsList == null || detailsList.size() == 0) return;

		try
		{
			template.batchUpdate("insert into ar_qtx_work_details (qtx_wid,qualtx_key,components,analysis_method,ctry_of_import,reason_code,time_stamp) values (?,?,?,?,?,?,?)", detailsList, this.commitSize, new ParameterizedPreparedStatementSetter<QTXWorkDetails>() {
				   public void setValues(PreparedStatement ps, QTXWorkDetails details) throws SQLException {
					   ps.setLong(1, details.qtx_wid);
					   
					   if (details.qualtx_key != null)
						   ps.setLong(2, details.qualtx_key);
					   else
						   ps.setNull(2,  java.sql.Types.NUMERIC);
					   
						if (details.components != null)
							ps.setLong(3, details.components);
						else
							ps.setNull(3, java.sql.Types.NUMERIC);
						
						if (details.analysis_method != null)
							ps.setInt(4, details.analysis_method.ordinal());
						else
							ps.setInt(4, TrackerCodes.AnalysisMethod.TOP_DOWN_ANALYSIS.ordinal());
						
						if (details.ctry_of_import != null)
							ps.setString(5, details.ctry_of_import);
						else
							ps.setNull(5, java.sql.Types.VARCHAR);
						
						if (details.reason_code != null)
							ps.setLong(6, details.reason_code);
						else
							ps.setNull(6, java.sql.Types.NUMERIC);

						ps.setTimestamp(7, details.time_stamp);
				   }
				});
		}
		catch (Exception e)
		{
			throw new RepositoryException("Failed to batch insert work details", e);
		}
	}
	
//	public int updateCompWork_QualTxCompKey(long compWid, long qualTxCompKey, Connection connection) throws RepositoryException
//	{
//		RepositoryException primaryException = null;
//		PreparedStatement stmt = null;
//		
//		try
//		{
//			stmt = JDBCUtility.prepareStatement("update ar_qtx_comp_work set qualtx_comp_key=?,time_stamp=? where qtx_comp_wid=?", connection);
//			
//			stmt.setLong(1, qualTxCompKey);
//			stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
//
//			stmt.setLong(3, compWid);
//
//			return JDBCUtility.executeUpdate(stmt);
//		}
//		catch (Exception e)
//		{
//			primaryException = new RepositoryException("Failed to update comp work qualtx comp key for comp wid [" + compWid + "]", e);
//			
//			throw primaryException;
//		}
//		finally
//		{
//			JDBCUtility.closeStatement(stmt);
//		}
//	}

//	public void updateCompWork_QualTxCompKey(List<QTXCompWork> compWorkList, Connection connection) throws RepositoryException
//	{
//		String sql = "update ar_qtx_comp_work set qualtx_comp_key=?,time_stamp=? where qtx_comp_wid=?";
//		template.batchUpdate(sql, compWorkList, this.commitSize, new ParameterizedPreparedStatementSetter<QTXCompWork>() {
//			   public void setValues(PreparedStatement ps, QTXCompWork compWork) throws SQLException {
//					ps.setLong(1, compWork.qualtx_comp_key);
//					ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
//
//					ps.setLong(3, compWork.qtx_comp_wid);
//			   }
//		});
//	}

//	public int updateWorkDetails(long wid, long qualTxKey, long components, TrackerCodes.AnalysisMethod analysisMethod) throws RepositoryException
//	{
//		try
//		{
//			return template.update("update ar_qtx_work_details set qualtx_key=?,components=?,analysis_method=?,time_stamp=? where qtx_wid=?", 
//					qualTxKey,
//					components,
//					analysisMethod.ordinal(),
//					new Timestamp(System.currentTimeMillis()),
//					wid);
//		}
//		catch (Exception e)
//		{
//			throw new RepositoryException("Failed to update work details for id [" + wid + "]", e);
//		}
//	}

//	public void updateWorkDetails(List<QTXWorkDetails> workDetailsList) throws RepositoryException
//	{
//		try
//		{
//			String sql = "update ar_qtx_work_details set qualtx_key=?,components=?,analysis_method=?,time_stamp=? where qtx_wid=?";
//			template.batchUpdate(sql, workDetailsList, this.commitSize, new ParameterizedPreparedStatementSetter<QTXWorkDetails>() {
//				   public void setValues(PreparedStatement ps, QTXWorkDetails details) throws SQLException {
//						ps.setLong(1, details.qualtx_key);
//						ps.setLong(2, details.components);
//						ps.setInt(3, details.analysis_method.ordinal());
//						ps.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
//						
//						ps.setLong(5, details.qtx_wid);
//				   }
//			});
//		}
//		catch (Exception e)
//		{
//			throw new RepositoryException("Failed to batch update work for the table ar_qtx_work_details", e);
//		}
//	}

	public void storeWorkHS(QTXWorkHS workHS) throws RepositoryException
	{
		List<QTXWorkHS> workHSList = new ArrayList<QTXWorkHS>();
		
		workHSList.add(workHS);
		
		this.storeWorkHS(workHSList);
	}
	
	public void storeWorkHS(List<QTXWorkHS> workHSList) throws RepositoryException
	{
		if (workHSList == null || workHSList.size() == 0) return;
		
		try
		{
			String sql = "insert into ar_qtx_work_hs (QTX_HSPULL_WID,QTX_WID,STATUS,CTRY_CMPL_KEY,TARGET_HS_CTRY,HS_NUMBER,TIME_STAMP,REASON_CODE) values (?,?,?,?,?,?,?,?)";
			template.batchUpdate(sql, workHSList, this.commitSize, new ParameterizedPreparedStatementSetter<QTXWorkHS>() {
				   public void setValues(PreparedStatement ps, QTXWorkHS workHS) throws SQLException {
					   ps.setLong(1, workHS.qtx_hspull_wid);
					   ps.setLong(2, workHS.qtx_wid);
					   ps.setLong(3, workHS.status.ordinal());
					   ps.setLong(4, workHS.ctry_cmpl_key);
					   ps.setString(5, workHS.target_hs_ctry);
						if(workHS.hs_number != null)
							ps.setString(6, workHS.hs_number);
						else
							ps.setNull(6, java.sql.Types.VARCHAR);
						
						ps.setTimestamp(7, workHS.time_stamp);
						ps.setLong(8, workHS.reason_code);
				   }
			});
		}
		catch (Exception e)
		{
			throw new RepositoryException("Failed to batch insert work for the table ar_qtx_work_hs", e);
		}
	}
	
	//TODO set limit on batch size (i.e. no more than 100 per batch)
	public void storeCompWorkHS(List<QTXCompWorkHS> compWorkHSList) throws RepositoryException
	{
		if (compWorkHSList == null || compWorkHSList.size() == 0) return;

		String sql = "insert into ar_qtx_comp_work_hs (QTX_COMP_HSPULL_WID,QTX_COMP_WID,QTX_WID,STATUS,CTRY_CMPL_KEY,TARGET_HS_CTRY,HS_NUMBER,TIME_STAMP,REASON_CODE) values (?,?,?,?,?,?,?,?,?)";
		template.batchUpdate(sql, compWorkHSList, this.commitSize, new ParameterizedPreparedStatementSetter<QTXCompWorkHS>() {
			   public void setValues(PreparedStatement ps, QTXCompWorkHS compWorkHS) throws SQLException {
				   ps.setLong(1, compWorkHS.qtx_comp_hspull_wid);
				   ps.setLong(2, compWorkHS.qtx_comp_wid);
				   ps.setLong(3, compWorkHS.qtx_wid);
				   ps.setLong(4, compWorkHS.status.ordinal());
				   ps.setLong(5, compWorkHS.ctry_cmpl_key);
				   ps.setString(6, compWorkHS.target_hs_ctry);
					if(compWorkHS.hs_number != null)
						ps.setString(7, compWorkHS.hs_number);
					else
						ps.setNull(7, java.sql.Types.VARCHAR);
					
					ps.setTimestamp(8, compWorkHS.time_stamp);
					ps.setLong(9, compWorkHS.reason_code);
			   }
		});
	}
	
	public void storeCompWorkHS(QTXCompWorkHS compWorkHS) throws RepositoryException
	{
		List<QTXCompWorkHS> compWorkHSList = new ArrayList<QTXCompWorkHS>();
		
		compWorkHSList.add(compWorkHS);
		
		this.storeCompWorkHS(compWorkHSList);
	}
	
	public void storeCompWorkIVA(QTXCompWorkIVA compWorkIVA) throws RepositoryException
	{
		List<QTXCompWorkIVA> compWorkIVAList = new ArrayList<QTXCompWorkIVA>();
		
		compWorkIVAList.add(compWorkIVA);
		
		this.storeCompWorkIVA(compWorkIVAList);
	}
	
	public void storeCompWorkIVA(List<QTXCompWorkIVA> compWorkIVAList) throws RepositoryException
	{
		if (compWorkIVAList == null || compWorkIVAList.size() == 0) return;
		
		try
		{
			String sql = "insert into ar_qtx_comp_work_iva (QTX_COMP_IVA_WID,QTX_COMP_WID,QTX_WID,STATUS,IVA_KEY,TIME_STAMP,REASON_CODE) values (?,?,?,?,?,?,?)";
			template.batchUpdate(sql, compWorkIVAList, this.commitSize, new ParameterizedPreparedStatementSetter<QTXCompWorkIVA>() {
				   public void setValues(PreparedStatement ps, QTXCompWorkIVA compWorkIVA) throws SQLException {
					   ps.setLong(1, compWorkIVA.qtx_comp_iva_wid);
					   ps.setLong(2, compWorkIVA.qtx_comp_wid);
					   ps.setLong(3, compWorkIVA.qtx_wid);
					   ps.setLong(4, compWorkIVA.status.ordinal());
					   if(compWorkIVA.iva_key != null)
						   ps.setLong(5, compWorkIVA.iva_key);
						else
						   ps.setNull(5, java.sql.Types.NUMERIC);
					  // ps.setLong(5, compWorkIVA.iva_key);
					   ps.setTimestamp(6, compWorkIVA.time_stamp);
					   ps.setLong(7, compWorkIVA.reason_code);
				   }
			});
		}
		catch (Exception e)
		{
			throw new RepositoryException("Failed to batch insert work for the table ar_qtx_comp_work_iva", e);
		}
	}
	
	public QTXCompWork getCompWork(ResultSet results) throws RepositoryException
	{
		try
		{
			QTXCompWork work = new QTXCompWork();
			
			work.qtx_wid = results.getLong("QTX_WID");
			work.qtx_comp_wid = results.getLong("QTX_COMP_WID");
			work.priority = results.getInt("PRIORITY");
			work.bom_key = results.getLong("BOM_KEY");
			work.bom_comp_key = results.getLong("BOM_COMP_KEY");
			work.entity_key = results.getLong("ENTITY_KEY");
			work.entity_src_key = results.getLong("ENTITY_SRC_KEY");
			work.qualtx_key = results.getLong("QUALTX_KEY");
			
			if (results.getObject("QUALIFIER") != null)
				work.qualifier = TrackerCodes.QualtxCompQualifier.values()[results.getInt("QUALIFIER")];

			if (results.getObject("STATUS") != null)
				work.setWorkStatus(TrackerCodes.QualtxCompStatus.values()[results.getInt("STATUS")]);
			
			work.qualtx_comp_key = results.getLong("QUALTX_COMP_KEY");
			work.reason_code = results.getLong("REASON_CODE");
			
			work.time_stamp = results.getTimestamp("TIME_STAMP");
			
			return work;
		}
		catch (SQLException s)
		{
			throw new RepositoryException("Failed to load comp work from result set", s);
		}
	}
	
	public void logError(long wid, Exception error) throws RepositoryException
	{
		try
		{
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			error.printStackTrace(pw);
			pw.close();

			template.update("insert into ar_qtx_work_log (qtx_logid, qtx_wid,entity_id,entity_name,error_log,time_stamp) values (?,?,?,?,?,?)", 
					this.getNextSequence(),
					wid,
					0,
					0,
					sw.toString(),
					new Timestamp(System.currentTimeMillis()));
		}
		catch (Exception e)
		{
			throw new RepositoryException("Failed to log work error for qtx_wid [" + wid + "]", e);
		}
	}
	
	public void updateWorkStatus(List<QTXWork> workList, TrackerCodes.QualtxStatus status) throws RepositoryException
	{
		try
		{
			template.batchUpdate("update ar_qtx_work_status set status=?,time_stamp=? where qtx_wid=?", workList, this.commitSize, new ParameterizedPreparedStatementSetter<QTXWork>() {
				   public void setValues(PreparedStatement ps, QTXWork work) throws SQLException {
					   ps.setInt(1, status.ordinal());
					   ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
					   ps.setLong(3, work.qtx_wid);
				   }
				});
		}
		catch (Exception e)
		{
			throw new RepositoryException("Failed to bulk update status", e);
		}
	}
	
	public void updateWorkStatus(List<QTXWorkStatus> statusList) throws RepositoryException
	{
		try
		{
			template.batchUpdate("update ar_qtx_work_status set status=?,time_stamp=? where qtx_wid=?", statusList, this.commitSize, new ParameterizedPreparedStatementSetter<QTXWorkStatus>() {
				   public void setValues(PreparedStatement ps, QTXWorkStatus work) throws SQLException {
						ps.setInt(1, work.status.ordinal());
						ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
			
						ps.setLong(3, work.qtx_wid);
				   }
				});
		}
		catch (Exception e)
		{
			throw new RepositoryException("Failed to bulk update status", e);
		}
	}
	
	public void updateCompWorkStatus(List<QTXCompWork> compWorkList, TrackerCodes.QualtxCompStatus status) throws RepositoryException
	{
		try
		{
			template.batchUpdate("update ar_qtx_comp_work_status set status=?,time_stamp=? where qtx_comp_wid=?", compWorkList, this.commitSize, new ParameterizedPreparedStatementSetter<QTXCompWork>() {
				   public void setValues(PreparedStatement ps, QTXCompWork compWork) throws SQLException {
						ps.setInt(1, status.ordinal());
						ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));

						ps.setLong(3, compWork.qtx_comp_wid);
				   }
				});
		}
		catch (Exception e)
		{
			throw new RepositoryException("Failed to bulk update status", e);
		}
	}
	
	public void updateCompWorkStatus(List<QTXCompWorkStatus> compWorkStatusList) throws RepositoryException
	{
		try
		{
			template.batchUpdate("update ar_qtx_comp_work_status set status=?,time_stamp=? where qtx_comp_wid=?", compWorkStatusList, this.commitSize, new ParameterizedPreparedStatementSetter<QTXCompWorkStatus>() {
				   public void setValues(PreparedStatement ps, QTXCompWorkStatus compWorkStatus) throws SQLException {
						ps.setInt(1, compWorkStatus.status.ordinal());
						ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
						ps.setLong(3, compWorkStatus.qtx_comp_wid);
				   }
				});
		}
		catch (Exception e)
		{
			throw new RepositoryException("Failed to bulk update status", e);
		}
	}
	
	public void updateWorkHSStatus(Collection<QTXWorkHS> hsList, TrackerCodes.QualtxHSPullStatus status) throws RepositoryException
	{
		try
		{
			template.batchUpdate("update ar_qtx_work_hs set status=?,time_stamp=? where qtx_hspull_wid=?", hsList, this.commitSize, new ParameterizedPreparedStatementSetter<QTXWorkHS>() {
				   public void setValues(PreparedStatement ps, QTXWorkHS workHS) throws SQLException {
						ps.setInt(1, status.ordinal());
						ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
						ps.setLong(3, workHS.qtx_hspull_wid);
				   }
				});
		}
		catch (Exception e)
		{
			throw new RepositoryException("Failed to bulk update status", e);
		}
	}
	
	public void updateCompWorkHSStatus(Collection <QTXCompWorkHS> compWorkHS, TrackerCodes.QualtxCompHSPullStatus status) throws RepositoryException
	{
		try
		{
			template.batchUpdate("update ar_qtx_comp_work_hs set status=?,time_stamp=? where qtx_comp_hspull_wid=?", compWorkHS, this.commitSize, new ParameterizedPreparedStatementSetter<QTXCompWorkHS>() {
				   public void setValues(PreparedStatement ps, QTXCompWorkHS compWorkHS) throws SQLException {
						ps.setInt(1, status.ordinal());
						ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
						ps.setLong(3, compWorkHS.qtx_comp_hspull_wid);
				   }
				});
		}
		catch (Exception e)
		{
			throw new RepositoryException("Failed to bulk update status", e);
		}
	}
	
	public void updateCompWorkIVAStatus(Collection <QTXCompWorkIVA> compIVAList, TrackerCodes.QualtxCompIVAPullStatus status) throws RepositoryException
	{
		try
		{
			template.batchUpdate("update ar_qtx_comp_work_iva set status=?,time_stamp=? where qtx_comp_iva_wid=?", compIVAList, this.commitSize, new ParameterizedPreparedStatementSetter<QTXCompWorkIVA>() {
				   public void setValues(PreparedStatement ps, QTXCompWorkIVA compWorkIVA) throws SQLException {
						ps.setInt(1, status.ordinal());
						ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
						ps.setLong(3, compWorkIVA.qtx_comp_iva_wid);
				   }
				});
		}
		catch (Exception e)
		{
			throw new RepositoryException("Failed to bulk update status", e);
		}
	}
	
//	public ArrayList<QTXWorkHS> loadWorkHS(long wid, Connection connection) throws RepositoryException
//	{
//		ArrayList<QTXWorkHS> workHSList = new ArrayList<QTXWorkHS>();
//		
//		try
//		{
//			PreparedStatement stmt = null;
//			
//			try
//			{
//				stmt = JDBCUtility.prepareStatement("select * from ar_qtx_work_hs where qtx_wid=?", connection);
//				
//				stmt.setLong(1, wid);
//				
//				ResultSet results = null;
//				try
//				{
//					results = stmt.executeQuery();
//					
//					while (results.next())
//					{
//						QTXWorkHS workHS = new QTXWorkHS();
//						
//						workHS.qtx_wid = results.getLong("QTX_WID");
//						workHS.qtx_hspull_wid = results.getLong("QTX_HSPULL_WID");
//						
//						Object status = results.getObject("STATUS");
//						
//						if (status != null)
//							workHS.status = TrackerCodes.QualtxHSPullStatus.values()[results.getInt("STATUS")];
//						
//						workHS.ctry_cmpl_key = results.getLong("CTRY_CMPL_KEY");
//						workHS.target_hs_ctry = results.getString("TARGET_HS_CTRY");
//						workHS.hs_number = results.getString("HS_NUMBER");
//						workHS.reason_code = results.getLong("REASON_CODE");
//						workHS.time_stamp = results.getTimestamp("TIME_STAMP");
//						
//						workHSList.add(workHS);
//					}
//				}
//				finally
//				{
//					JDBCUtility.closeResultSet(results);
//				}
//			}
//			finally
//			{
//				JDBCUtility.closeStatement(stmt);
//			}
//		}
//		catch (SQLException s)
//		{
//			throw new RepositoryException("Failed to load work hs for comp wid of " + wid, s);
//		}
//
//		return workHSList;
//	}
//	
//	public ArrayList<QTXCompWork> loadFullCompWorkforWork(long wid, Connection connection) throws RepositoryException
//	{
//		ArrayList<QTXCompWork> compWorkList = new ArrayList<QTXCompWork>();
//		
//		try
//		{
//			PreparedStatement stmt = null;
//			
//			try
//			{
//				stmt = JDBCUtility.prepareStatement("select * from ar_qtx_comp_work join ar_qtx_comp_work_status on ar_qtx_comp_work.qtx_comp_wid=ar_qtx_comp_work_status.qtx_comp_wid where qtx_wid=?", connection);
//				
//				stmt.setLong(1, wid);
//				
//				ResultSet results = null;
//				try
//				{
//					results = stmt.executeQuery();
//					
//					while (results.next())
//					{
//						QTXCompWork compWork = new QTXCompWork();
//						
//						compWork.qtx_wid = results.getLong("QTX_WID");
//						compWork.qtx_comp_wid = results.getLong("QTX_COMP_WID");
//						
//						Object status = results.getObject("STATUS");
//						if (status != null)
//							compWork.setWorkStatus(TrackerCodes.QualtxCompStatus.values()[results.getInt("STATUS")]);
//
//						compWork.priority = results.getInt("PRIORITY");
//
//						Object qualifier = results.getObject("QUALIFIER");
//						if (qualifier != null)
//							compWork.qualifier = TrackerCodes.QualtxCompQualifier.values()[results.getInt("QUALIFIER")];
//
//						compWork.bom_key = results.getLong("BOM_KEY");
//						compWork.bom_comp_key = results.getLong("BOM_COMP_KEY");
//						compWork.entity_key = results.getLong("ENTITY_KEY");
//						compWork.entity_src_key = results.getLong("ENTITY_SRC_KEY");
//						compWork.qualtx_key = results.getLong("QUALTX_KEY");
//						compWork.qualtx_comp_key = results.getLong("QUALTX_COMP_KEY");
//						compWork.reason_code = results.getLong("REASON_CODE");
//						compWork.time_stamp = results.getTimestamp("TIME_STAMP");
//						
//						compWork.compWorkHSList = this.loadCompWorkHSforCompWork(compWork.qtx_comp_wid, connection);
//						compWork.compWorkIVAList = this.loadCompWorkIVAforCompWork(compWork.qtx_comp_wid, connection);
//						
//						compWorkList.add(compWork);
//					}
//				}
//				finally
//				{
//					JDBCUtility.closeResultSet(results);
//				}
//			}
//			finally
//			{
//				JDBCUtility.closeStatement(stmt);
//			}
//		}
//		catch (SQLException s)
//		{
//			throw new RepositoryException("Failed to load all comp work for wid of " + wid, s);
//		}
//
//		return compWorkList;
//	}
//	
//	public ArrayList<QTXCompWorkHS> loadCompWorkHSforCompWork(long compWid, Connection connection) throws RepositoryException
//	{
//		ArrayList<QTXCompWorkHS> compWorkHSList = new ArrayList<QTXCompWorkHS>();
//		
//		try
//		{
//			PreparedStatement stmt = null;
//			
//			try
//			{
//				stmt = JDBCUtility.prepareStatement("select * from ar_qtx_comp_work_hs where qtx_comp_wid=?", connection);
//				
//				stmt.setLong(1, compWid);
//				
//				ResultSet results = null;
//				try
//				{
//					results = stmt.executeQuery();
//					
//					while (results.next())
//					{
//						QTXCompWorkHS compWorkHS = new QTXCompWorkHS();
//						
//						compWorkHS.qtx_wid = results.getLong("QTX_WID");
//						compWorkHS.qtx_comp_wid = results.getLong("QTX_COMP_WID");
//						compWorkHS.qtx_comp_hspull_wid = results.getLong("QTX_COMP_HSPULL_WID");
//						
//						Object status = results.getObject("STATUS");
//						
//						if (status != null)
//							compWorkHS.status = TrackerCodes.QualtxCompHSPullStatus.values()[results.getInt("STATUS")];
//						
//						compWorkHS.ctry_cmpl_key = results.getLong("CTRY_CMPL_KEY");
//						compWorkHS.target_hs_ctry = results.getString("TARGET_HS_CTRY");
//						compWorkHS.hs_number = results.getString("HS_NUMBER");
//						compWorkHS.reason_code = results.getLong("REASON_CODE");
//						compWorkHS.time_stamp = results.getTimestamp("TIME_STAMP");
//						
//						
//						compWorkHSList.add(compWorkHS);
//					}
//				}
//				finally
//				{
//					JDBCUtility.closeResultSet(results);
//				}
//			}
//			finally
//			{
//				JDBCUtility.closeStatement(stmt);
//			}
//		}
//		catch (SQLException s)
//		{
//			throw new RepositoryException("Failed to load comp work hs for comp wid of " + compWid, s);
//		}
//
//		return compWorkHSList;
//	}
//	
//	public ArrayList<QTXCompWorkIVA> loadCompWorkIVAforCompWork(long compWid, Connection connection) throws RepositoryException
//	{
//		ArrayList<QTXCompWorkIVA> compWorkIVAList = new ArrayList<QTXCompWorkIVA>();
//		
//		try
//		{
//			PreparedStatement stmt = null;
//			
//			try
//			{
//				stmt = JDBCUtility.prepareStatement("select * from ar_qtx_comp_work_iva where qtx_comp_wid=?", connection);
//				
//				stmt.setLong(1, compWid);
//				
//				ResultSet results = null;
//				try
//				{
//					results = stmt.executeQuery();
//					
//					while (results.next())
//					{
//						QTXCompWorkIVA compWorkIVA = new QTXCompWorkIVA();
//						
//						compWorkIVA.qtx_wid = results.getLong("QTX_WID");
//						compWorkIVA.qtx_comp_wid = results.getLong("QTX_COMP_WID");
//						compWorkIVA.qtx_comp_iva_wid = results.getLong("QTX_COMP_IVA_WID");
//						
//						Object status = results.getObject("STATUS");
//						
//						if (status != null)
//							compWorkIVA.status = TrackerCodes.QualtxCompIVAPullStatus.values()[results.getInt("STATUS")];
//						
//						compWorkIVA.iva_key = results.getLong("IVA_KEY");
//						compWorkIVA.reason_code = results.getLong("REASON_CODE");
//						compWorkIVA.time_stamp = results.getTimestamp("TIME_STAMP");
//
//						compWorkIVAList.add(compWorkIVA);
//					}
//				}
//				finally
//				{
//					JDBCUtility.closeResultSet(results);
//				}
//			}
//			finally
//			{
//				JDBCUtility.closeStatement(stmt);
//			}
//		}
//		catch (SQLException s)
//		{
//			throw new RepositoryException("Failed to load comp work iva for comp wid of " + compWid, s);
//		}
//
//		return compWorkIVAList;
//	}
}
