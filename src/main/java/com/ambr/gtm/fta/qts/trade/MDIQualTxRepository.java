package com.ambr.gtm.fta.qts.trade;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ambr.gtm.fta.qts.RepositoryException;
import com.ambr.gtm.fta.qts.util.JDBCUtility;
import com.ambr.gtm.fta.trade.client.LockException;
import com.ambr.gtm.fta.trade.model.BOMQualAuditColumn;
import com.ambr.gtm.fta.trade.model.BOMQualAuditEntity;
import com.ambr.platform.uoid.UniversalObjectIDGenerator;

public class MDIQualTxRepository
{
	private UniversalObjectIDGenerator idGenerator;

	public MDIQualTxRepository(UniversalObjectIDGenerator idGenerator)
	{
		this.idGenerator = idGenerator;
	}
	
//	public MDIQualTx createQualTx(String orgCode) throws RepositoryException
//	{
//		MDIQualTx qualTx = new MDIQualTx();
//		
//		try
//		{
//			qualTx.alt_key_qualtx = this.idGenerator.generate().getLSB();
//			qualTx.tx_id = "" + this.idGenerator.generate().getLSB();
//			qualTx.created_date = new Timestamp(System.currentTimeMillis());
//			qualTx.last_modified_date = qualTx.created_date;
//			qualTx.org_code = orgCode;
//		}
//		catch (Exception s)
//		{
//			throw new RepositoryException("Failed to create qual tx record", s);
//		}
//		
//		return qualTx;
//	}
//	
//	public MDIQualTx getQualTxPrimaryKey(long altKeyQualtx, Connection connection) throws RepositoryException
//	{
//		MDIQualTx qualTx = new MDIQualTx();
//		
//		try
//		{
//			PreparedStatement stmt = null;
//			
//			try
//			{
//				stmt = JDBCUtility.prepareStatement("select org_code,tx_id from mdi_qualtx where alt_key_qualtx=?", connection);
//				
//				stmt.setLong(1, altKeyQualtx);
//				
//				ResultSet results = null;
//				try
//				{
//					results = stmt.executeQuery();
//					
//					if (results.next())
//					{
//						qualTx.alt_key_qualtx = altKeyQualtx;
//						qualTx.org_code = results.getString(1);
//						qualTx.tx_id = results.getString(2);
//					}
//					else
//					{
//						throw new RepositoryException("qualtx record does not exist, alt key of " + altKeyQualtx);
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
//			throw new RepositoryException("Failed to load prod qualtx alt key of " + altKeyQualtx, s);
//		}
//		
//		return qualTx;
//	}
	
//	public MDIQualTxComp createQualTxComp(MDIQualTx qualTx) throws RepositoryException
//	{
//		MDIQualTxComp qualTxComp = new MDIQualTxComp();
//		
//		try
//		{
//			qualTxComp.org_code = qualTx.org_code;
//			qualTxComp.tx_id = qualTx.tx_id;
//			qualTxComp.alt_key_qualtx = qualTx.alt_key_qualtx;
//			qualTxComp.comp_id = "" + this.idGenerator.generate().getLSB();
//			qualTxComp.alt_key_comp = this.idGenerator.generate().getLSB();
//			qualTxComp.created_date = new Timestamp(System.currentTimeMillis());
//			qualTxComp.last_modified_date = qualTxComp.created_date;
//		}
//		catch (Exception s)
//		{
//			throw new RepositoryException("Failed to create qual tx comp record", s);
//		}
//		
//		return qualTxComp;
//	}
	
//	public void storeQualTx(MDIQualTx qualTx, Connection connection) throws RepositoryException
//	{
//		ArrayList<MDIQualTx> qualTxList = new ArrayList<MDIQualTx>();
//		
//		qualTxList.add(qualTx);
//		
//		storeQualTx(qualTxList, connection);
//	}
//	
//	public void storeQualTx(ArrayList<MDIQualTx> qualTxList, Connection connection) throws RepositoryException
//	{
//		RepositoryException primaryException = null;
//		PreparedStatement stmt = null;
//		
//		try
//		{
//			stmt = JDBCUtility.prepareStatement("insert into mdi_qualtx (tx_id,org_code,alt_key_qualtx,"
//					+ "fta_code,CTRY_OF_IMPORT,CTRY_OF_MANUFACTURE,CURRENCY_CODE,EFFECTIVE_FROM,EFFECTIVE_TO,VALUE,COST,"
//					+ "SRC_ID,SRC_KEY,DESCRIPTION,GROSS_WEIGHT,UOM,AREA,AREA_UOM,DIRECT_PROCESSING_COST,created_date,created_by,last_modified_date,last_modified_by) "
//					+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", connection);
//			
//			for (int index=0; index<qualTxList.size(); index++)
//			{
//				MDIQualTx qualTx = qualTxList.get(index);
//				stmt.setString(1, qualTx.tx_id);
//				stmt.setString(2, qualTx.org_code);
//				stmt.setLong(3, qualTx.alt_key_qualtx);
//				
//				stmt.setString(4, qualTx.fta_code);
//				stmt.setString(5, qualTx.ctry_of_import);
//				stmt.setString(6, qualTx.ctry_of_manufacture);
//				stmt.setString(7, qualTx.currency_code);
//				stmt.setDate(8, qualTx.effective_from);
//				stmt.setDate(9, qualTx.effective_to);
//				stmt.setDouble(10, qualTx.value);
//				stmt.setDouble(11, qualTx.cost);
//				stmt.setString(12,qualTx.src_id);
//				stmt.setLong(13, qualTx.src_key);
//				stmt.setString(14, qualTx.description);
//				stmt.setDouble(15, qualTx.gross_weight);
//				stmt.setString(16, qualTx.uom);
//				stmt.setDouble(17, qualTx.area);
//				stmt.setString(18, qualTx.area_uom);
//				stmt.setDouble(19, qualTx.direct_processing_cost);
//				
//				stmt.setTimestamp(20, qualTx.created_date);
//				stmt.setString(21, qualTx.created_by);
//				stmt.setTimestamp(22, qualTx.last_modified_date);
//				stmt.setString(23, qualTx.last_modified_by);
//				
//				stmt.addBatch();
//			}
//			
//			JDBCUtility.executeBatch(stmt);
//		}
//		catch (Exception e)
//		{
//			primaryException = new RepositoryException("Failed to insert qualtx record, attempted to insert " + qualTxList.size() + " records", e);
//			
//			throw primaryException;
//		}
//		finally
//		{
//			JDBCUtility.closeStatement(stmt);
//		}
//	}
//	
//	public void storeQualTxComp(MDIQualTxComp qualTxComp, Connection connection) throws RepositoryException
//	{
//		ArrayList<MDIQualTxComp> list = new ArrayList<MDIQualTxComp>();
//		
//		list.add(qualTxComp);
//		
//		storeQualTxComp(list, connection);
//	}
//	
//	public void storeQualTxComp(ArrayList<MDIQualTxComp> qualTxCompList, Connection connection) throws RepositoryException
//	{
//		RepositoryException primaryException = null;
//		PreparedStatement stmt = null;
//
//		try
//		{
//			stmt = JDBCUtility.prepareStatement("insert into mdi_qualtx_comp ("
//					+ "tx_id,"
//					+ "org_code,"
//					+ "alt_key_qualtx,"
//					+ "comp_id,"
//					+ "alt_key_comp,"
//					+ "area,"
//					+ "area_uom,"
//					+ "component_type,"
//					+ "cost,"
//					+ "critical_indicator,"
//					+ "ctry_of_manufacture,"
//					+ "ctry_of_origin,"
//					+ "cumulation_currency,"
//					+ "cumulation_value,"
//					+ "description,"
//					+ "essential_character,"
//					+ "hs_num,"
//					+ "make_buy_flg,"
//					+ "qualified_flg,"
//					+ "qualified_from,"
//					+ "qualified_to,"
//					+ "source_of_data,"
//					+ "src_id,"
//					+ "src_key,"
//					+ "traced_value,"
//					+ "weight,"
//					+ "weight_uom,"
//					+ "top_down_ind,"
//					+ "raw_material_ind,"
//					+ "intermediate_ind,"
//					+ "prod_key,"
//					+ "prod_src_key,"
//					+ "created_date,"
//					+ "created_by,"
//					+ "last_modified_date,"
//					+ "last_modified_by"
//					+ ") values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", connection);
//			
//			for (int index=0; index<qualTxCompList.size(); index++)
//			{
//				MDIQualTxComp qualTxComp = qualTxCompList.get(index);
//				
//				stmt.setString(1, qualTxComp.tx_id);
//				stmt.setString(2, qualTxComp.org_code);
//				stmt.setLong(3, qualTxComp.alt_key_qualtx);
//				stmt.setString(4, qualTxComp.comp_id);
//				stmt.setLong(5, qualTxComp.alt_key_comp);
//				stmt.setDouble(6, qualTxComp.area);
//				stmt.setString(7, qualTxComp.area_uom);
//				stmt.setString(8, qualTxComp.component_type);
//				stmt.setDouble(9, qualTxComp.cost);
//				stmt.setString(10, qualTxComp.critical_indicator);
//				stmt.setString(11, qualTxComp.ctry_of_manufacture);
//				stmt.setString(12, qualTxComp.ctry_of_origin);
//				stmt.setString(13, qualTxComp.cumulation_currency);
//				stmt.setDouble(14, qualTxComp.cumulation_value);
//				stmt.setString(15, qualTxComp.description);
//				stmt.setString(16, qualTxComp.essential_character);
//				stmt.setString(17, qualTxComp.hs_num);
//				stmt.setString(18, qualTxComp.make_buy_flg);
//				stmt.setString(19, qualTxComp.qualified_flg);
//				stmt.setDate(20, qualTxComp.qualified_from);
//				stmt.setDate(21, qualTxComp.qualified_to);
//				stmt.setString(22, qualTxComp.source_of_data);
//				stmt.setLong(23, qualTxComp.src_id);
//				stmt.setLong(24, qualTxComp.src_key);
//				stmt.setDouble(25, qualTxComp.traced_value);
//				stmt.setDouble(26, qualTxComp.weight);
//				stmt.setString(27, qualTxComp.weight_uom);
//				stmt.setDouble(28, qualTxComp.top_down_ind);
//				stmt.setDouble(29, qualTxComp.raw_materia_ind);
//				stmt.setDouble(30, qualTxComp.intermediate_ind);
//				
//				stmt.setLong(31, qualTxComp.prod_key);
//				stmt.setLong(32, qualTxComp.prod_src_key);
//				
//				stmt.setTimestamp(33, qualTxComp.created_date);
//				stmt.setString(34, qualTxComp.created_by);
//				stmt.setTimestamp(35, qualTxComp.last_modified_date);
//				stmt.setString(36, qualTxComp.last_modified_by);
//				
//				stmt.addBatch();
//			}
//			
//			JDBCUtility.executeBatch(stmt);
//		}
//		catch (Exception e)
//		{
//			primaryException = new RepositoryException("Failed to insert qualtx comp record, attempted to store " + qualTxCompList.size() + " records", e);
//			
//			throw primaryException;
//		}
//		finally
//		{
//			JDBCUtility.closeStatement(stmt);
//		}
//	}
//	
//	public int updateQualTxCompHSNumber(long qualTxCompKey, String hsNumber, Connection connection) throws RepositoryException
//	{
//		RepositoryException primaryException = null;
//		PreparedStatement stmt = null;
//		
//		try
//		{
//			stmt = JDBCUtility.prepareStatement("UPDATE mdi_qualtx_comp SET HS_NUM=?,last_modified_date=? WHERE alt_key_comp=?", connection);
//			
//			stmt.setString(1, hsNumber);
//			stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
//			stmt.setLong(3, qualTxCompKey);
//			
//			long time=System.currentTimeMillis();
//						
//			int updateCount = JDBCUtility.executeUpdate(stmt);
//			
//			System.out.println("UPDATE mdi_qualtx_comp SET HS_NUM=? WHERE alt_key_comp=?\t" + hsNumber + "\t" + qualTxCompKey + "\t" + (System.currentTimeMillis() - time));
//			
//			if (updateCount == 0)
//			{
//				throw new RepositoryException("Record does not exist, invalid alt_key_comp of " + qualTxCompKey);
//			}
//			
//			return updateCount;
//		}
//		catch (Exception e)
//		{
//			primaryException = new RepositoryException("Failed to update qualtx comp record with hs_number, alt comp key[" + qualTxCompKey + "]", e);
//			
//			throw primaryException;
//		}
//		finally
//		{
//			JDBCUtility.closeStatement(stmt);
//		}
//	}

//	public int updateQualTxCompIVAData(MDIQualTxComp qualtxComp, Connection connection) throws RepositoryException
//	{
//		RepositoryException primaryException = null;
//		PreparedStatement stmt = null;
//		
//		try
//		{
//			stmt = JDBCUtility.prepareStatement(
//			"UPDATE mdi_qualtx_comp " +
//			"SET " +
//				"QUALIFIED_FLG=?,  " +
//				"QUALIFIED_FROM=?,  " +
//				"QUALIFIED_TO=?,  " +
//				"CUMULATION_VALUE=?, " +
//				"CUMULATION_CURRENCY=?, " +
//				"CUMULATION_CTRY_LIST=?, " +
//				"CAMPAIGN_ID=?, " +
//				"RESPONSE_ID=?, " +
//				"TRACE_CAMPAIGN_ID=?, " +
//				"TRACE_RESPONSE_ID=?, " +
//				"SUB_PULL_CTRY=?, " +
//				"CUMULATION_RULE_APPLIED=?, " +
//				"PREV_YEAR_QUAL_APPLIED=?, " +
//				"CUMULATION_RULE_FTA_USED=?,  " +
//				"last_modified_by=?,  " +
//				"last_modified_date=?  " +
//			"WHERE alt_key_comp=?", connection);
//			
//			stmt.setString(1, qualtxComp.qualified_flg);
//			stmt.setDate(2, qualtxComp.qualified_from);
//			stmt.setDate(3, qualtxComp.qualified_to);
//			stmt.setDouble(4, qualtxComp.cumulation_value);
//			stmt.setString(5, qualtxComp.cumulation_currency);
//			
//			//TODO CUMULATION_CTRY_LIST column does not exit in mdi_qualtx_comp
//			stmt.setString(6, null);
//			
//			//TODO CAMPAIGN_ID column does not exist in mdi_qualtx_comp
//			stmt.setString(7, null);
//			
//			//TODO RESPONSE_ID column does not exist in mdi_qualtx_comp
//			stmt.setString(8, null);
//			
//			//TODO TRACE_CAMPAIGN_ID column does not exist in mdi_qualtx_comp
//			stmt.setString(9, null);
//			
//			//TODO TRACE_RESPONSE_ID column does not exist in mdi_qualtx_comp
//			stmt.setString(10, null);
//			
//			//TODO SUB_PULL_CTRY column does not exist in mdi_qualtx_comp
//			stmt.setString(11, null);
//			
//			//TODO CUMULATION_RULE_APPLIED column does not exist in mdi_qualtx_comp
//			stmt.setString(12, null);
//			
//			//TODO PREV_YEAR_QUAL_APPLIED column does not exist in mdi_qualtx_comp
//			stmt.setString(13, null);
//			
//			//TODO CUMULATION_RULE_FTA_USED column does not exist in mdi_qualtx_comp
//			stmt.setString(14, null);
//			
//			stmt.setString(15, "SYSTEM");
//			stmt.setTimestamp(16, new Timestamp(System.currentTimeMillis()));
//			
//			stmt.setLong(17, qualtxComp.alt_key_comp);
//			
//			//TODO uncomment this when sql is ready
////			int updateCount = JDBCUtility.executeUpdate(stmt);
////			
////			if (updateCount == 0)
////			{
////				throw new RepositoryException("Record does not exist, invalid alt_key_comp of " + qualtxComp.alt_key_comp);
////			}
//			return 1;
//		}
//		catch (Exception e)
//		{
//			primaryException = new RepositoryException("Failed to update qualtx comp record with iva data, alt comp key[" + qualtxComp.alt_key_comp + "]", e);
//			
//			throw primaryException;
//		}
//		finally
//		{
//			JDBCUtility.closeStatement(stmt);
//		}
//	}
//
//	public int clearQualTxCtryCmplDtls(long qualTxKey, Connection connection) throws RepositoryException
//	{
//		RepositoryException primaryException = null;
//		PreparedStatement stmt = null;
//		
//		try
//		{
//			stmt = JDBCUtility.prepareStatement("UPDATE mdi_qualtx "
//					+ "SET "
//					+ "hs_num=?, "
//					+ "sub_pull_ctry = ?, "
//					+ "prod_ctry_cmpl_key = ? "
//					+ "where alt_key_qualtx=? ", connection);
//			
//				stmt.setNull(1, java.sql.Types.VARCHAR);
//				stmt.setNull(2, java.sql.Types.VARCHAR);
//				stmt.setNull(3, java.sql.Types.DOUBLE);
//				stmt.setLong(4, qualTxKey);
//			int updateCount = JDBCUtility.executeUpdate(stmt);
//			
//			if (updateCount == 0)
//			{
//				throw new RepositoryException("Record does not exist, invalid alt_key_qualtx of " + qualTxKey);
//			}
//			
//			return updateCount;
//		}
//		catch (Exception e)
//		{
//			primaryException = new RepositoryException("Failed to update qualtx record with hs_number, qualtx key[" + qualTxKey + "]", e);
//			
//			throw primaryException;
//		}
//		finally
//		{
//			JDBCUtility.closeStatement(stmt);
//		}
//	}
//	
//	public int updateQualTxHSNumber(long qualTxKey, String hsNumber, Connection connection) throws RepositoryException
//	{
//		RepositoryException primaryException = null;
//		PreparedStatement stmt = null;
//		
//		try
//		{
//			stmt = JDBCUtility.prepareStatement("UPDATE mdi_qualtx SET HS_NUM=? WHERE alt_key_qualtx=?", connection);
//			
//			stmt.setString(1, hsNumber);
//			stmt.setLong(2, qualTxKey);
//			
//			int updateCount = JDBCUtility.executeUpdate(stmt);
//			
//			if (updateCount == 0)
//			{
//				throw new RepositoryException("Record does not exist, invalid alt_key_qualtx of " + qualTxKey);
//			}
//			
//			return updateCount;
//		}
//		catch (Exception e)
//		{
//			primaryException = new RepositoryException("Failed to update qualtx record with hs_number, qualtx key[" + qualTxKey + "]", e);
//			
//			throw primaryException;
//		}
//		finally
//		{
//			JDBCUtility.closeStatement(stmt);
//		}
//	}
//	
//	public int updateQualTx(long qualTxKey, String columnName, boolean columnValue, Connection connection) throws RepositoryException
//	{
//		RepositoryException primaryException = null;
//		PreparedStatement stmt = null;
//		
//		try
//		{
//			stmt = JDBCUtility.prepareStatement("UPDATE mdi_qualtx SET "+columnName+"=? WHERE alt_key_qualtx=?", connection);
//			
//			stmt.setBoolean(1, columnValue);
//			stmt.setLong(2, qualTxKey);
//			
//			int updateCount = JDBCUtility.executeUpdate(stmt);
//			
//			if (updateCount == 0)
//			{
//				throw new RepositoryException("Record does not exist, invalid alt_key_qualtx of " + qualTxKey);
//			}
//			
//			return updateCount;
//		}
//		catch (Exception e)
//		{
//			primaryException = new RepositoryException("Failed to update qualtx record with "+columnName +", qualtx key[" + qualTxKey + "]", e);
//			
//			throw primaryException;
//		}
//		finally
//		{
//			JDBCUtility.closeStatement(stmt);
//		}
//	}
	
	
//	public void deleteQualTxComp(Long altkeycomp, Connection connection) throws RepositoryException
//	{
//		RepositoryException primaryException = null;
//		PreparedStatement stmt = null;
//		
//		if (altkeycomp <= 0 ) return;
//		
//		try
//		{
//			stmt = JDBCUtility.prepareStatement(" delete from MDI_QUALTX_COMP where ALT_KEY_COMP=?", connection);
//			stmt.setLong(1, altkeycomp);
//			JDBCUtility.executeUpdate(stmt);
//		}
//		catch (Exception e)
//		{
//			primaryException = new RepositoryException("Failed to delete the qualtx comp records from mdi_qualtx_comp", e);
//			
//			throw primaryException;
//		}
//		finally {
//			JDBCUtility.closeStatement(stmt);
//		}
//	}
	
//	public int clearQualTxCompCtryCmplDtls(long qualTxCompKey, Connection connection) throws RepositoryException
//	{
//		RepositoryException primaryException = null;
//		PreparedStatement stmt = null;
//		
//		try
//		{
//			stmt = JDBCUtility.prepareStatement("UPDATE mdi_qualtx_comp "
//					+ "SET "
//					+ "hs_num=?, "
//					+ "sub_pull_ctry = ?, "
//					+ "prod_ctry_cmpl_key = ? "
//					+ "where alt_key_comp=? ", connection);
//			
//				stmt.setNull(1, java.sql.Types.VARCHAR);
//				stmt.setNull(2, java.sql.Types.VARCHAR);
//				stmt.setNull(3, java.sql.Types.NUMERIC);
//				stmt.setLong(4, qualTxCompKey);
//			int updateCount = JDBCUtility.executeUpdate(stmt);
//			
//			if (updateCount == 0)
//			{
//				throw new RepositoryException("Record does not exist, invalid alt_key_qualtx of " + qualTxCompKey);
//			}
//			
//			return updateCount;
//		}
//		catch (Exception e)
//		{
//			primaryException = new RepositoryException("Failed to update qualtx record with hs_number, qualtx key[" + qualTxCompKey + "]", e);
//			
//			throw primaryException;
//		}
//		finally
//		{
//			JDBCUtility.closeStatement(stmt);
//		}
//	}
//	
//	public int updateQualTxCtryCmplDtls(long qualTxKey, MDIProdCtryCmpl mdiProdCtryCmpl, Connection connection) throws RepositoryException
//	{
//		RepositoryException primaryException = null;
//		PreparedStatement stmt = null;
//		
//		try
//		{
//			stmt = JDBCUtility.prepareStatement("UPDATE mdi_qualtx "
//					+ "SET "
//					+ "hs_num=?, "
//					+ "sub_pull_ctry = ?, "
//					+ "prod_ctry_cmpl_key = ? "
//					+ "where alt_key_qualtx=? ", connection);
//			
//				stmt.setString(1, mdiProdCtryCmpl.hs_number);
//				stmt.setString(2, mdiProdCtryCmpl.target_hs_ctry);
//				stmt.setLong(3, mdiProdCtryCmpl.ctry_cmpl_key);
//				stmt.setLong(4, qualTxKey);
//			int updateCount = JDBCUtility.executeUpdate(stmt);
//			
//			if (updateCount == 0)
//			{
//				throw new RepositoryException("Record does not exist, invalid alt_key_qualtx of " + qualTxKey);
//			}
//			
//			return updateCount;
//		}
//		catch (Exception e)
//		{
//			primaryException = new RepositoryException("Failed to update qualtx record for the ctry_cmpl_key: "+mdiProdCtryCmpl.ctry_cmpl_key+", qualtx key[" + qualTxKey + "]", e);
//			
//			throw primaryException;
//		}
//		finally
//		{
//			JDBCUtility.closeStatement(stmt);
//		}
//	}
//	
//
//	public int updateQualTxCompCtryCmplDtls(long qualTxCompKey, MDIProdCtryCmpl mdiProdCtryCmpl, Connection connection) throws RepositoryException
//	{
//		RepositoryException primaryException = null;
//		PreparedStatement stmt = null;
//		
//		try
//		{
//			stmt = JDBCUtility.prepareStatement("UPDATE mdi_qualtx_comp "
//					+ "SET "
//					+ "hs_num=?, "
//					+ "sub_pull_ctry = ?, "
//					+ "prod_ctry_cmpl_key = ? "
//					+ "where alt_key_comp=? ", connection);
//			
//				stmt.setString(1, mdiProdCtryCmpl.hs_number);
//				stmt.setString(2, mdiProdCtryCmpl.target_hs_ctry);
//				stmt.setLong(3, mdiProdCtryCmpl.ctry_cmpl_key);
//				stmt.setLong(4, qualTxCompKey);
//			int updateCount = JDBCUtility.executeUpdate(stmt);
//			
//			if (updateCount == 0)
//			{
//				throw new RepositoryException("Record does not exist, invalid alt_key_qualtx of " + qualTxCompKey);
//			}
//			
//			return updateCount;
//		}
//		catch (Exception e)
//		{
//			primaryException = new RepositoryException("Failed to update qualtx_comp record for the ctry_cmpl_key: "+mdiProdCtryCmpl.ctry_cmpl_key+", qualtx key[" + qualTxCompKey + "]", e);
//			
//			throw primaryException;
//		}
//		finally
//		{
//			JDBCUtility.closeStatement(stmt);
//		}
//	}
//	
//	
//	public int updateQualTxCompFinalDecision(long qualTxCompKey, boolean isQualified, Connection connection) throws RepositoryException
//	{
//		RepositoryException primaryException = null;
//		PreparedStatement stmt = null;
//		
//		try
//		{
//			stmt = JDBCUtility.prepareStatement("update mdi_qualtx_comp "
//					+ "set "
//					+ "qualified_flg=? "
//					+ "where alt_key_comp=? ", connection);
//				
//				
//				stmt.setString(1, (isQualified) ? "QUALIFIED": "NOT_QUALIFIED");
//				stmt.setLong(2, qualTxCompKey);
//				
//			int updateCount = JDBCUtility.executeUpdate(stmt);
//			
//			if (updateCount == 0)
//			{
//				throw new RepositoryException("Record does not exist in the table qualtx_comp of " + qualTxCompKey);
//			}
//			
//			return updateCount;
//		}
//		catch (Exception e)
//		{
//			primaryException = new RepositoryException("Failed to update the final Decision for qualtx_comp record, qualtx comp key[" + qualTxCompKey + "]", e);
//			
//			throw primaryException;
//		}
//		finally
//		{
//			JDBCUtility.closeStatement(stmt);
//		}
//	}
//	
//	public int clearQualtxProdSrcDetails(long qualTxKey, Connection connection) throws RepositoryException
//	{
//		RepositoryException primaryException = null;
//		PreparedStatement stmt = null;
//		
//		try
//		{
//			stmt = JDBCUtility.prepareStatement("update mdi_qualtx "
//					+ "set "
//					+ "prod_src_key=?, "
//					+ "supplier_key=?, "
//					+ "manufacturer_key= ? "
//					+ "where alt_key_qualtx =? ", connection);
//				
//				stmt.setNull(1, java.sql.Types.DOUBLE);
//				stmt.setNull(2, java.sql.Types.DOUBLE);
//				stmt.setNull(3, java.sql.Types.DOUBLE);
//				stmt.setLong(4, qualTxKey);
//			int updateCount = JDBCUtility.executeUpdate(stmt);
//			
//			if (updateCount == 0)
//			{
//				throw new RepositoryException("Record does not exist in the table qualtx of " + qualTxKey);
//			}
//			
//			return updateCount;
//		}
//		catch (Exception e)
//		{
//			primaryException = new RepositoryException("Failed to update Null values for source related columns of, qualtx key[" + qualTxKey + "]", e);
//			
//			throw primaryException;
//		}
//		finally
//		{
//			JDBCUtility.closeStatement(stmt);
//		}
//	}
//	
//	public int clearQualtxCompProdSrcIvaKey(long qualTxCompKey, Connection connection) throws RepositoryException
//	{
//		RepositoryException primaryException = null;
//		PreparedStatement stmt = null;
//		
//		try
//		{
//			stmt = JDBCUtility.prepareStatement("update mdi_qualtx_comp "
//					+ "set "
//					+ "prod_src_key=?, "
//					+ "supplier_key=?, "
//					+ "manufacturer_key=?, "
//					+ "prod_src_iva_key=? "
//					+ "where alt_key_comp=? ", connection);
//				
//				stmt.setNull(1, java.sql.Types.DOUBLE);
//				stmt.setNull(2, java.sql.Types.DOUBLE);
//				stmt.setNull(3, java.sql.Types.DOUBLE);
//				stmt.setNull(4, java.sql.Types.DOUBLE);
//				stmt.setLong(5, qualTxCompKey);
//				
//			int updateCount = JDBCUtility.executeUpdate(stmt);
//			
//			if (updateCount == 0)
//			{
//				throw new RepositoryException("Record does not exist in the table qualtx_comp of " + qualTxCompKey);
//			}
//			
//			return updateCount;
//		}
//		catch (Exception e)
//		{
//			primaryException = new RepositoryException("Failed to update the Null values related to prod source columns of qualtx_comp record of , qualtx comp key[" + qualTxCompKey + "]", e);
//			
//			throw primaryException;
//		}
//		finally
//		{
//			JDBCUtility.closeStatement(stmt);
//		}
//	}
//	
//	public int clearQualtxCompIvaKey(long qualTxCompKey, Connection connection) throws RepositoryException
//	{
//		RepositoryException primaryException = null;
//		PreparedStatement stmt = null;
//		
//		try
//		{
//			stmt = JDBCUtility.prepareStatement("update mdi_qualtx_comp "
//					+ "set "
//					+ "prod_src_iva_key=? "
//					+ "where alt_key_comp=? ", connection);
//				
//				stmt.setNull(1, java.sql.Types.DOUBLE);
//				stmt.setLong(2, qualTxCompKey);
//			int updateCount = JDBCUtility.executeUpdate(stmt);
//			
//			if (updateCount == 0)
//			{
//				throw new RepositoryException("Record does not exist in the table qualtx_comp of " + qualTxCompKey);
//			}
//			
//			return updateCount;
//		}
//		catch (Exception e)
//		{
//			primaryException = new RepositoryException("Failed to update the Null value for prodSrcIvaKey column of qualtx_comp record of , qualtx comp key[" + qualTxCompKey + "]", e);
//			
//			throw primaryException;
//		}
//		finally
//		{
//			JDBCUtility.closeStatement(stmt);
//		}
//	}
//	
//	public void updateQualTx(long bomkey, BOMQualAuditEntity bomQualAuditEntity, Connection connection) throws Exception
//	{
//		
//		executeUpdate(bomQualAuditEntity, connection);
//					
//		List<BOMQualAuditEntity> bomQualAuditChildEntityList = bomQualAuditEntity.getChildTables();
//		
//		for(BOMQualAuditEntity bomQualAuditChildEntity : bomQualAuditChildEntityList)
//		{
//			if(bomQualAuditChildEntity.getState() == BOMQualAuditEntity.STATE.MODIFY) //update child works qualtx_comp
//			{
//				executeUpdate(bomQualAuditChildEntity, connection);
//			}
//			else if(bomQualAuditChildEntity.getState() == BOMQualAuditEntity.STATE.CREATE) //insert qualtx_comp
//			{
//				executeInsert(bomQualAuditChildEntity, bomQualAuditEntity.getAltKey(), connection);
//			}
//			else if(bomQualAuditChildEntity.getState() == BOMQualAuditEntity.STATE.DELETE) //delete qualtx_price, qualtx_comp_price,qualtx_comp
//			{
//				executedelete(bomQualAuditChildEntity, connection);
//			}
//		}
//		
//		MDIBomRepository.deleteBomQualRecords(bomkey, bomQualAuditEntity.getQtxkeysToDeleteBomQual(), bomQualAuditEntity.getOrgCode(), bomQualAuditEntity.getUserID(), connection);
//		
//	}
//	
//	private void executeUpdate(BOMQualAuditEntity bomQualAuditEntity, Connection connection ) throws RepositoryException
//	 {
//		
//		RepositoryException primaryException = null;
//		PreparedStatement stmt = null;
//		
//		Long surrogateKey = bomQualAuditEntity.getAltKey();
//		
//		
//		HashMap<String, BOMQualAuditColumn> modifiedColumns = bomQualAuditEntity.getModifiedColumns();
//		if(modifiedColumns.isEmpty()) return;
//		String tableName = bomQualAuditEntity.getTableName();
//		String surrogateKeyColumn = bomQualAuditEntity.getSurrogateKeyColumn();
//		StringBuilder sql = new StringBuilder("update " + tableName + " set " );
//		List<Object> objectList = new ArrayList<Object>();
//		for (Map.Entry<String, BOMQualAuditColumn> entry : modifiedColumns.entrySet())
//		{
//			sql.append(entry.getKey()+ " =?,");
//			objectList.add(entry.getValue().newValue);
//		}
//		
//		String updateSql = sql.toString().replaceAll(".$", ""); 
//		StringBuilder whereClause = new StringBuilder(" where " + surrogateKeyColumn + " = ? " );
//		
//		try
//		{
//			stmt = JDBCUtility.prepareStatement(updateSql.toString() + whereClause.toString(),connection);
//			int i =0;
//			for (i=0 ; i < objectList.size(); i++)
//			{
//				Object object = objectList.get(i);
//				
//				//TODO this is occurring due to the BOM object java.util.Date properties initially created as java.sql.Date/Timestamp but converted to java.util.Date during JSON deserialization
//				if (object != null && object.getClass() == java.util.Date.class)
//				{
//					object = new java.sql.Timestamp(((java.util.Date) object).getTime());
//				}
//				
//				stmt.setObject(i+1, object);
//			}
//			
//			stmt.setObject(i+1, surrogateKey);
//			
//			int updateCount = JDBCUtility.executeUpdate(stmt);
//			if (updateCount == 0)
//			{
//					throw new RepositoryException("Record does not exist, invalid "+surrogateKeyColumn+" of " + surrogateKey);
//			}
//			
//		}
//		
//		catch (Exception e)
//		{
//			primaryException = new RepositoryException("Failed to update "+ tableName +", attempted to update " + surrogateKey + " record", e);
//			
//			throw primaryException;
//		}
//		finally
//		{
//			JDBCUtility.closeStatement(stmt);
//		}
//		
//	}
//	
//private int executeInsert( BOMQualAuditEntity bomQualAuditEntity, Long qualtxKey, Connection connection) throws RepositoryException {
//		
//		RepositoryException primaryException = null;
//		PreparedStatement stmt = null;
//		
//		HashMap<String, BOMQualAuditColumn> modifiedColumns = bomQualAuditEntity.getModifiedColumns();
//		String tableName = bomQualAuditEntity.getTableName();
//		String flexName = bomQualAuditEntity.getFlexConfigName();
//		
//		if(tableName.equalsIgnoreCase(MDIQualTxPrice.TABLE_NAME) || tableName.equalsIgnoreCase(MDIQualTxCompPrice.TABLE_NAME))
//			deletePrice(bomQualAuditEntity, qualtxKey, connection);
//		
//		if(flexName != null && flexName.equalsIgnoreCase("IMPL_BOM_PROD_FAMILY:TEXTILES"))
//		{
//			executedeleteYarnDetails(qualtxKey, bomQualAuditEntity.getParentAltkey(), connection);
//		}
//		
//		StringBuilder sql = new StringBuilder("insert into " + tableName + " (") ;
//		StringBuilder sqlValues = new StringBuilder(" values ( ") ;
//		List<Object> objectList = new ArrayList<Object>();
//		
//		for (Map.Entry<String, BOMQualAuditColumn> entry : modifiedColumns.entrySet())
//		{
//			sql.append(entry.getKey()+ ",");
//			sqlValues.append("?,");
//			objectList.add(entry.getValue().newValue);
//		}
//		sql.append(" source_of_data ) ");
//		sqlValues.append("? ) ");
//		
//		try
//		{
//			stmt = JDBCUtility.prepareStatement(sql.toString()+sqlValues.toString(),connection);
//			int i =0;
//			for (i=0 ; i < objectList.size(); i++)
//			{
//				stmt.setObject(i+1, objectList.get(i));
//			}
//			
//			stmt.setString(i+1, "REQUAL");
//			
//			int updateCount = JDBCUtility.executeUpdate(stmt);
//			if (updateCount == 0)
//			{
//					throw new RepositoryException("Failed to insert record for the table" +tableName);
//			}
//			
//			return updateCount;
//		}
//		
//		catch (Exception e)
//		{
//			primaryException = new RepositoryException("Failed to insert record for the table  "+ tableName +", for the record " + tableName, e);
//			
//			throw primaryException;
//		}
//		finally
//		{
//			JDBCUtility.closeStatement(stmt);
//		}
//		
//	}
//
//	private void executedelete(BOMQualAuditEntity bomQualAuditEntity, Connection connection)
//		throws RepositoryException
//	{
//			RepositoryException primaryException;
//			PreparedStatement stmt = null;
//			String tableName =  bomQualAuditEntity.getTableName() ;
//			String surrogateKeyColumn  = bomQualAuditEntity.getSurrogateKeyColumn();
//			Long surrogateKey = bomQualAuditEntity.getAltKey();
//			
//			try
//			{
//				stmt = JDBCUtility.prepareStatement("delete from "+tableName+" where "+ surrogateKeyColumn +"=?", connection);
//				stmt.setLong(1, surrogateKey);
//				JDBCUtility.executeUpdate(stmt);
//				
////				template.update("delete from "+tableName+" where "+ surrogateKeyColumn +"=?", surrogateKey);
//			}
//			catch (Exception e)
//			{
//				throw new RepositoryException("Failed to delete from the table "+tableName+" for key " + surrogateKey, e);
//			}
//			finally {
//				JDBCUtility.closeStatement(stmt);
//			}
//	}
//	
//	
//	private void executedeleteYarnDetails(long altkeyQualtx, long altkeyComp, Connection connection) throws RepositoryException
//		{
//				RepositoryException primaryException;
//				PreparedStatement stmt = null;
//				
//				try
//				{
//					stmt = JDBCUtility.prepareStatement(" delete from MDI_QUALTX_COMP_DE where group_name = 'IMPL_BOM_PROD_FAMILY:TEXTILES' and alt_key_qualtx=? and alt_key_comp =?", connection);
//					stmt.setLong(1, altkeyQualtx);
//					stmt.setLong(2, altkeyComp);
//					JDBCUtility.executeUpdate(stmt);
//				}
//				catch (Exception e)
//				{
//					primaryException = new RepositoryException("Failed to delete the price records from MDI_QUALTX_COMP_DE", e);
//					
//					throw primaryException;
//				}
//				finally {
//					JDBCUtility.closeStatement(stmt);
//				}
//		
//		}
//	
//	private void deletePrice( BOMQualAuditEntity bomQualAuditEntity, long qualtxKey, Connection connection)
//			throws RepositoryException
//		{
//				RepositoryException primaryException;
//				PreparedStatement stmt = null;
//				String tableName =  bomQualAuditEntity.getTableName() ;
//				
//				StringBuilder sql = new StringBuilder(" delete from "+tableName+" where ");
//				
//				if(tableName.equalsIgnoreCase(MDIQualTxPrice.TABLE_NAME))
//				{
//					sql.append(" alt_key_qualtx = ? ");
//				}
//				if(tableName.equalsIgnoreCase(MDIQualTxCompPrice.TABLE_NAME))
//				{
//					sql.append(" alt_key_qualtx = ? and alt_key_comp = ? ");
//				}
//					
//				try
//				{
//					stmt = JDBCUtility.prepareStatement(sql.toString(), connection);
//					stmt.setLong(1, qualtxKey);
//					if(tableName.equalsIgnoreCase(MDIQualTxCompPrice.TABLE_NAME))
//					{
//						stmt.setLong(2, bomQualAuditEntity.getParentAltkey());
//					}
//					JDBCUtility.executeUpdate(stmt);
//				}
//				catch (SQLException e)
//				{
//					primaryException = new RepositoryException("Failed to delete from the table "+tableName+"", e);
//					
//					throw primaryException;
//				}
//				finally {
//					JDBCUtility.closeStatement(stmt);
//				}
//		
//		}
}
