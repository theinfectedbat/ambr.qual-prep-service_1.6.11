package com.ambr.gtm.fta.qts.trade;

//import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ambr.gtm.fta.qts.util.Env;
import com.ambr.gtm.fta.trade.client.LockException;
import com.ambr.gtm.fta.trade.client.TradeQualtxClient;

public class MDIBomRepository
{
	public MDIBomRepository()
	{
	}
	
	private static Logger logger = LogManager.getLogger(MDIBomRepository.class);
	
//	public static MDIBom getBOM(long altKeyBom, Connection connection) throws RepositoryException
//	{
//		MDIBom bom = new MDIBom();
//		
//		try
//		{
//			PreparedStatement stmt = null;
//			
//			try
//			{
//				stmt = JDBCUtility.prepareStatement("select BOM_ID,ORG_CODE, CTRY_OF_MANUFACTURE, CURRENCY_CODE, GROSS_WEIGHT, uom, AREA, AREA_UOM,DIRECT_PROCESSING_COST,prod_key, ASSEMBLY_TYPE,PROD_FAMILY,EFFECTIVE_FROM,EFFECTIVE_TO,COST,PRICE from MDI_BOM where alt_key_bom=?", connection);
//				
//				stmt.setLong(1, altKeyBom);
//				
//				ResultSet results = null;
//				try
//				{
//					results = stmt.executeQuery();
//					
//					if (results.next())
//					{
//						bom.alt_key_bom = altKeyBom;
//						bom.org_code = results.getString("ORG_CODE");
//						bom.bom_id=results.getString("BOM_ID");
//						bom.ctry_of_manufacture = results.getString("CTRY_OF_MANUFACTURE");
//						bom.currency_code = results.getString("CURRENCY_CODE");
//						bom.gross_weight = results.getDouble("GROSS_WEIGHT");
//						bom.uom = results.getString("UOM");
//						bom.area = results.getDouble("AREA");
//						bom.area_uom = results.getString("AREA_UOM");
//						bom.direct_processing_cost = results.getDouble("DIRECT_PROCESSING_COST");
//						bom.prod_key = results.getLong("PROD_KEY");
//						bom.assembly_type = results.getString("ASSEMBLY_TYPE");
//						bom.prod_family = results.getString("PROD_FAMILY");
//						bom.effective_from = results.getDate("EFFECTIVE_FROM");
//						bom.effective_to = results.getDate("EFFECTIVE_TO");
//						bom.cost = results.getDouble("COST");
//						bom.price = results.getDouble("PRICE");
//					}
//					else
//					{
//						throw new RepositoryException("BOM record does not exist, alt key of " + altKeyBom);
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
//			throw new RepositoryException("Failed to load bom alt key of " + altKeyBom, s);
//		}
//		
//		return bom;
//	}
	
//	public static MDIBomComp getBOMComp(long altKeyComp, Connection connection) throws RepositoryException
//	{
//		MDIBomComp bomComp = new MDIBomComp();
//		try
//		{
//			PreparedStatement stmt = null;
//			
//			try
//			{
//				stmt = JDBCUtility.prepareStatement("select "+
//						"MDI_BOM_COMP.org_code," +
//						"MDI_BOM_COMP.bom_id," +
//						"MDI_BOM_COMP.alt_key_bom," +
//						"MDI_BOM_COMP.comp_num," +
//						"MDI_BOM_COMP.alt_key_comp," +
//						"MDI_BOM_COMP.extended_cost," +
//						"MDI_BOM_COMP.ctry_of_origin," +
//						"MDI_BOM_COMP.description," +
//						"MDI_BOM_COMP.essential_character," +
//						"MDI_BOM_COMP.net_weight," +
//						"MDI_BOM_COMP.weight_uom," +
//						"MDI_BOM_COMP.component_type," +
//						"MDI_BOM_COMP.area," +
//						"MDI_BOM_COMP.area_uom," +
//						"MDI_BOM_COMP.ctry_of_manufacture," +
//						"MDI_BOM_COMP.prod_key," +
//						"MDI_BOM_COMP.prod_src_key, " +
//						"MDI_BOM_COMP.supplier_key, " +
//						"MDI_BOM_COMP.qty_per " +
//						"from MDI_BOM_COMP " +
//						" where MDI_BOM_COMP.alt_key_comp = ? ", connection);
//				
//				stmt.setLong(1, altKeyComp);
//				
//				ResultSet results = null;
//				try
//				{
//					results = stmt.executeQuery();
//					
//					if (results.next())
//					{
//						bomComp.org_code = results.getString(1);
//						bomComp.bom_id = results.getString(2);
//						bomComp.alt_key_bom = results.getLong(3);
//						bomComp.comp_num = results.getLong(4);
//						bomComp.alt_key_comp = results.getLong(5);
//						bomComp.extended_cost = results.getDouble(6);
//						bomComp.ctry_of_origin = results.getString(7);
//						bomComp.description = results.getString(8);
//						bomComp.essential_character = results.getString(9);
//						bomComp.net_weight = results.getDouble(10);
//						bomComp.weight_uom = results.getString(11);
//						bomComp.component_type = results.getString(12);
//						bomComp.area = results.getDouble(13);
//						bomComp.area_uom = results.getString(14);
//						bomComp.ctry_of_manufacture = results.getString(15);
//						bomComp.prod_key = results.getLong(16);
//						bomComp.prod_src_key = results.getLong(17);
//						bomComp.supplier_key = results.getLong(18);
//						bomComp.qty_per = results.getDouble(19);
//					}
//					else
//					{
//						throw new RepositoryException("BOMComp record does not exist, alt key comp  " + altKeyComp);
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
//			throw new RepositoryException("Failed to load bom comp  of alt key comp " + altKeyComp, s);
//		}
//		
//		return bomComp;
//	}
	
//	public static MDIBomComp getBOMComp(long altKeyComp, long altKeyQulatx, Connection connection) throws RepositoryException
//	{
//		MDIBomComp bomComp = new MDIBomComp();
//		try
//		{
//			PreparedStatement stmt = null;
//			
//			try
//			{
//				stmt = JDBCUtility.prepareStatement("select "+
//						"MDI_BOM_COMP.org_code," +
//						"MDI_BOM_COMP.bom_id," +
//						"MDI_BOM_COMP.alt_key_bom," +
//						"MDI_BOM_COMP.comp_num," +
//						"MDI_BOM_COMP.alt_key_comp," +
//						"MDI_BOM_COMP.extended_cost," +
//						"MDI_BOM_COMP.ctry_of_origin," +
//						"MDI_BOM_COMP.description," +
//						"MDI_BOM_COMP.essential_character," +
//						"MDI_BOM_COMP.net_weight," +
//						"MDI_BOM_COMP.weight_uom," +
//						"MDI_BOM_COMP.component_type," +
//						"MDI_BOM_COMP.area," +
//						"MDI_BOM_COMP.area_uom," +
//						"MDI_BOM_COMP.ctry_of_manufacture," +
//						"MDI_BOM_COMP.prod_key," +
//						"MDI_BOM_COMP.prod_src_key, " +
//						"MDI_BOM_COMP.supplier_key, " +
//						"MDI_BOM_COMP.qty_per, " +
//						"qualtx.tx_id " +
//						"from mdi_bom_comp " +
//						" join mdi_qualtx qualtx on (mdi_bom_comp.alt_key_bom = qualtx.src_key)" +
//						" where mdi_bom_comp.alt_key_comp = ? and qualtx.alt_key_qualtx=?", connection);
//				
//				stmt.setLong(1, altKeyComp);
//				stmt.setLong(2, altKeyQulatx);
//				
//				ResultSet results = null;
//				try
//				{
//					results = stmt.executeQuery();
//					
//					if (results.next())
//					{
//						bomComp.org_code = results.getString(1);
//						bomComp.bom_id = results.getString(2);
//						bomComp.alt_key_bom = results.getLong(3);
//						bomComp.comp_num = results.getLong(4);
//						bomComp.alt_key_comp = results.getLong(5);
//						bomComp.extended_cost = results.getDouble(6);
//						bomComp.ctry_of_origin = results.getString(7);
//						bomComp.description = results.getString(8);
//						bomComp.essential_character = results.getString(9);
//						bomComp.net_weight = results.getDouble(10);
//						bomComp.weight_uom = results.getString(11);
//						bomComp.component_type = results.getString(12);
//						bomComp.area = results.getDouble(13);
//						bomComp.area_uom = results.getString(14);
//						bomComp.ctry_of_manufacture = results.getString(15);
//						bomComp.prod_key = results.getLong(16);
//						bomComp.prod_src_key = results.getLong(17);
//						bomComp.supplier_key = results.getLong(18);
//						bomComp.qty_per = results.getDouble(19);
//						bomComp.tx_id = results.getString(20);
//					}
//					else
//					{
//						throw new RepositoryException("BOMComp record does not exist, alt key comp  " + altKeyComp);
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
//			throw new RepositoryException("Failed to load bom comp  of alt key comp " + altKeyComp, s);
//		}
//		
//		return bomComp;
//	}
	
//	public static long getBomCompCount(long altKeyBom, Connection connection) throws RepositoryException
//	{
//		PreparedStatement stmt = null;
//		
//		try
//		{
//			try
//			{
//				stmt = JDBCUtility.prepareStatement("select count(1) from MDI_BOM_COMP where alt_key_bom=?", connection);
//				
//				stmt.setLong(1, altKeyBom);
//				
//				ResultSet results = null;
//				try
//				{
//					results = stmt.executeQuery();
//					
//					results.next();
//
//					return results.getLong(1);
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
//			throw new RepositoryException("Failed to fetch bom comp count for bom alt key of " + altKeyBom, s);
//		}
//	}
	
	
//	public static List<MDIBomPrice> getBomPriceList(long altKeyBom, long altKeyQualtx, Connection connection) throws RepositoryException
//	{
//		PreparedStatement stmt = null;
//		List<MDIBomPrice>  bompricelist =  new ArrayList<MDIBomPrice>();
//		try
//		{
//			try
//			{
//				stmt = JDBCUtility.prepareStatement("select distinct PRICE_TYPE,PRICE,price.CURRENCY_CODE,price.ORG_CODE,qualtx.TX_ID,price.PRICE_SEQ_NUM from MDI_BOM_PRICE price"
//						+ " JOIN MDI_QUALTX qualtx ON(price.ALT_KEY_BOM =qualtx.SRC_KEY ) where alt_key_bom=? and alt_key_qualtx = ?", connection);
//				
//				stmt.setLong(1, altKeyBom);
//				stmt.setLong(2, altKeyQualtx);
//				
//				ResultSet results = null;
//				try
//				{
//					results = stmt.executeQuery();
//					
//					while (results.next())
//					{
//						MDIBomPrice mdibomprice = new MDIBomPrice();
//						mdibomprice.alt_key_bom = altKeyBom;
//						mdibomprice.price_type = results.getString(1);
//						mdibomprice.price = results.getDouble(2);
//						mdibomprice.currency_code = results.getString(3);
//						mdibomprice.org_code = results.getString(4); 
//						mdibomprice.tx_id = results.getString(5); 
//						mdibomprice.price_seq_num = results.getString(6); 
//						bompricelist.add(mdibomprice);
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
//			
//			return bompricelist;
//		}
//		catch (SQLException s)
//		{
//			throw new RepositoryException("Failed to fetch bom prices for bom alt key of " + altKeyBom, s);
//		}
//	}
//	
//	public static boolean getBomDeChange(long altKeyBom, String groupName, String columnName, Connection connection) throws RepositoryException
//	{
//		PreparedStatement stmt = null;
//		
//		try
//		{
//			try
//			{
//				stmt = JDBCUtility.prepareStatement("select "
//						+ columnName 
//						+ " from MDI_BOM_DE where GROUP_NAME = '"+ groupName +"' AND alt_key_bom=?", connection);
//				
//				stmt.setLong(1, altKeyBom);
//				
//				ResultSet results = null;
//				try
//				{
//					results = stmt.executeQuery();
//					
//					return results.next();
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
//			throw new RepositoryException("Failed to fetch bom DE for bom alt key of " + altKeyBom, s);
//		}
//	}
	
//	public static List<MDIYarnDetails> getYarnDetailsList(long altKeycomp, long qualtxcomp, Connection connection) throws RepositoryException
//	{
//		PreparedStatement stmt = null;
//		List<MDIYarnDetails>  mdiYarnDetails =  new ArrayList<MDIYarnDetails>();
//		try
//		{
//			try
//			{
//				stmt = JDBCUtility.prepareStatement("select de.ORG_CODE,FLEXFIELD_VAR1,FLEXFIELD_VAR2,FLEXFIELD_VAR3,FLEXFIELD_VAR4,"
//						+ "FLEXFIELD_VAR5,FLEXFIELD_VAR6,FLEXFIELD_VAR7,FLEXFIELD_NUM1,qualtx_comp.TX_ID from MDI_BOM_COMP_DE de "
//						+ "join mdi_qualtx_comp qualtx_comp ON (de.ALT_KEY_COMP = qualtx_comp.src_key) where group_name = 'IMPL_BOM_PROD_FAMILY:TEXTILES'"
//						+ " and de.alt_key_comp=? and qualtx_comp.alt_key_comp =?", connection);
//				
//				stmt.setLong(1, altKeycomp);
//				stmt.setLong(2, qualtxcomp);
//				
//				
//				ResultSet results = null;
//				try
//				{
//					results = stmt.executeQuery();
//					
//					while (results.next())
//					{
//						MDIYarnDetails mdiYarnDetail = new MDIYarnDetails();
//						mdiYarnDetail.org_code = results.getString(1);
//						mdiYarnDetail.type = results.getString(2);
//						mdiYarnDetail.originating_status = results.getString(3);
//						mdiYarnDetail.ctry_of_origin = results.getString(4);
//						mdiYarnDetail.ctry_of_manufacture = results.getString(5);
//						mdiYarnDetail.weight_type = results.getString(6);
//						mdiYarnDetail.knit_to_shape = results.getString(7);
//						mdiYarnDetail.uom = results.getString(8); 
//						mdiYarnDetail.weight = results.getDouble(9); 
//						mdiYarnDetail.tx_id = results.getString(10); 
//						mdiYarnDetails.add(mdiYarnDetail);
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
//			
//			return mdiYarnDetails;
//		}
//		catch (SQLException s)
//		{
//			throw new RepositoryException("Failed to fetch yarn details for bom alt key comp of " + altKeycomp, s);
//		}
//	}
	
	//TODO review locking here, it should be done early in the process when other locks are acquired
	public static void deleteBomQualRecords(Long bomkey, List<Long> altKeyQualtxList, String orgCode, String userId, JdbcTemplate template) throws Exception
	{
		TradeQualtxClient tradeQualtxClient = Env.getSingleton().getTradeQualtxClient();
		Long lockId = null;
		
		if (altKeyQualtxList == null || altKeyQualtxList.isEmpty()) return;
		logger.debug("MDIBomRepository deleting bom qual records: orgCode="+orgCode + "userId="+ userId);
		try
		{
			lockId = tradeQualtxClient.acquireLock(orgCode, userId, bomkey);
			logger.debug("MDIBomRepository Acquire Lock is successfull: lock Id ="+lockId);
		}
		catch (Exception e)
		{
			throw new LockException("Failed to acquire the lock on the bom for the alt key bom:"+bomkey, e);
		}
		
		try
		{
			for (Long qtxkey : altKeyQualtxList)
			{
				template.update("delete from mdi_bom_qual where qualification_key =?", qtxkey);
				logger.debug("MDIBomRepository: deleting the mdi_bom_qual record, qualification_key  ="+qtxkey);
			}
		}
		finally {
			if (lockId != null)
			{
				try
				{
					tradeQualtxClient.releaseLock(lockId);
				}
				catch (Exception e)
				{
					logger.debug("MDIBomRepository: Exception occured while releasing the lock , lock id  ="+lockId);
					throw e;
				}
				
			}
		}
				
	}
}

