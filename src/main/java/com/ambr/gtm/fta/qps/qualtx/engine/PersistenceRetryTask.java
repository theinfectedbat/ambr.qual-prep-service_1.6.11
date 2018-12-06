package com.ambr.gtm.fta.qps.qualtx.engine;

import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ambr.gtm.fta.qps.qualtx.engine.result.BOMStatusTracker;
import com.ambr.platform.rdbms.orm.EntityManager;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.platform.utils.queue.TaskInterface;
import com.ambr.platform.utils.queue.TaskProgressInterface;
import com.ambr.platform.utils.queue.TaskQueue;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class PersistenceRetryTask 
	implements TaskInterface, TaskProgressInterface
{
	private static Logger						logger = LogManager.getLogger(PersistenceRetryTask.class);
	
	private PersistenceRetryQueue				queue;
	private QualTX								qualTX;
	private String								description;
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theQueue
	 * @param	theQualTX
	 *************************************************************************************
	 */
	public PersistenceRetryTask(
		PersistenceRetryQueue	theQueue, 
		QualTX 					theQualTX)
		throws Exception
	{
		this.queue = theQueue;
		this.qualTX = theQualTX;
		this.description = "QTX." + this.qualTX.alt_key_qualtx;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theTaskQueue
	 *************************************************************************************
	 */
	@Override
	public void execute(TaskQueue<? extends TaskInterface> theTaskQueue) 
		throws Exception 
	{
		EntityManager<QualTX>	aEntityMgr;
		int						aMaxAttemptCount = 3;
		JdbcTemplate			aTemplate = new JdbcTemplate(this.queue.queueUniverse.dataSrc);
		BOMStatusTracker		aTracker;
		long					aPrevQualTXKey;
		
		aTracker = this.queue.queueUniverse.qtxPrepProgressMgr.getStatusManager().getBOMTracker(this.qualTX.alt_key_bom);
		
		for (int aAttempt = 1; aAttempt <= aMaxAttemptCount; aAttempt++) {
			
			try {
				aPrevQualTXKey = this.qualTX.alt_key_qualtx;
				this.qualTX.generateNewKeys(true);
				aEntityMgr = new EntityManager<QualTX>(QualTX.class, this.queue.queueUniverse.txMgr, this.queue.queueUniverse.schemaDesc, aTemplate);
				aEntityMgr.createNewEntity(this.qualTX);
				aEntityMgr.save("SYSTEM");
				aTracker.replaceTradeLaneID(aPrevQualTXKey, this.qualTX.alt_key_qualtx);
				return;
			}
			catch (Exception e) {
				MessageFormatter.debug(logger, "execute", e, "Qual TX [{0}]:  Attempt [[{1}] failed to save.", this.qualTX.alt_key_qualtx, aAttempt);
			}
		}
		
		aTracker.setTradeLaneFailed(this.qualTX.fta_code, this.qualTX.ctry_of_import);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theWorkIdentifier
	 * @param	theFilter
	 *************************************************************************************
	 */
	@Override
	public boolean filterWorkIdentifier(String theWorkIdentifier, String theFilter) 
		throws Exception 
	{
		return StandardWorkIdentifierFilterLogic.execute(theWorkIdentifier, theFilter);
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	@Override
	public String getDescription() 
	{
		return this.description;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	@Override
	public ArrayList<String> getWorkIdentifiersForTask() 
		throws Exception 
	{
		ArrayList<String>	aList;
		
		aList = new ArrayList<>();
		// TODO: implement method
		
//		aList.add(StandardBOMRelatedWorkIdentifierGenerator.execute(this.bom.alt_key_bom, this.newQualTXKey, "QTX"));
		return aList;
	}
}
