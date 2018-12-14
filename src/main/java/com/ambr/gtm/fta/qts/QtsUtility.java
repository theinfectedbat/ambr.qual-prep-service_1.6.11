package com.ambr.gtm.fta.qts;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.qts.container.TrackerContainer;
import com.ambr.gtm.fta.qts.work.QtxWorkInfo;
import com.ambr.gtm.fta.qts.workmgmt.QTXWorkInfo;
import com.ambr.platform.utils.log.MessageFormatter;

public class QtsUtility
{
	static Logger logger = LogManager.getLogger(QtsUtility.class);
	public void executeQtxQualificationWork(QTXWorkInfo work, QTXWorkRepository workRepository, TrackerContainer trackerContainer) throws Exception
	{	logger.debug("Started Posting Qualification work for BOM KEY: " + work.bomKey + " Qtx key :" + work.qualtx_key);
		try
		{
			QTXWork qtxWork = workRepository.createWork();
			qtxWork.setWorkStatus(TrackerCodes.QualtxStatus.READY_FOR_QUALIFICATION);
			if (work.priority == null) qtxWork.priority = 70;
			else qtxWork.priority = work.priority;
			
			qtxWork.userId = work.userId;
			qtxWork.entity_type = QTXWork.ENTITY_TYPE_PRODUCT;
			qtxWork.bom_key = work.bomKey;
			qtxWork.iva_key = work.ivaKey;
			qtxWork.company_code = work.companyCode;
			QTXWorkDetails workDetails =  workRepository.createWorkDetails(qtxWork.qtx_wid);
			workDetails.qualtx_key = work.qualtx_key;
			workDetails.analysis_method = TrackerCodes.AnalysisMethod.values()[work.analysisMathod];
			qtxWork.details = workDetails;

			workRepository.storeWork(qtxWork);
			
			informTrackerChangeOfQtxStatus(qtxWork, work.qualtx_key, trackerContainer);
		}
		catch (Exception e)
		{
			MessageFormatter.error(logger, "executeQtxQualificationWork",e, "Exception while persisting the qualification work for BOM KEY: [{0}]  and Qtx key : [{1}] ",work.bomKey,work.qualtx_key);
			
			throw e;
		}
		

	}
	
	public void informTrackerChangeOfQtxStatus(QTXWork qtxWork, long qualtxKey, TrackerContainer trackerContainer)
	{
		try
		{
			QtxWorkInfo qtxWorkInfo = new QtxWorkInfo();
			qtxWorkInfo.setBomKey(qtxWork.bom_key);
			qtxWorkInfo.setQtxWorkId(qtxWork.qtx_wid);
			qtxWorkInfo.setQtxKey(qualtxKey);
			qtxWorkInfo.setAnalysisMethod(qtxWork.details.analysis_method);
			qtxWorkInfo.setTotalComponents(qtxWork.details.components);
			qtxWorkInfo.setQtxStatus(qtxWork.status.status);
			
			BOMTracker aBOMTracker = trackerContainer.getBomTracker(qtxWork.bom_key);
			int newPriority = qtxWork.priority;
			int existingPriority = aBOMTracker.getPriority();
			if (newPriority >= 0 && newPriority > existingPriority) aBOMTracker.setPriority(newPriority);
			QtxTracker aQtxTracker = aBOMTracker.addQualtx(qtxWorkInfo);
			QtxWorkTracker aQtxWorkTracker = aQtxTracker.addQualtxWork(qtxWorkInfo);
			aQtxWorkTracker.setQtxLastModifiedTime(System.currentTimeMillis());
		}
		catch (Exception e)
		{
			MessageFormatter.error(logger, "informTrackerChangeOfQtxStatus",e, "Exception while updating status to tracker for BOM KEY: [{0}]  and Qtx key : [{1}] ",qtxWork.bom_key,qualtxKey);
		}
	}
}
