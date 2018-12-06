package com.ambr.gtm.fta.qts.workmgmt;

import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.gtm.fta.qps.qualtx.engine.api.GenerateUniverseClientAPI;
import com.ambr.gtm.fta.qps.qualtx.engine.request.QualTXUniverseGenerationRequest;
import com.ambr.gtm.fta.qps.qualtx.engine.request.TradeLaneSet;

public class QTXCreateConsumer extends QTXConsumer<QualTXUniverseGenerationRequest>
{
	private static final Logger logger = LogManager.getLogger(QTXCreateConsumer.class);

	private GenerateUniverseClientAPI generateUniverseClientAPI;
	
	public QTXCreateConsumer(ArrayList <QualTXUniverseGenerationRequest> workList, GenerateUniverseClientAPI generateUniverseClientAPI)
	{
		super(workList);
		
		this.generateUniverseClientAPI = generateUniverseClientAPI;
	}
	
	public QTXCreateConsumer(QualTXUniverseGenerationRequest work, GenerateUniverseClientAPI generateUniverseClientAPI)
	{
		super(work);
		
		this.generateUniverseClientAPI = generateUniverseClientAPI;
	}
	
	@Override
	protected void processWork() throws Exception
	{
		for (QualTXUniverseGenerationRequest work : this.workList)
		{
			this.processWork(work);
		}
	}

	//TODO get protocol host port
	protected void processWork(QualTXUniverseGenerationRequest work) throws Exception
	{
		logger.debug("QualTXUniverseGenerationRequest");
		if (work.tradeLaneSetList != null)
		{
			for (TradeLaneSet tradeLaneSet : work.tradeLaneSetList)
				for (String coi : tradeLaneSet.coiList)
					logger.debug(tradeLaneSet.ftaCode + ":" + coi);
		}
		
		for (Long bomKey : work.bomKeyList)
			logger.debug(bomKey);
		
		this.generateUniverseClientAPI.execute(work);
	}
}
