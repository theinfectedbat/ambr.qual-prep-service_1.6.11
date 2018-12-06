package com.ambr.gtm.fta.qts.api;

import java.sql.Connection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qts.QTXStageRepository;
import com.ambr.gtm.fta.qts.WorkManagementException;
import com.ambr.gtm.fta.qts.util.Env;

@RestController
public class QTXStageTestAPI
{
	static Logger logger = LogManager.getLogger(TrackerServiceAPI.class);
	
   @Autowired
    private Env env;

    public QTXStageTestAPI() throws Exception
	{
      
	}
	
    @RequestMapping(value = "/qts/api/workmgmt/teststagerepository", method = RequestMethod.GET)
    public void qtxStart(@RequestParam(name="filename") String filename) throws Exception
    {
    	QTXStageRepository stageRepository = new QTXStageRepository();
    	Connection connection = null;
    	
    	try
    	{
    		connection = env.getPoolConnection();
        	stageRepository.testLoad(filename, connection);
    	}
    	finally
    	{
    		env.releasePoolConnection(connection);
    	}
    	
    }
}
