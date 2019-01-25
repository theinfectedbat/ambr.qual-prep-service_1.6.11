package com.ambr.gtm.fta.qts.api;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qts.workmgmt.QTXMonitoredMetrics;
import com.ambr.gtm.fta.qts.workmgmt.QTXWorkProducer;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@RestController
public class GetRequalificationMetricsServiceAPI 
{
    @Autowired
	QTXWorkProducer qtxWorkProducer;
    
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GetRequalificationMetricsServiceAPI()
		throws Exception
	{
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBOMKey
     *************************************************************************************
     */
	@RequestMapping(GetRequalificationMetricsClientAPI.URL_PATH_PREFIX)
	public List<QTXMonitoredMetrics> execute(@RequestParam(GetRequalificationMetricsClientAPI.URL_PATH_VARIABLE_DURATION) int duration)
		throws Exception
	{
		//TODO duration - implement in this method or qtxworkproducer : register metric monitor, sleep duration, return metric monitor
		return this.qtxWorkProducer.getMonitoredMetrics();
	}
}
