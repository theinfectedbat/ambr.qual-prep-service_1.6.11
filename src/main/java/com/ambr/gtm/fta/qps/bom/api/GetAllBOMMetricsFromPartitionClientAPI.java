package com.ambr.gtm.fta.qps.bom.api;

import java.net.URI;
import java.text.MessageFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.ambr.gtm.fta.qps.bom.BOMMetricSetPartitionContainer;
import com.ambr.platform.utils.subservice.SubordinateServiceReference;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GetAllBOMMetricsFromPartitionClientAPI 
{
	private static Logger		logger = LogManager.getLogger(GetAllBOMMetricsFromPartitionClientAPI.class);

	private String							serviceURLProtocol;
	private String							serviceURL;
	private SubordinateServiceReference		serviceRef;

    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theServiceRef
     *************************************************************************************
     */
	public GetAllBOMMetricsFromPartitionClientAPI(SubordinateServiceReference theServiceRef)
		throws Exception
	{
		this(theServiceRef, "http");
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theServiceRef
     * @param	theProtocol
     *************************************************************************************
     */
	public GetAllBOMMetricsFromPartitionClientAPI(
		SubordinateServiceReference theServiceRef, 
		String 						theProtocol)
		throws Exception
	{
		this.serviceRef = theServiceRef;
		this.serviceURLProtocol = theProtocol;

		this.serviceURL = this.serviceURLProtocol + "://";
		this.serviceURL += this.serviceRef.getHost() + ":";
		this.serviceURL += this.serviceRef.getServicePort();
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public BOMMetricSetPartitionContainer execute()
		throws Exception
	{
		RestTemplate									aTemplate = new RestTemplate();
		ResponseEntity<BOMMetricSetPartitionContainer>	aResponse;
		BOMMetricSetPartitionContainer					aContainer;
		
		aResponse = aTemplate.getForEntity(
			new URI(MessageFormat.format("{0}{1}", this.serviceURL, GetAllBOMMetricsFromPartitionServiceAPI.URL_PATH)), 
			BOMMetricSetPartitionContainer.class
		);

		if (aResponse.getStatusCodeValue() == 200) {
			aContainer = aResponse.getBody();
			return aContainer;
		}
		else {
			throw new Exception(MessageFormat.format("Request failed: status [{0}]", aResponse.getStatusCodeValue()));
		}
	}
}
