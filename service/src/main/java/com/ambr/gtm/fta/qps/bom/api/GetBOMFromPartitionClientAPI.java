package com.ambr.gtm.fta.qps.bom.api;

import java.net.URI;
import java.text.MessageFormat;
import java.util.Collections;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.BufferingClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import com.ambr.gtm.fta.qps.RequestResponseLoggingInterceptor;
import com.ambr.gtm.fta.qps.bom.BOM;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.platform.utils.subservice.SubordinateServiceReference;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GetBOMFromPartitionClientAPI 
{
	private static Logger		logger = LogManager.getLogger(GetBOMFromPartitionClientAPI.class);

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
	public GetBOMFromPartitionClientAPI(SubordinateServiceReference theServiceRef)
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
	public GetBOMFromPartitionClientAPI(
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
     * 
     * @param	theBOMKey
     *************************************************************************************
     */
	public BOM execute(long theBOMKey)
		throws Exception
	{
		RestTemplate		aTemplate;
		ResponseEntity<BOM>	aResponse;
		BOM					aBOM;
		
		if (logger.isDebugEnabled()) {
			ClientHttpRequestFactory factory = new BufferingClientHttpRequestFactory(new SimpleClientHttpRequestFactory());
			aTemplate = new RestTemplate(factory);
			aTemplate.setInterceptors(Collections.singletonList(new RequestResponseLoggingInterceptor()));
		}
		else {
			aTemplate = new RestTemplate();
		}
		
		try {
			aResponse = aTemplate.getForEntity(
				new URI(MessageFormat.format("{0}{1}", this.serviceURL, GetBOMFromPartitionServiceAPI.GetURLPath(theBOMKey))), 
				BOM.class
			);
	
			if (aResponse.getStatusCodeValue() == 200) {
				aBOM = aResponse.getBody();
				return aBOM;
			}
			else {
				throw new Exception(MessageFormat.format("Request failed: status [{0}]", aResponse.getStatusCodeValue()));
			}
		}
		catch (Exception e) {
			MessageFormatter.error(logger, "execute", e, "BOM [{0}", theBOMKey);
			throw e;
		}
	}
}
