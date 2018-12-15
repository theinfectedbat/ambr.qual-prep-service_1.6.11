package com.ambr.gtm.fta.qps.qualtx.universe.api;

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
import com.ambr.gtm.fta.qps.qualtx.universe.QualTXDetailBOMContainer;
import com.ambr.platform.utils.log.MessageFormatter;
import com.ambr.platform.utils.subservice.SubordinateServiceReference;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GetQualTXCountFromPartitionClientAPI 
{
	private static Logger		logger = LogManager.getLogger(GetQualTXCountFromPartitionClientAPI.class);

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
	public GetQualTXCountFromPartitionClientAPI(SubordinateServiceReference theServiceRef)
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
	public GetQualTXCountFromPartitionClientAPI(
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
	public int execute()
		throws Exception
	{
		RestTemplate				aTemplate;
		ResponseEntity<Integer>		aResponse;
		Integer						aCount;
		
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
				new URI(MessageFormat.format("{0}{1}", this.serviceURL, GetQualTXCountFromPartitionServiceAPI.URL_PATH)), 
				Integer.class
			);
	
			if (aResponse.getStatusCodeValue() == 200) {
				aCount = aResponse.getBody();
				return aCount;
			}
			else {
				throw new Exception(MessageFormat.format("Request failed: status [{0}]", aResponse.getStatusCodeValue()));
			}
		}
		catch (Exception e) {
			MessageFormatter.error(logger, "execute", e, "error occured");
			throw e;
		}
	}
}
