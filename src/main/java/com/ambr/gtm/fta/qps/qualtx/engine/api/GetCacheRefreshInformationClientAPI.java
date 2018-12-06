package com.ambr.gtm.fta.qps.qualtx.engine.api;

import java.net.URL;
import java.text.MessageFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.ambr.gtm.fta.qps.qualtx.engine.request.QualTXUniverseGenerationRequest;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GetCacheRefreshInformationClientAPI 
{
	private static Logger		logger = LogManager.getLogger(GetCacheRefreshInformationClientAPI.class);
	
	private String		serviceURLProtocol;
	private String		serviceURLHost;
	private int			serviceURLPort;
	private String		serviceURL;
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theURL
     *************************************************************************************
     */
	public GetCacheRefreshInformationClientAPI(URL theURL)
		throws Exception
	{
		this(theURL.getProtocol(), theURL.getHost(), theURL.getPort());
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theProtocol
     * @param	theHost
     * @param	thePort
     *************************************************************************************
     */
	public GetCacheRefreshInformationClientAPI(String theProtocol, String theHost, int thePort)
		throws Exception
	{
		this.serviceURLProtocol = theProtocol;
		this.serviceURLHost = theHost;
		this.serviceURLPort = thePort;

		this.serviceURL = this.serviceURLProtocol + "://";
		this.serviceURL += this.serviceURLHost + ":";
		this.serviceURL += this.serviceURLPort;
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public CacheRefreshInformation execute()
		throws Exception
	{
		RestTemplate							aTemplate = new RestTemplate();
		CacheRefreshInformation					aResponse;
		ResponseEntity<CacheRefreshInformation>	aResponseEntity;
		
		aResponseEntity = aTemplate.postForEntity(
			this.serviceURL + GetCacheRefreshInformationServiceAPI.URL_PATH, 
			HttpMethod.GET, 
			CacheRefreshInformation.class
		);
		
		if (aResponseEntity.getStatusCodeValue() == 200) {
			aResponse = aResponseEntity.getBody();
			return aResponse;
		}
		else {
			throw new Exception(MessageFormat.format("Request failed: status [{0}]", aResponseEntity.getStatusCodeValue()));
		}
	}
}
