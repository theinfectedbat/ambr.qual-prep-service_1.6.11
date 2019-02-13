package com.ambr.gtm.fta.qps.gpmclass.api;

import java.net.URI;
import java.net.URL;
import java.text.MessageFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.ambr.gtm.fta.qps.gpmclass.GPMClassificationProductContainer;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GetGPMClassificationsByProductFromUniverseClientAPI 
{
	private static Logger		logger = LogManager.getLogger(GetGPMClassificationsByProductFromUniverseClientAPI.class);
	
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
	public GetGPMClassificationsByProductFromUniverseClientAPI(URL theURL)
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
	public GetGPMClassificationsByProductFromUniverseClientAPI(String theProtocol, String theHost, int thePort)
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
	public GPMClassificationProductContainer execute(long theProdKey)
		throws Exception
	{
		RestTemplate aTemplate = new RestTemplate();
		ResponseEntity<GPMClassificationProductContainer> aResponseEntity;
		
		aResponseEntity = aTemplate.getForEntity(
				new URI(MessageFormat.format("{0}{1}", this.serviceURL, GetGPMClassificationsByProductFromUniverseServiceAPI.GetURLPath(theProdKey))), 
				GPMClassificationProductContainer.class
		);
		
		if (aResponseEntity.getStatusCodeValue() == 200) {
			return aResponseEntity.getBody();
		}
		else {
			throw new Exception(MessageFormat.format("Request failed: status [{0}]", aResponseEntity.getStatusCodeValue()));
		}
	}
}
