package com.ambr.gtm.fta.qps.gpmsrciva.api;

import java.net.URL;
import java.text.MessageFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GPMSourceIVAUniverseRefreshClientAPI 
{
	private static Logger		logger = LogManager.getLogger(GPMSourceIVAUniverseRefreshClientAPI.class);
	
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
	public GPMSourceIVAUniverseRefreshClientAPI(URL theURL)
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
	public GPMSourceIVAUniverseRefreshClientAPI(String theProtocol, String theHost, int thePort)
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
	public void execute()
		throws Exception
	{
		RestTemplate									aTemplate = new RestTemplate();
		ResponseEntity<Void>							aResponseEntity;
		
		aResponseEntity = aTemplate.exchange(
			this.serviceURL + GPMSourceIVAUniverseRefreshAPI.URL_PATH, 
			HttpMethod.POST, 
			null, 
			Void.class
		);
		
		if (aResponseEntity.getStatusCodeValue() == 200) {
			return;
		}
		else {
			throw new Exception(MessageFormat.format("Request failed: status [{0}]", aResponseEntity.getStatusCodeValue()));
		}
	}
}
