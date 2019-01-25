package com.ambr.gtm.fta.qts.api;

import java.net.URL;
import java.text.MessageFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.ambr.gtm.fta.qts.RequalificationBOMStatus;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GetRequalificationMetricsClientAPI 
{
	private static Logger logger = LogManager.getLogger(GetRequalificationMetricsClientAPI.class);
	
	public static final String URL_PATH_PREFIX = "/qts/api/requal/get_metrics";
	public static final String URL_PATH_VARIABLE_DURATION = "duration";
	
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
	public GetRequalificationMetricsClientAPI(URL theURL)
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
	public GetRequalificationMetricsClientAPI(String theProtocol, String theHost, int thePort)
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
	public RequalificationBOMStatus execute(long duration)
		throws Exception
	{
		RestTemplate aTemplate = new RestTemplate();
		ResponseEntity<RequalificationBOMStatus> aResponseEntity;
		
		UriComponentsBuilder builder = UriComponentsBuilder
			    .fromUriString(this.serviceURL + URL_PATH_PREFIX)
			    .queryParam(URL_PATH_VARIABLE_DURATION, duration);
		
		aResponseEntity = aTemplate.exchange(
			builder.toUriString(), 
			HttpMethod.GET, 
			null, 
			RequalificationBOMStatus.class
		);
		
		if (aResponseEntity.getStatusCodeValue() == 200) {
			return aResponseEntity.getBody();
		}
		else {
			throw new Exception(MessageFormat.format("Request failed: status [{0}]", aResponseEntity.getStatusCodeValue()));
		}
	}
}
