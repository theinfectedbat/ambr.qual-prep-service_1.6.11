package com.ambr.gtm.fta.qps.bom.qualstatus.api;

import java.net.URI;
import java.net.URL;
import java.text.MessageFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.ambr.gtm.fta.qps.bom.qualstatus.BOMQualificationStatus;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GetBOMQualificationStatusClientAPI 
{
	private static Logger		logger = LogManager.getLogger(GetBOMQualificationStatusClientAPI.class);

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
	public GetBOMQualificationStatusClientAPI(URL theURL)
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
	public GetBOMQualificationStatusClientAPI(String theProtocol, String theHost, int thePort)
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
     * 
     * @param	theBOMKey
     *************************************************************************************
     */
	public BOMQualificationStatus execute(long theBOMKey)
		throws Exception
	{
		RestTemplate							aTemplate = new RestTemplate();
		ResponseEntity<BOMQualificationStatus>	aResponse;
		BOMQualificationStatus					aContainer;
		
		aResponse = aTemplate.getForEntity(
			new URI(MessageFormat.format("{0}{1}", this.serviceURL, GetBOMQualificationStatusServiceAPI.GetURLPath(theBOMKey))), 
			BOMQualificationStatus.class
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
