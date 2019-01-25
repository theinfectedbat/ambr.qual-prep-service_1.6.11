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
public class GenerateUniverseClientAPI 
{
	private static Logger		logger = LogManager.getLogger(GenerateUniverseClientAPI.class);
	
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
	public GenerateUniverseClientAPI(URL theURL)
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
	public GenerateUniverseClientAPI(String theProtocol, String theHost, int thePort)
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
		RestTemplate			aTemplate = new RestTemplate();
		Void					aResponse;
		ResponseEntity<Void>	aResponseEntity;
		
		aResponseEntity = aTemplate.postForEntity(
			this.serviceURL + GenerateUniverseServiceAPI.URL_PATH_GENERATE_UNIVERSE, 
			HttpMethod.POST, 
			Void.class
		);
		
		if (aResponseEntity.getStatusCodeValue() == 200) {
			aResponse = aResponseEntity.getBody();
			return;
		}
		else {
			throw new Exception(MessageFormat.format("Request failed: status [{0}]", aResponseEntity.getStatusCodeValue()));
		}
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theGenReq
     *************************************************************************************
     */
	public void execute(QualTXUniverseGenerationRequest theGenReq)
		throws Exception
	{
		RestTemplate									aTemplate = new RestTemplate();
		Void											aResponse;
		HttpEntity<QualTXUniverseGenerationRequest>		aRequest;
		ResponseEntity<Void>							aResponseEntity;
		
		aRequest = new HttpEntity<>(theGenReq);
		aResponseEntity = aTemplate.exchange(
			this.serviceURL + GenerateUniverseServiceAPI.URL_PATH_GENERATE_UNIVERSE_WITH_OPTIONS, 
			HttpMethod.POST, 
			aRequest, 
			Void.class
		);
		
		if (aResponseEntity.getStatusCodeValue() == 200) {
			aResponse = aResponseEntity.getBody();
			return;
		}
		else {
			throw new Exception(MessageFormat.format("Request failed: status [{0}]", aResponseEntity.getStatusCodeValue()));
		}
	}
}
