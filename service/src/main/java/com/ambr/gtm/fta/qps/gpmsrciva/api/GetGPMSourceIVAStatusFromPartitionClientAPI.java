package com.ambr.gtm.fta.qps.gpmsrciva.api;

import java.net.URI;
import java.text.MessageFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.ambr.platform.utils.subservice.SubordinateServiceReference;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GetGPMSourceIVAStatusFromPartitionClientAPI 
{
	private static Logger		logger = LogManager.getLogger(GetGPMSourceIVAStatusFromPartitionClientAPI.class);

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
	public GetGPMSourceIVAStatusFromPartitionClientAPI(SubordinateServiceReference theServiceRef)
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
	public GetGPMSourceIVAStatusFromPartitionClientAPI(
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
     * @param	thePaddingLength
     *************************************************************************************
     */
	public String execute(int thePaddingLength)
		throws Exception
	{
		RestTemplate			aTemplate = new RestTemplate();
		ResponseEntity<String>	aResponse;
		String					aStatusMsg;
		
		aResponse = aTemplate.getForEntity(
			new URI(MessageFormat.format("{0}{1}", this.serviceURL, GetGPMSourceIVAStatusFromPartitionServiceAPI.GetURLPath(thePaddingLength))), 
			String.class
		);

		if (aResponse.getStatusCodeValue() == 200) {
			aStatusMsg = aResponse.getBody();
			return aStatusMsg;
		}
		else {
			throw new Exception(MessageFormat.format("Request failed: status [{0}]", aResponse.getStatusCodeValue()));
		}
	}
}
