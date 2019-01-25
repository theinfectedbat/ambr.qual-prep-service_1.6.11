package com.ambr.gtm.fta.qps.gpmsrciva.api;

import java.net.URI;
import java.text.MessageFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAProductContainer;
import com.ambr.gtm.fta.qps.gpmsrciva.GPMSourceIVAProductSourceContainer;
import com.ambr.platform.utils.subservice.SubordinateServiceReference;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GetGPMSourceIVAByProductFromPartitionClientAPI 
{
	private static Logger		logger = LogManager.getLogger(GetGPMSourceIVAByProductFromPartitionClientAPI.class);

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
	public GetGPMSourceIVAByProductFromPartitionClientAPI(SubordinateServiceReference theServiceRef)
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
	public GetGPMSourceIVAByProductFromPartitionClientAPI(
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
     * @param	theProdKey
     *************************************************************************************
     */
	public GPMSourceIVAProductContainer execute(long theProdKey)
		throws Exception
	{
		RestTemplate									aTemplate = new RestTemplate();
		ResponseEntity<GPMSourceIVAProductContainer>	aResponse;
		GPMSourceIVAProductContainer					aContainer;
		
		aResponse = aTemplate.getForEntity(
			new URI(MessageFormat.format("{0}{1}", this.serviceURL, GetGPMSourceIVAByProductFromPartitionServiceAPI.GetURLPath(theProdKey))), 
			GPMSourceIVAProductContainer.class
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
