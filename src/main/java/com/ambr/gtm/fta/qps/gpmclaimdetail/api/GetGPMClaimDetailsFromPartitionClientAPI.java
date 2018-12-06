package com.ambr.gtm.fta.qps.gpmclaimdetail.api;

import java.net.URI;
import java.text.MessageFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.ambr.gtm.fta.qps.gpmclaimdetail.GPMClaimDetails;
import com.ambr.platform.utils.subservice.SubordinateServiceReference;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GetGPMClaimDetailsFromPartitionClientAPI 
{
	private static Logger		logger = LogManager.getLogger(GetGPMClaimDetailsFromPartitionClientAPI.class);

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
	public GetGPMClaimDetailsFromPartitionClientAPI(SubordinateServiceReference theServiceRef)
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
	public GetGPMClaimDetailsFromPartitionClientAPI(
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
     * @param	theProdSrcIVAKey
     *************************************************************************************
     */
	public GPMClaimDetails execute(long theProdSrcIVAKey)
		throws Exception
	{
		RestTemplate					aTemplate = new RestTemplate();
		ResponseEntity<GPMClaimDetails>	aResponse;
		GPMClaimDetails					aClaimDetail;
		
		aResponse = aTemplate.getForEntity(
			new URI(MessageFormat.format("{0}{1}", this.serviceURL, GetGPMClaimDetailsFromPartitionServiceAPI.GetURLPath(theProdSrcIVAKey))), 
			GPMClaimDetails.class
		);

		if (aResponse.getStatusCodeValue() == 200) {
			aClaimDetail = aResponse.getBody();
			return aClaimDetail;
		}
		else {
			throw new Exception(MessageFormat.format("Request failed: status [{0}]", aResponse.getStatusCodeValue()));
		}
	}
}
