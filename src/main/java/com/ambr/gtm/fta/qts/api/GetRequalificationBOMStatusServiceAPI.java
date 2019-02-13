package com.ambr.gtm.fta.qts.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ambr.gtm.fta.qts.RequalificationBOMStatus;
import com.ambr.gtm.fta.qts.workmgmt.QTXWorkProducer;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
@RestController
public class GetRequalificationBOMStatusServiceAPI 
{
    @Autowired
	QTXWorkProducer qtxWorkProducer;
    
	/**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GetRequalificationBOMStatusServiceAPI()
		throws Exception
	{
	}

	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBOMKey
     *************************************************************************************
     */
	@RequestMapping(GetRequalificationBOMStatusClientAPI.URL_PATH_TEMPLATE)
	public RequalificationBOMStatus execute(@PathVariable(name = GetRequalificationBOMStatusClientAPI.URL_PATH_VARIABLE_NAME_BOM_KEY) long bomKey)
		throws Exception
	{
		return this.qtxWorkProducer.getRequalificationBOMStatus(bomKey);
	}
}
