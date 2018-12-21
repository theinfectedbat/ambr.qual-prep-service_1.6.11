package com.ambr.gtm.fta.qps.ptnr;

import com.ambr.gtm.fta.qps.UniverseStatusEnum;
import com.ambr.platform.utils.subservice.SubordinateServiceEventHandler;
import com.ambr.platform.utils.subservice.SubordinateServiceReference;
import com.ambr.platform.utils.subservice.exception.ServerUnavailableException;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class PartnerDetailUniversePartitionEventHandler
	implements SubordinateServiceEventHandler
{
	PartnerDetailUniverse		universe;
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	PartnerDetailUniversePartitionEventHandler(PartnerDetailUniverse theUniverse)
		throws Exception
	{
		this.universe = theUniverse;
	}

    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theServiceRef
     *************************************************************************************
     */
	@Override
	public void serviceStarted(SubordinateServiceReference theServiceRef) 
		throws Exception
	{
	}

    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theServiceRef
     * @param	theExitCode
     *************************************************************************************
     */
    @Override
	public void serviceStopped(
		SubordinateServiceReference theServiceRef, 
		int 						theExitCode) 
		throws Exception 
    {
    	this.universe.status = UniverseStatusEnum.PARTITION_STOPPED;
	}

    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theServiceRef
     * @param	theServerException
     *************************************************************************************
     */
	@Override
	public void serviceFailed(
		SubordinateServiceReference theServiceRef, 
		ServerUnavailableException 	theServerException)
		throws Exception 
	{
    	this.universe.status = UniverseStatusEnum.PARTITION_FAILED;
	}
}
