package com.ambr.gtm.fta.qps.gpmclass;

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
public class GPMClassificationUniversePartitionEventHandler
	implements SubordinateServiceEventHandler
{
	GPMClassificationUniverse		bomUniverse;
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	GPMClassificationUniversePartitionEventHandler(GPMClassificationUniverse theBOMUniverse)
		throws Exception
	{
		this.bomUniverse = theBOMUniverse;
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
    	this.bomUniverse.status = UniverseStatusEnum.PARTITION_STOPPED;
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
    	this.bomUniverse.status = UniverseStatusEnum.PARTITION_FAILED;
	}
}
