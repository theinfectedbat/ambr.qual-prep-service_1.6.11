package com.ambr.gtm.fta.qps.gpmsrciva;

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
public class GPMSourceIVAUniversePartitionEventHandler
	implements SubordinateServiceEventHandler
{
	GPMSourceIVAUniverse		gpmSrcIVAUniverse;
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBOMUniverse
     *************************************************************************************
     */
	GPMSourceIVAUniversePartitionEventHandler(GPMSourceIVAUniverse theBOMUniverse)
		throws Exception
	{
		this.gpmSrcIVAUniverse = theBOMUniverse;
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
    	this.gpmSrcIVAUniverse.status = UniverseStatusEnum.PARTITION_STOPPED;
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
    	this.gpmSrcIVAUniverse.status = UniverseStatusEnum.PARTITION_FAILED;
	}
}
