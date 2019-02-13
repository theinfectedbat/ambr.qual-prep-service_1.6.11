package com.ambr.gtm.fta.qps.bom;

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
public class BOMUniversePartitionEventHandler
	implements SubordinateServiceEventHandler
{
	BOMUniverse		bomUniverse;
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	BOMUniversePartitionEventHandler(BOMUniverse theBOMUniverse)
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
