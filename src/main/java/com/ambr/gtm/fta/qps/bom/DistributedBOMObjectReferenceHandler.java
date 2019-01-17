package com.ambr.gtm.fta.qps.bom;

/**
 *****************************************************************************************
 * <P>
 * The purpose of this object is to invoke the required initialization routine of the BOM
 * object, once it has been received from the rest endpoint.  The required methods are left
 * non-public by design.  This class has the appropriate access to invoke the required
 * methods.
 * </P>
 *****************************************************************************************
 */
public class DistributedBOMObjectReferenceHandler 
{
    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBOM
     *************************************************************************************
     */
	public static void finalizeObjectReference(BOM theBOM)
		throws Exception
	{
		theBOM.initializeComponentBOMReferences();
	}
}
