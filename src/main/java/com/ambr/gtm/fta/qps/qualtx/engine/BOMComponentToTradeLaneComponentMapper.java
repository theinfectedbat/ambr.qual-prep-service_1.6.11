package com.ambr.gtm.fta.qps.qualtx.engine;

import com.ambr.gtm.fta.qps.bom.BOMComponent;
import com.ambr.gtm.fta.qps.qualtx.engine.result.TradeLaneStatusTracker;
import com.ambr.gtm.fta.qps.util.QualTXComponentUtility;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfigurationRepository;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class BOMComponentToTradeLaneComponentMapper 
{
	private BOMComponent								bomComp;
	private QualTXComponent								qualTXComp;
	private DataExtensionConfigurationRepository		dataExtCfgRepos;
	private TradeLaneStatusTracker						statusTracker;
	
	/**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theBOMComp
     * @param	theQualTXComponent
     * @param	theConfigRepos
     *************************************************************************************
     */
	public BOMComponentToTradeLaneComponentMapper(
		BOMComponent 							theBOMComp, 
		QualTXComponent 						theQualTXComponent,
		DataExtensionConfigurationRepository	theConfigRepos,
		TradeLaneStatusTracker					theStatusTracker)
		throws Exception
	{
		this.bomComp = theBOMComp;
		this.qualTXComp = theQualTXComponent;
		this.dataExtCfgRepos = theConfigRepos;
		this.statusTracker = theStatusTracker;
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
		
		QualTXComponentUtility aQualTXComponentUtility = new QualTXComponentUtility(this.qualTXComp, this.bomComp, this.dataExtCfgRepos, this.statusTracker);
		aQualTXComponentUtility.pullComponentBasicInfo(true);
	}
}
