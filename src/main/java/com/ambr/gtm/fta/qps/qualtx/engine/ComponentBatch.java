package com.ambr.gtm.fta.qps.qualtx.engine;

import java.text.MessageFormat;
import java.util.ArrayList;

import com.ambr.gtm.fta.qps.bom.BOMComponent;
import com.ambr.gtm.fta.qps.bom.BOMComponentDataExtension;
import com.ambr.gtm.fta.qps.qualtx.engine.result.TradeLaneStatusTracker;
import com.ambr.gtm.fta.qps.qualtx.exception.ComponentMaxBatchSizeReachedException;
import com.ambr.gtm.fta.qps.util.ComponentType;
import com.ambr.gtm.utils.legacy.rdbms.de.DataExtensionConfigurationRepository;
import com.ambr.gtm.utils.legacy.rdbms.de.GroupNameSpecification;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
class ComponentBatch 
{
	private int								startIndex;
	private int								maxBatchSize;
	final QualTX							qualTX;
	private ArrayList<Component>			compList;
	DataExtensionConfigurationRepository	dataExtCfgRepos;
	TradeLaneStatusTracker					statusTracker;
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	class Component
	{
		BOMComponent		bomComp;
		QualTXComponent		qualTXComponent;
		
		/**
		 *************************************************************************************
		 * <P>
		 * </P>
		 * 
		 * @param	theBOMComponent
		 *************************************************************************************
		 */
		public Component(BOMComponent theBOMComponent)
			throws Exception
		{
			this.bomComp = theBOMComponent;
		}

		/**
		 *************************************************************************************
		 * <P>
		 * </P>
		 *************************************************************************************
		 */
		public void createQualTXComponent()
			throws Exception
		{
			BOMComponentToTradeLaneComponentMapper		aMapper;
			
			try {
				this.qualTXComponent = ComponentBatch.this.qualTX.createComponent();
				
				aMapper = new BOMComponentToTradeLaneComponentMapper(this.bomComp, this.qualTXComponent, ComponentBatch.this.dataExtCfgRepos, ComponentBatch.this.statusTracker);
				aMapper.execute();
				
				ComponentBatch.this.statusTracker.constructComponentSuccess(this.qualTXComponent);
			}
			catch (Exception e) {
				ComponentBatch.this.statusTracker.constructComponentFailure(this.qualTXComponent, e);
				throw e;
			}
		}
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param 	theQualTX 
	 * @param 	theStatusTracker 
	 * @param	theBatchSize
	 * @param 	theBatchSize 
	 * @param 	theDataExtCfgRepos 
	 *************************************************************************************
	 */
	public ComponentBatch(
		QualTX 									theQualTX, 
		TradeLaneStatusTracker 					theStatusTracker, 
		int 									theStartIndex, 
		int 									theBatchSize, 
		DataExtensionConfigurationRepository 	theDataExtCfgRepos)
		throws Exception
	{
		this.qualTX = theQualTX;
		this.maxBatchSize = theBatchSize;
		this.compList = new ArrayList<>();
		this.dataExtCfgRepos = theDataExtCfgRepos;
		this.statusTracker = theStatusTracker;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theBOMComp
	 *************************************************************************************
	 */
	public void addBOMComponent(BOMComponent theBOMComp)
		throws Exception
	{
		Component aComp;
		
		aComp = new Component(theBOMComp);
		this.compList.add(aComp);
		
		if (this.compList.size() >= this.maxBatchSize) {
			throw new ComponentMaxBatchSizeReachedException();
		}	
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public void createQualTXComponents()
		throws Exception
	{
		for (Component aComp : this.compList) {
			if(ComponentType.DEFUALT.EXCLUDE_QUALIFICATION.name().equalsIgnoreCase(aComp.bomComp.component_type)
					|| ComponentType.DEFUALT.PACKING.name().equalsIgnoreCase(aComp.bomComp.component_type))
				continue;
				
			aComp.createQualTXComponent();
		}
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theComponentIndex
	 *************************************************************************************
	 */
	public BOMComponent getBOMComponent(int theComponentIndex)
		throws Exception
	{
		if ((theComponentIndex < 0) || (theComponentIndex >= this.getSize())) {
			throw new IndexOutOfBoundsException();
		}
		
		return this.compList.get(theComponentIndex).bomComp;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public long getBOMKey()
		throws Exception
	{
		if (this.compList.size() == 0) {
			throw new IllegalStateException("Batch contains no components");
		}
		
		return this.compList.get(0).bomComp.alt_key_bom;
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public int getEndIndex()
		throws Exception
	{
		return this.startIndex + this.getSize() - 1;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 * 
	 * @param	theComponentIndex
	 *************************************************************************************
	 */
	public QualTXComponent getQualTXComponent(int theComponentIndex)
		throws Exception
	{
		if ((theComponentIndex < 0) || (theComponentIndex >= this.getSize())) {
			throw new IndexOutOfBoundsException();
		}
		
		return this.compList.get(theComponentIndex).qualTXComponent;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public int getSize()
		throws Exception
	{
		return this.compList.size();
	}
	
	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public int getStartIndex()
		throws Exception
	{
		return this.startIndex;
	}

	/**
	 *************************************************************************************
	 * <P>
	 * </P>
	 *************************************************************************************
	 */
	public ArrayList<String> getWorkIdentifiersForTask() 
		throws Exception 
	{
		ArrayList<String>	aList;
		
		aList = new ArrayList<>();
		
		for (int aIndex = 0; aIndex < this.getSize(); aIndex++) {
			BOMComponent	aBOMComp = this.getBOMComponent(aIndex);
//			QualTXComponent	aQualTXComp = this.getQualTXComponent(aIndex);
			
			aList.add(StandardBOMRelatedWorkIdentifierGenerator.execute(aBOMComp.alt_key_bom, aBOMComp.alt_key_comp, "COMP"));
			
//			if (aQualTXComp == null) {
//				aList.add(StandardBOMRelatedWorkIdentifierGenerator.execute(aBOMComp.alt_key_bom, aBOMComp.alt_key_comp, "COMP"));
//			}
//			else {
//				aList.add(StandardBOMRelatedWorkIdentifierGenerator.execute(aBOMComp.alt_key_bom, aQualTXComp.alt_key_comp, "QTXCOMP"));
//			}
		}
		
		return aList;
	}
}
