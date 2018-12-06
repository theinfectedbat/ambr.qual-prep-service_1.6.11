package com.ambr.gtm.fta.qps.gpmsrciva;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ambr.platform.utils.log.MessageFormatter;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 *****************************************************************************************
 * <P>
 * </P>
 *****************************************************************************************
 */
public class GPMSourceIVAProductContainer
	implements Serializable
{
	static final Logger			logger = LogManager.getLogger(GPMSourceIVAProductContainer.class);
	
	private static final long serialVersionUID = 1L;
	
	public long													prodKey;
	public ArrayList<Long>										prodSrcKeyList;
	private ArrayList<GPMSourceIVAProductSourceContainer>		prodSrcContainerList;
	
	@JsonIgnore
	private HashMap<Long, GPMSourceIVAProductSourceContainer> prodSourceKeyIndex;
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	public GPMSourceIVAProductContainer()
	{
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     * @param theProdKey 
     *************************************************************************************
     */
	public GPMSourceIVAProductContainer(long theProdKey)
		throws Exception
	{
		this.prodKey = theProdKey;
		this.prodSrcKeyList = new ArrayList<>();
		this.prodSrcContainerList = new ArrayList<>();
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     * 
     * @param	theProdSrcKey
     *************************************************************************************
     */
	public void add(long theProdSrcKey)
		throws Exception
	{
		if (!this.prodSrcKeyList.contains(theProdSrcKey)) {
			this.prodSrcKeyList.add(theProdSrcKey);
		}
	}
	
    /**
     *************************************************************************************
     * <P>
     * </P>
     *************************************************************************************
     */
	@JsonIgnore
	public ArrayList<GPMSourceIVAProductSourceContainer> getSourceContainers()
		throws Exception
	{
		return this.prodSrcContainerList;
	}
	
	public void indexByProdSourceKey()
	{
		this.prodSourceKeyIndex = new HashMap<Long, GPMSourceIVAProductSourceContainer>();
		
		if (this.prodSrcContainerList != null)
		{
			for (GPMSourceIVAProductSourceContainer prodSourceContainer : this.prodSrcContainerList)
			{
				this.prodSourceKeyIndex.put(prodSourceContainer.prodSrcKey, prodSourceContainer);
				
				prodSourceContainer.indexByIVAKey();
			}
		}
	}
	
	public GPMSourceIVAProductSourceContainer getGPMSourceIVAProductSourceContainerByProdSourceKey(long theProdSourceKey)
	{
		return this.prodSourceKeyIndex.get(theProdSourceKey);
	}
	
	public GPMSourceIVA getGPMSourceIVA(long theProdSourceKey, long theIVAKey)
	{
		GPMSourceIVAProductSourceContainer container = this.getGPMSourceIVAProductSourceContainerByProdSourceKey(theProdSourceKey);
		
		if (container != null)
		{
			return container.getGPMSourceIVA(theIVAKey);
		}
		
		return null;
	}
	
    /**
     *************************************************************************************
     * <P>
     * This is an internal method.  It should only be called by the specified cache.
     * </P>
     * 
     * @param theCache 
     *************************************************************************************
     */
	public void loadSourceContainers(GPMSourceIVAContainerCache theCache)
		throws Exception
	{
		this.prodSrcKeyList.forEach(
			(Long theProdSrcKey)->
			{
				try {
					GPMSourceIVAProductSourceContainer aContainer;

					aContainer = theCache.getSourceIVABySource(theProdSrcKey);
					if (aContainer != null) {
						this.prodSrcContainerList.add(aContainer);
					}
				}
				catch (Exception e) {
					MessageFormatter.error(logger, "loadSourceContainers", e, "PROD [{0}]", this.prodKey);
				}
			}
		);
	}
}
