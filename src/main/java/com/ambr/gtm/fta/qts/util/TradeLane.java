package com.ambr.gtm.fta.qts.util;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class TradeLane implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
    
	private String	ftaCode;
	private String	ctryOfImport;
	
	public TradeLane(String ftaCode, String ctrycode) {
		this.ftaCode = ftaCode;
		this.ctryOfImport = ctrycode;
	}

	public String getFtaCode()
	{
		return ftaCode;
	}
	public void setFtaCode(String ftaCode)
	{
		this.ftaCode=ftaCode;
	}
	public String getCtryOfImport()
	{
		return ctryOfImport;
	}

	public void setCtryOfImport(String ctryOfImport) {
		this.ctryOfImport = ctryOfImport;
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(ctryOfImport,ftaCode);
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		TradeLane other = (TradeLane) obj;
		if (ctryOfImport == null)
		{
			if (other.ctryOfImport != null) return false;
		}
		else if (!ctryOfImport.equals(other.ctryOfImport)) return false;
		if (ftaCode == null)
		{
			if (other.ftaCode != null) return false;
		}
		else if (!ftaCode.equals(other.ftaCode)) return false;
		return true;
	}

}
