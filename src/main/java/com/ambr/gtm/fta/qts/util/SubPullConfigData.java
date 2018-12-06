package com.ambr.gtm.fta.qts.util;

import java.util.List;

public class SubPullConfigData
{

	private String					headerHsLength;
	private String					compCtry;
	private String					headerCtry;
	private String					ftaCode;
	private String					ftaCoi;
	private String					hsLength;

	private String					manufactureCountryChecked;
	private List<BaseHSFallConfg>	baseHSfallConfList;

	public List<BaseHSFallConfg> getBaseHSfallConfList()
	{
		return baseHSfallConfList;
	}

	public void setBaseHSfallConfList(List<BaseHSFallConfg> baseHSfallConfList)
	{
		this.baseHSfallConfList = baseHSfallConfList;
	}

	public String getHeaderHsLength()
	{
		return headerHsLength;
	}

	public void setHeaderHsLength(String headerHsLength)
	{
		this.headerHsLength = headerHsLength;
	}

	public String getCompCtry()
	{
		return compCtry;
	}

	public void setCompCtry(String compCtry)
	{
		this.compCtry = compCtry;
	}

	public String getHeaderCtry()
	{
		return headerCtry;
	}

	public void setHeaderCtry(String headerCtry)
	{
		this.headerCtry = headerCtry;
	}

	public String getFtaCode()
	{
		return ftaCode;
	}

	public void setFtaCode(String ftaCode)
	{
		this.ftaCode = ftaCode;
	}

	public String getFtaCoi()
	{
		return ftaCoi;
	}

	public void setFtaCoi(String ftaCoi)
	{
		this.ftaCoi = ftaCoi;
	}

	public String getCompHsLength()
	{
		return hsLength;
	}

	public void setCompHsLength(String hsLength)
	{
		this.hsLength = hsLength;
	}

	public void setManufactureCountry(String chacked)
	{
		this.manufactureCountryChecked = chacked;
	}

	public String getManufactureCountry()
	{
		return this.manufactureCountryChecked;
	}

	public class BaseHSFallConfg
	{
		private String	headerHsLength;
		private String	compCtry;
		private String	headerCtry;
		private String	compHslength;

		public String getHeaderHsLength()
		{
			return headerHsLength;
		}

		public void setHeaderHsLength(String headerHsLength)
		{
			this.headerHsLength = headerHsLength;
		}

		public String getCompCtry()
		{
			return compCtry;
		}

		public void setCompCtry(String compCtry)
		{
			this.compCtry = compCtry;
		}

		public String getHeaderCtry()
		{
			return headerCtry;
		}

		public void setHeaderCtry(String headerCtry)
		{
			this.headerCtry = headerCtry;
		}

		public String getCompHslength()
		{
			return compHslength;
		}

		public void setCompHslength(String compHslength)
		{
			this.compHslength = compHslength;
		}
	}
}
