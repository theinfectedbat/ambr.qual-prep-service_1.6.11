package com.ambr.gtm.fta.qts.api;

import java.math.BigDecimal;

import org.junit.Assert;
import org.junit.Test;

import com.ambr.gtm.fta.qts.QTXWorkDetails;
import com.ambr.gtm.fta.qts.util.BitFlag;

public class TestBitFlag {

	public TestBitFlag() {
		// TODO Auto-generated constructor stub
	}
	
//	@Test
	public void testBitFlag()
	{
		BitFlag flag = new BitFlag(0);
		
		flag.setBit(com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_ADDED);
		
		System.out.println(flag.getFlag() == com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_ADDED);
		System.out.println(flag.getFlag());
		Assert.assertEquals(flag.getFlag(), com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_ADDED);
		Assert.assertEquals(flag.isBitSet(com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_ADDED), true);

		
		flag.setBit(com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_ADDED);
		flag.setBit(com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_ADDED);
		System.out.println(flag.getFlag() == com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_ADDED);
		Assert.assertEquals(flag.isBitSet(com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_ADDED), true);
		Assert.assertEquals(flag.isBitSet(com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_DELETED), false);
		Assert.assertEquals(flag.isBitSet(com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_DELETED), false);
		
		flag.setBit(com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_DELETED);
		flag.setBit(com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_MODIFIED);
		Assert.assertEquals(flag.isBitSet(com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_ADDED), true);
		Assert.assertEquals(flag.isBitSet(com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_DELETED), true);
		Assert.assertEquals(flag.isBitSet(com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_MODIFIED), true);
		System.out.println(flag.getFlag());

		flag.unsetBit(com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_MODIFIED);
		Assert.assertEquals(flag.isBitSet(com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_ADDED), true);
		Assert.assertEquals(flag.isBitSet(com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_DELETED), true);
		Assert.assertEquals(flag.isBitSet(com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_MODIFIED), false);
		System.out.println(flag.getFlag());
		
		QTXWorkDetails details = new QTXWorkDetails();
		details.setReasonCodeFlag(com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_ADDED);
		details.setReasonCodeFlag(com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_MODIFIED);
		Assert.assertEquals(details.isReasonCodeFlagSet(com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_ADDED), true);
		Assert.assertEquals(details.isReasonCodeFlagSet(com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_DELETED), false);
		Assert.assertEquals(details.isReasonCodeFlagSet(com.ambr.gtm.fta.qts.RequalificationWorkCodes.BOM_COMP_MODIFIED), true);
	}
	
//	@Test
	public void testNumber()
	{
		Double a = new Double(1.1);
		String b = "1.1000";
		
		System.out.println(a.equals(new Double(b)));
		
		Double x = new Double(1.10);
		BigDecimal y = new BigDecimal(x.toString());
		BigDecimal z = new BigDecimal("1.1");
		
		System.out.println("BD4 " + y.equals(z));
		
		System.out.println(x + "/" + y + "/" + z);
		
		Long d = new Long(1);
		BigDecimal e = new BigDecimal(d.toString());
		BigDecimal f = new BigDecimal("1");

		System.out.println("BD5 " + e.equals(f));
		System.out.println(d + "/" + e + "/" + f);
	}
	
}
