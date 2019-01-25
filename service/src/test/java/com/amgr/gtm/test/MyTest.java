package com.amgr.gtm.test;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.theories.suppliers.TestedOn;
import org.springframework.core.GenericTypeResolver;

import com.ambr.gtm.fta.qps.qualtx.engine.QualTX;
import com.ambr.gtm.fta.qps.qualtx.engine.QualTXComponent;
import com.ambr.platform.rdbms.orm.DataRecordColumnModification;
import com.ambr.platform.rdbms.orm.DataRecordModificationTracker;
import com.ambr.platform.rdbms.orm.EntityModificationTracker;
import com.ambr.platform.rdbms.util.ParameterizedSQLStatementUtility;

public class MyTest 
{
	ArrayList<QualTX>	m_list = new ArrayList<QualTX>();

//	@Test
	public void test1()
		throws Exception
	{
		QualTX									aQualTX;
		QualTXComponent							aQualTXComp;
		EntityModificationTracker<QualTX>		aTracker;
		
		aQualTX = new QualTX();
		aQualTX.alt_key_qualtx = 1L;
		aQualTX.ctry_of_import = "US";
		aQualTX.fta_code = "NAFTA";
		
		aTracker = new EntityModificationTracker<QualTX>(aQualTX);
		aTracker.startTracking();

		aQualTXComp = aQualTX.createComponent();
		aQualTXComp.area = 2.3;
		aQualTXComp.description = "COMPUTER";
		
//		aTracker = new EntityModificationTracker<QualTX>(QualTX.class);
//		aTracker.startTracking(aQualTX);
		
		aTracker.stopTracking();
		System.out.println(MessageFormat.format("1. New records [{0}]", aTracker.getNewRecords().size()));
		System.out.println(MessageFormat.format("1. Modified records [{0}]", aTracker.getModifiedRecords().size()));
		System.out.println(MessageFormat.format("1. Deleted records [{0}]", aTracker.getDeletedRecords().size()));
		System.out.println();
		
		aTracker.startTracking();
		aQualTX.alt_key_qualtx = new Long(1);
		aQualTX.ctry_of_import = MessageFormat.format("{0}", "USA");
		aQualTX.fta_code = "NAFTA_MX";
		aQualTXComp.area = 2.3;

		aTracker.stopTracking();
		System.out.println(MessageFormat.format("2. New records [{0}]", aTracker.getNewRecords().size()));
		System.out.println(MessageFormat.format("2. Modified records [{0}]", aTracker.getModifiedRecords().size()));
		
		for (DataRecordModificationTracker<?> aRecordTracker : aTracker.getModifiedRecords()) {
			
			for (DataRecordColumnModification aColMod : aRecordTracker.getColumnModifications()) {
				System.out.print("2.   ");
				System.out.println(aColMod.toString());
			}
		}
		
		System.out.println(MessageFormat.format("2. Deleted records [{0}]", aTracker.getDeletedRecords().size()));
		
		System.out.println();
		
		aTracker.startTracking();
		aQualTX.compList.remove(0);

		aTracker.stopTracking();
		System.out.println(MessageFormat.format("3. New records [{0}]", aTracker.getNewRecords().size()));
		System.out.println(MessageFormat.format("3. Modified records [{0}]", aTracker.getModifiedRecords().size()));
		System.out.println(MessageFormat.format("3. Deleted records [{0}]", aTracker.getDeletedRecords().size()));
		System.out.println();
	}

//	@Test
	public void test2()
		throws Exception
	{
		ArrayList<QualTX>	aList = new ArrayList<QualTX>();
		
		for (Type aType : ((ParameterizedType)(aList.getClass().getGenericSuperclass())).getActualTypeArguments()) {
			if (aType instanceof Class) {
				System.out.println();
			}
			System.out.println();
		}
		
		Class<?> aClass = GenericTypeResolver.resolveTypeArgument(m_list.getClass(), AbstractCollection.class);
		Class<?>[]	aClassList =  GenericTypeResolver.resolveTypeArguments(m_list.getClass(),  AbstractCollection.class);
		if (aClassList != null) {
		
			for (Class<?> aClass2 : GenericTypeResolver.resolveTypeArguments(aList.getClass(),  ArrayList.class)) {
				System.out.println();
				
			}
		}
		
		System.out.println(List.class.isAssignableFrom(aList.getClass()));
		for (TypeVariable aTypeVar : aList.getClass().getTypeParameters()) {
			System.out.println(aTypeVar.getName());
			for (Type aType : aTypeVar.getBounds()) {
				System.out.println(aType.getTypeName());
			}
		}
	}
	
//	@Test
	public void test3()
		throws Exception
	{
		System.out.println(MessageFormat.format("date [{0,date,yyyy/MM/dd HH:mm:ss}]", new Timestamp(System.currentTimeMillis())));
	}
	
//	@Test
	public void test4()
		throws Exception
	{
		ParameterizedSQLStatementUtility	aUtil;
		
		aUtil = new ParameterizedSQLStatementUtility(
			"select * from mdi_bom where alt_key_bom <> 123", 
//			"select * from mdi_bom where alt_key_bom <> ? and created_date <> ? and created_by <> ?", 
			new Object[]{123, new Timestamp(System.currentTimeMillis()), "claude"}
		);
		
		System.out.println(aUtil.convertToStaticSQLText());
		
	}
}
