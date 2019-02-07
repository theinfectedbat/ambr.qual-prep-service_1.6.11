package com.ambr.gtm.fta.qts.workmgmt;

import java.lang.reflect.Field;
//import java.sql.Connection;
//import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
//import java.util.ArrayList;
import java.util.HashMap;

public class DataLoader<T>
{
	private Class<T> classType;
	private HashMap<String, Field> mapping;
	
	public DataLoader(Class<T> classType)
	{
		this.classType = classType;
		this.mapping = this.getColumnAnnotatedFieldMap(this.classType);
	}

	private HashMap<String, Field> getColumnAnnotatedFieldMap(Class<?> targetClass)
	{
		HashMap<String, Field> mapping = new HashMap<String, Field>();
		Field[] declaredFields = targetClass.getDeclaredFields();
		
		if (declaredFields == null) return mapping;
		
		for(Field field : declaredFields)
		{
			javax.persistence.Column column = field.getAnnotation(javax.persistence.Column.class);
			
			if (column != null)
			{
				mapping.put(column.name().toUpperCase(), field);
			}
		}
		
		return mapping;
	}
	
	//TODO review all java types to see if any are not compensated for
	public T getObjectFromResultSet(ResultSet results) throws InstantiationException, SQLException, IllegalAccessException
	{
		T targetObject = this.classType.newInstance();		
		
		ResultSetMetaData metaData = results.getMetaData();
		int maxColumns = metaData.getColumnCount();
		for (int i=1; i<=maxColumns; i++)
		{
			Field field = this.mapping.get(metaData.getColumnName(i));
			
			if (field != null)
			{
				int type = metaData.getColumnType(i);
				Class<?> fieldClassType = field.getType();
				Object value = null;
				
				if (type == java.sql.Types.DATE)
					value = results.getDate(i);
				else if (type == java.sql.Types.TIMESTAMP)
					value = results.getTimestamp(i);
				else if (type == java.sql.Types.CLOB)
					value = results.getString(i);
				else value = results.getObject(i);
				
				if (value == null) continue;
				
				if (fieldClassType == Long.class || fieldClassType == long.class)
				{
					field.set(targetObject, results.getLong(i));
				}
				else if (fieldClassType == Double.class || fieldClassType == double.class)
				{
					field.set(targetObject, results.getDouble(i));
				}
				else if (fieldClassType == Integer.class || fieldClassType == int.class)
				{
					field.set(targetObject, results.getInt(i));
				}
				else if ((fieldClassType instanceof Class) && ((Class<?>) fieldClassType).isEnum())
				{
					//TODO this is based on the current method of storing the ordinal value of the enum and translating back to the enum constant
					field.set(targetObject, fieldClassType.getEnumConstants()[results.getInt(i)]);
				}
				else
				{
					field.set(targetObject, value);
				}
			}
		}
		
		return targetObject;
	}
	
	public static Long getLong(ResultSet resultSet, String columnName) throws SQLException
	{
		if (resultSet.getObject(columnName) != null) return resultSet.getLong(columnName);
		
		return null;
	}
}
