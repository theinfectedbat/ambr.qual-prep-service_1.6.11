package com.ambr.gtm.fta.qts.workmgmt;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.JdbcTemplate;

import com.ambr.gtm.fta.qts.QTXWork;
import com.ambr.gtm.fta.qts.QTXWorkDetails;
import com.ambr.gtm.fta.qts.QTXWorkStatus;

public class QTXWorkResultSet extends SimpleDataLoaderResultSet<QTXWork>
{
	private DataLoader<QTXWorkDetails> loaderDetails;
	private DataLoader<QTXWorkStatus> loaderStatus;

	public QTXWorkResultSet(JdbcTemplate template)
	{
		super(QTXWork.class, template);
		
		this.loaderDetails = new DataLoader<QTXWorkDetails>(QTXWorkDetails.class);
		this.loaderStatus = new DataLoader<QTXWorkStatus>(QTXWorkStatus.class);
	}
	
	public QTXWork next() throws SQLException
	{
		try
		{
			if(this.resultSet.next())
			{
				this.object = this.loader.getObjectFromResultSet(this.resultSet);
					
				this.object.details = this.loaderDetails.getObjectFromResultSet(this.resultSet);
				this.object.status = this.loaderStatus.getObjectFromResultSet(this.resultSet);
				
				this.loadCount++;
			}
			else
				this.object = null;
		}
		catch (Exception e)
		{
			throw new SQLException("Error iterating through dataset", e);
		}
		
		return this.object;
	}
}
