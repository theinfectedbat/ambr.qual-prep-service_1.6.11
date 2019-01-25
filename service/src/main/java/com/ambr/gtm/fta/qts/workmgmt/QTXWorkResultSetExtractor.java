package com.ambr.gtm.fta.qts.workmgmt;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

import com.ambr.gtm.fta.qts.QTXWork;
import com.ambr.gtm.fta.qts.QTXWorkDetails;
import com.ambr.gtm.fta.qts.QTXWorkStatus;

public class QTXWorkResultSetExtractor implements ResultSetExtractor<List<QTXWork>>
{
	public QTXWorkResultSetExtractor()
	{
	}

	@Override
	public List<QTXWork> extractData(ResultSet rs) throws SQLException, DataAccessException
	{
		DataLoader<QTXWork> loaderWork = new DataLoader<QTXWork>(QTXWork.class);
		DataLoader<QTXWorkDetails> loaderDetails = new DataLoader<QTXWorkDetails>(QTXWorkDetails.class);
		DataLoader<QTXWorkStatus> loaderStatus = new DataLoader<QTXWorkStatus>(QTXWorkStatus.class);

		List<QTXWork> workList = new ArrayList<QTXWork>();
		
		try
		{
			while (rs.next())
			{
				QTXWork work = loaderWork.getObjectFromResultSet(rs);
					
				work.details = loaderDetails.getObjectFromResultSet(rs);
				work.status = loaderStatus.getObjectFromResultSet(rs);
				
				workList.add(work);
			}
		}
		catch (Exception e)
		{
			throw new SQLException("Error iterating through dataset", e);
		}
		
		return workList;
	}

}
