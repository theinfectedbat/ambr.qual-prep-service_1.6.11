package com.ambr.gtm.fta.qts;

public class RepositoryException extends Exception
{
	private static final long serialVersionUID = 1L;

	public RepositoryException()
	{
	}

	public RepositoryException(String message)
	{
		super(message);
	}

	public RepositoryException(Throwable cause)
	{
		super(cause);
	}

	public RepositoryException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public RepositoryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
