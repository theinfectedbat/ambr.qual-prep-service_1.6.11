package com.ambr.gtm.fta.qts;

public class CommonUtility {

	public static String parametrizeSQLInClause(String theSQLPart, int theParamValuesLength,
			int theParametrizationIndex, boolean isNotIn) throws Exception {
		String aCondition = "(";
		String aPartialExpression = "";

		if (theParametrizationIndex == 1) {
			aCondition += theSQLPart + (isNotIn ? " <> " : " = ") + ":INCLAUSE: ";
			aPartialExpression = (isNotIn ? " AND " : " OR ") + theSQLPart + (isNotIn ? " <> " : " = ");
		} else {
			aCondition += "(";
			aPartialExpression += "(";
			for (String aQueryPart : theSQLPart.split(",")) {
				aQueryPart = aQueryPart.replaceAll("\\(", "").replaceAll("\\)", "");
				aCondition += aQueryPart + (isNotIn ? " <> ? " : " = ? ") + "AND ";
				aPartialExpression += aQueryPart + (isNotIn ? " <> ? " : " = ? ") + "AND ";
			}
			aCondition = aCondition.substring(0, aCondition.lastIndexOf("AND")) + ") :INCLAUSE: ";
			aPartialExpression = (isNotIn ? " AND " : " OR ")
					+ aPartialExpression.substring(0, aPartialExpression.lastIndexOf("AND")) + " )";
		}

		StringBuffer aParamBuffer = new StringBuffer();

		if (theParametrizationIndex < 1 || theParamValuesLength % theParametrizationIndex != 0)
			throw new Exception("The intended number of parameters doesn't match the parametrization index. "
					+ theParamValuesLength + " % " + theParametrizationIndex + " != 0");

		for (int i = 0; i < theParamValuesLength; i += theParametrizationIndex) {
			if (theParametrizationIndex == 1)
				aParamBuffer = aParamBuffer.append("? ").append(aPartialExpression);
			else {
				aParamBuffer = aParamBuffer.append(aPartialExpression);
			}
		}
		return aCondition.replace(":INCLAUSE:", aParamBuffer.substring(0, aParamBuffer.lastIndexOf(aPartialExpression)))
				+ ")";

	}
}
