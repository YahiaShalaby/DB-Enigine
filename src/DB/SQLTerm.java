package DB;

public class SQLTerm {
	private String strTableName;
	private String strColumnName;
	private String strOperator;
	private Object ObjValue;
	public SQLTerm() {
		
	}
	
	public SQLTerm(String strTableName, String strColumnName, String strOperator, Object objValue) {
		this.strTableName = strTableName;
		this.strColumnName = strColumnName;
		this.strOperator = strOperator;
		this.ObjValue = objValue;
	}

	public String getStrTableName() {
		return strTableName;
	}
	public void setStrTableName(String strTableName) {
		this.strTableName = strTableName;
	}
	public String getStrColumnName() {
		return strColumnName;
	}
	public void setStrColumnName(String strColumnName) {
		this.strColumnName = strColumnName;
	}
	public String getStrOperator() {
		return strOperator;
	}
	public void setStrOperator(String strOperator) {
		this.strOperator = strOperator;
	}
	public Object getObjValue() {
		return ObjValue;
	}
	public void setObjValue(Object objValue) {
		ObjValue = objValue;
	}
	

}
