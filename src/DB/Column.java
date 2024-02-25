package DB;

import java.util.Date;

public class Column {
	 private String name;
	 private String type;
	 private int maxInt;
	 private int minInt;
	 private String maxString;
	 private String minString;
	 private double maxDouble;
	 private double minDouble;
	 private Date maxDate;
	 private Date minDate;
	 private String used;
	 
	    public Column(String name, String type, int min, int max) throws DBAppException {
				switch(type){
				case "java.lang.Integer":
				case "java.lang.String":
				case "java.lang.Double":
				case "java.util.Date":break;
				default: throw new DBAppException("The Column: "+name+" is of type: "+type+" which is not accpeted"+"\n"
				+"The Accepted types are java.lang.Integer, java.lang.String, java.lang.Double & java.util.Date");
				}
	        this.name = name;
	        this.type = type;
	        this.minInt = min;
	        this.maxInt = max;
	        this.used="int";
	        
	    }
	    public Column(String name, String type, String min, String max)  throws DBAppException{
			switch(type){
			case "java.lang.Integer":
			case "java.lang.String":
			case "java.lang.Double":
			case "java.util.Date":break;
			default: throw new DBAppException("The Column: "+name+" is of type: "+type+" which is not accpeted"+"\n"
			+"The Accepted types are java.lang.Integer, java.lang.String, java.lang.Double & java.util.Date");
			}
	        this.name = name;
	        this.type = type;
	        this.minString = min;
	        this.maxString = max;
	        this.used="String";
	        
	    }
	    public Column(String name, String type, double min, double max)  throws DBAppException{
			switch(type){
			case "java.lang.Integer":
			case "java.lang.String":
			case "java.lang.Double":
			case "java.util.Date":break;
			default: throw new DBAppException("The Column: "+name+" is of type: "+type+" which is not accpeted"+"\n"
			+"The Accepted types are java.lang.Integer, java.lang.String, java.lang.Double & java.util.Date");
			}
	        this.name = name;
	        this.type = type;
	        this.minDouble = min;
	        this.maxDouble = max;
	        this.used="double";
	        
	    }
	    public Column(String name, String type, Date min, Date max)  throws DBAppException{
			switch(type){
			case "java.lang.Integer":
			case "java.lang.String":
			case "java.lang.Double":
			case "java.util.Date":break;
			default: throw new DBAppException("The Column: "+name+" is of type: "+type+" which is not accpeted"+"\n"
			+"The Accepted types are java.lang.Integer, java.lang.String, java.lang.Double & java.util.Date");
			}
	        this.name = name;
	        this.type = type;
	        this.minDate = min;
	        this.maxDate = max;
	        this.used="Date";
	    }


	    public String getName() {
	        return name;
	    }

	    public String getType() {
	        return type;
	    }
		public int getMaxInt() {
			return maxInt;
		}
		public void setMaxInt(int maxInt) {
			this.maxInt = maxInt;
		}
		public int getMinInt() {
			return minInt;
		}
		public void setMinInt(int minInt) {
			this.minInt = minInt;
		}
		public String getMaxString() {
			return maxString;
		}
		public void setMaxString(String maxString) {
			this.maxString = maxString;
		}
		public String getMinString() {
			return minString;
		}
		public void setMinString(String minString) {
			this.minString = minString;
		}
		public double getMaxDouble() {
			return maxDouble;
		}
		public void setMaxDouble(double maxDouble) {
			this.maxDouble = maxDouble;
		}
		public double getMinDouble() {
			return minDouble;
		}
		public void setMinDouble(double minDouble) {
			this.minDouble = minDouble;
		}
		
		public Date getMaxDate() {
			return maxDate;
		}
		public void setMaxDate(Date maxDate) {
			this.maxDate = maxDate;
		}
		public Date getMinDate() {
			return minDate;
		}
		public void setMinDate(Date minDate) {
			this.minDate = minDate;
		}
		public String getUsed() {
			return used;
		}
		public String toString() {
			switch(this.used) {
			case "int":return this.name+" "+this.type+" "+this.minInt+" "+this.maxInt;
			case "double":return this.name+" "+this.type+" "+this.minDouble+" "+this.maxDouble;
			case "Date":return this.name+" "+this.type+" "+this.minDate+" "+this.maxDate;
			default:return this.name+" "+this.type+" "+this.minString+" "+this.maxString;
			}
	    }
}
