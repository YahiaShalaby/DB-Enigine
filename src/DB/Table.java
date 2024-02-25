package DB;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

public class Table {
	private String tableName;
    private String pk;
    private List<Column> columns;
    private int n;
    private int ttl;
    private int p;
    private boolean empty;

	public Table(String tableName, List<Column> columns, String pk) {
		super();
		this.tableName = tableName;
		this.columns = columns;
		this.pk = pk;
		this.ttl =0;
		this.p = 0;
		this.empty = true;
	}
	
	
//	public void createTable(String tableName, List<Column> columns, int n) throws IOException { //will be used for requirement 12 for ms2
//		for(int i=0;i<columns.size();i++) {/// checks if table is constructed of only a variation of the 4 basic data types
//			switch(columns.get(i).getType()){
//			case "java.lang.Integer":
//			case "java.lang.String":
//			case "java.lang.Double":
//			case "java.util.Date":break;
//			default: int x = i+1;System.out.println("Invalid Data Type Detected in column "+x);return;
//			}
//		}
//		Table x = new Table(tableName, columns,n);
//        allTables.add(x);
//        System.out.println("Table Created Successfully");
//	}
//	
	public void insert(List<Object> values) throws IOException {

		for(int i=0;i<values.size();i++) {
			String tTmp = this.getColumns().get(i).getType();
			String vTmp =values.get(i).getClass().getName();
			if(!(vTmp.equals(tTmp))) {
				System.out.println("Invalid Entry!");
				return;
			}
			
		}
		if (empty==true || (ttl/p)>=n) {//// first insert or exceeds current page
			p++;  if(empty==true){empty=false;}
			String filename = this.tableName + this.p +".csv";
			File file = new File("C:\\Users\\Yahia Shalaby\\Desktop\\UNI\\DB II\\DBMS1\\dbtables\\"+filename);
			insertHelper(values,file.toString());
			addToPageMeta(filename,file.toString());
		}
		else {
			//// normal insert in current page
			String filename = "C:\\Users\\Yahia Shalaby\\Desktop\\UNI\\DB II\\DBMS1\\dbtables\\"+this.tableName + this.p +".csv";
			insertHelper(values,filename);
		}
	ttl++;	
	}
	public void insertHelper(List<Object> values, String filename) throws IOException {
		///////////////////////DON'T FORGET TO SORT////////////////////////////////////
        ///////////////////////////////////////////////////////////////////////////////
        ///////////////////////////////////////////////////////////////////////////////
		
		
		
        BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true));// Open the file for appending

        //StringBuilder sb = new StringBuilder();
        String sb ="";
        for (Object value : values) {
            //sb.append(value.toString()).append(",");
        	sb+=value.toString()+",";
        }
        //sb.deleteCharAt(sb.length() - 1); // removes the last comma
        sb = sb.substring(0, sb.length()-1);

        //writer.write(sb.toString()); // Write the values to a new line in the file
        writer.write(sb);
        writer.newLine();
        writer.close();     // Close the writer
	}
	public void addToPageMeta(String filename, String filepath) throws IOException {
		String s = this.getTableName()+", "+"Table"+", "+filename+", "+filepath;
		String r = "C:\\Users\\Yahia Shalaby\\Desktop\\UNI\\DB II\\DBMS1\\dbtables\\pageMeta.txt";
		BufferedWriter writer = new BufferedWriter(new FileWriter(r, true));
		writer.write(s);
        writer.newLine();
        writer.close(); 
		
	}
	
	
	public String getTableName() {
		return tableName;
	}
	public List<Column> getColumns() {
		return columns;
	}
	public int getN() {
		return n;
	}
	public int getP() {
		return p;
	}
	
	public void setP(int p) {
		this.p = p;
	}


	public boolean isEmpty() {
		return empty;
	}
	

	public void setEmpty(boolean empty) {
		this.empty = empty;
	}


	public String getPk() {
		return pk;
	}


	public int getTtl() {
		return ttl;
	}
	
	public void setTtl(int ttl) {
		this.ttl = ttl;
	}


	public String toString() {
		return tableName;
	}
	public int pkIndex() throws DBAppException {
		for(int i=0;i<this.getColumns().size();i++) {
			if(this.getPk().equals(this.getColumns().get(i).getName()))
				return i;
		}
		throw new DBAppException("Primary Key not found");
	}
	public int getIndex(String name) throws DBAppException {
		for(int i=0;i<this.getColumns().size();i++) {
			if(name.equals(this.getColumns().get(i).getName()))
				return i;
		}
		throw new DBAppException("Column not found");
	}
	public void tableHas(String name) throws DBAppException {
		for(int i=0;i<this.getColumns().size();i++) {
			if(name.equals(this.getColumns().get(i).getName()))
				return;
		}
		throw new DBAppException("Column not found");
	}


//	public static void main(String[] args) {
////		ArrayList<String> x = new ArrayList<String>();
////		x.add("shalaby");x.add("messi");x.add("cr7");
////		Iterator y = x.iterator();
////
////		while (y.hasNext()) {
////            // Returns the next element.
////            System.out.println(y.next());
////        }
////		String s1= "2002/05/01";
////		String s2= "2012/05/01";
////		System.out.println(s1.compareTo(s2));
//		System.out.println("100".compareTo("20"));
//	}

}
