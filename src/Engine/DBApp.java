package Engine;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

//import org.junit.jupiter.api.Assertions;
import DB.Column;
import DB.DBAppException;
import DB.SQLTerm;
import DB.Table;

public class DBApp {
	private int MaximumRowsCountinTablePage;
	private int MaxRowsInDense;
	private ArrayList<Table> tableList;
	public DBApp() throws IOException {
		MaximumRowsCountinTablePage=0;
		MaxRowsInDense=0;
		this.init();
		//tableList = new  ArrayList<Table>();
	}
	
	public int getMaximumRowsCountinTablePage() {
		return MaximumRowsCountinTablePage;
	}

	public void setMaximumRowsCountinTablePage(int maximumRowsCountinTablePage) {
		MaximumRowsCountinTablePage = maximumRowsCountinTablePage;
	}
	
	public int getMaxRowsInDense() {
		return MaxRowsInDense;
	}

	public void setMaxRowsInDense(int maxRowsInDense) {
		MaxRowsInDense = maxRowsInDense;
	}

	public void init( ) throws IOException {
		 String configFilePath = "src/DBApp.config";
		 FileInputStream propsInput = new FileInputStream(configFilePath);
		 Properties prop = new Properties();
		 prop.load(propsInput);
		 this.setMaximumRowsCountinTablePage(Integer.parseInt(prop.getProperty("MaximumRowsCountinTablePage")));
		 this.setMaxRowsInDense(Integer.parseInt(prop.getProperty("MaxRowsInDense")));
		 File tableMeta = new File("Src/metadata.csv");
		 tableMeta.createNewFile();
//		 if(tableMeta.createNewFile()) {
//			 System.out.println("Table MetaData Created");
//		 }else {
//			 System.out.println("Table MetaData Created");
//		 }
		 File pageMeta = new File("Src/files.csv");
		 pageMeta.createNewFile();
	}
	public void createTable(String strTableName,String strClusteringKeyColumn, Hashtable<String,String> htblColNameType, Hashtable<String,String> htblColNameMin, 
			Hashtable<String,String> htblColNameMax ) throws IOException, DBAppException, ParseException {
		    ArrayList<String> names = new ArrayList<String>();
		    ArrayList<String> type = new ArrayList<String>();
		    ArrayList<String> min = new ArrayList<String>();
		    ArrayList<String> max = new ArrayList<String>();
		    ArrayList<Column> columns = new ArrayList<Column>();
		 for (Map.Entry<String, String> e : htblColNameType.entrySet()) {
			 names.add(e.getKey());
			 type.add(e.getValue());
		 }
		 for (Map.Entry<String, String> e : htblColNameMin.entrySet()) {			
			 min.add(e.getValue());
		 }
		 for (Map.Entry<String, String> e : htblColNameMax.entrySet()) {
			 max.add(e.getValue());
		 }
		 for(int i=0;i<names.size();i++) {
			 if(type.get(i).equals("java.lang.Integer")) {
				 Column ctemp = new Column(names.get(i),type.get(i),Integer.parseInt(min.get(i)),Integer.parseInt(max.get(i)));
				 columns.add(ctemp);
			 }
			 else{
				 if(type.get(i).equals("java.lang.Double")) {
					 Column ctemp = new Column(names.get(i),type.get(i),Double.parseDouble(min.get(i)),Double.parseDouble(max.get(i)));
					 columns.add(ctemp);
				 }else {
					 if(type.get(i).equals("java.util.Date")) {
						 SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
						 Date dateMin=formatter.parse(min.get(i));
						 Date dateMax=formatter.parse(max.get(i));

						 Column ctemp = new Column(names.get(i),type.get(i),dateMin,dateMax);
						 columns.add(ctemp);
					 }else {
						 Column ctemp = new Column(names.get(i),type.get(i),min.get(i),max.get(i));
						 columns.add(ctemp);
					 }
					
				 }
				 
			 }
			
		 }
		 String s="";
			String r = "Src/metadata.csv";
			BufferedWriter writer = new BufferedWriter(new FileWriter(r, true));
		 for(int i=0;i<columns.size();i++) {
			 switch(columns.get(i).getUsed()) {
				case "int": s = strTableName+", "+columns.get(i).getName()+", "+columns.get(i).getType()+", "
						 +strClusteringKeyColumn.equals(columns.get(i).getName())+", "+"null"+", "+"null"+", "+columns.get(i).getMinInt()+", "+columns.get(i).getMaxInt();
				 writer.write(s);
			        writer.newLine();break;
				case "double":s = strTableName+", "+columns.get(i).getName()+", "+columns.get(i).getType()+", "
						 +strClusteringKeyColumn.equals(columns.get(i).getName())+", "+"null"+", "+"null"+", "+columns.get(i).getMinDouble()+", "+columns.get(i).getMaxDouble();
				 writer.write(s);
			        writer.newLine();break;
				case "Date":
					 SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
					 Date dateMin=columns.get(i).getMinDate();
					 Date dateMax=columns.get(i).getMaxDate();
					 String mind = formatter.format(dateMin);
					 String maxd =formatter.format(dateMax);
//					s = strTableName+", "+columns.get(i).getName()+", "+columns.get(i).getType()+", "
//						 +strClusteringKeyColumn.equals(columns.get(i).getName())+", "+columns.get(i).getMinDate()+", "+columns.get(i).getMaxDate();
						s = strTableName+", "+columns.get(i).getName()+", "+columns.get(i).getType()+", "
								 +strClusteringKeyColumn.equals(columns.get(i).getName())+", "+"null"+", "+"null"+", "+mind+", "+maxd;
				 writer.write(s);
			        writer.newLine();break;
				default:s = strTableName+", "+columns.get(i).getName()+", "+columns.get(i).getType()+", "
						 +strClusteringKeyColumn.equals(columns.get(i).getName())+", "+"null"+", "+"null"+", "+columns.get(i).getMinString()+", "+columns.get(i).getMaxString();
				 writer.write(s);
			     writer.newLine();break;
			 }
			 
		 }
		 writer.close(); 
		 Table t = new Table(strTableName,columns,strClusteringKeyColumn);
		 //tableList.add(t);
		 System.out.println("Table Created Successfully");
	
	}
	
	public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException, NumberFormatException, IOException, ParseException{
		Table t = loadTable(strTableName);
			 int i=0;
			 ArrayList<Object> values = new ArrayList<Object>();
			 for (Map.Entry<String, Object> e : htblColNameValue.entrySet()) {
				 if(e.getKey().equals(t.getColumns().get(i).getName())) {//Validate Column Data Types
					 if(!e.getValue().getClass().getName().equals(t.getColumns().get(i).getType())) {
						 throw new DBAppException("Error During Insertion "+e.getKey() +"(Column Data Types Dont Match)");
					 }
					 switch(t.getColumns().get(i).getUsed()) {//Validate Ranges
					 case "int":if((Integer)e.getValue()<t.getColumns().get(i).getMinInt() ||(Integer)e.getValue()>t.getColumns().get(i).getMaxInt())
						 throw new DBAppException("Error During Insertion "+e.getKey() +" (is out of range)");break;
					 case "String":if(e.getValue().toString().compareTo(t.getColumns().get(i).getMinString())==-1 ||e.getValue().toString().compareTo(t.getColumns().get(i).getMaxString())==1)
						 throw new DBAppException("Error During Loading "+e.getKey() +" (is out of range)");break;
					 case "double":if((Double)e.getValue()<t.getColumns().get(i).getMinDouble() ||(Double)e.getValue()>t.getColumns().get(i).getMaxDouble())
						 throw new DBAppException("Error During Insertion "+e.getKey() +" (is out of range)");break;
					 case "Date":						
					 SimpleDateFormat inputFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
				     SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
//				     Date temp = inputFormat.parse(""+ e.getValue());
				     String tempS = formatter.format(e.getValue());				     
					 Date date=formatter.parse(tempS);
						 if(date.compareTo(t.getColumns().get(i).getMinDate())==-1 ||date.compareTo(t.getColumns().get(i).getMaxDate())==1)
						 throw new DBAppException("Error During Insertion "+e.getKey() +"  (is out of range)");break;
					 }
				 }
				 values.add(e.getValue());//Fits Column Data Types And Ranges
				 i++;
			 }
//			 int valuePos=1;
//			 ArrayList<Object> valuesCopy = values;
		t.setP(this.countPagesInMeta(strTableName));
		if(t.getP()==0) {
			 String filename = t.getTableName() + 1 +".csv";
			 File file = new File("Src/"+filename);
			 String filepath = file.toString();
			 addToPageMeta(filename, filepath,t);
			 insertHelper(values,filepath,t);
		}else {
			ArrayList<String> allPagePaths = getAllPagePaths(strTableName);
			String targetPath = getTargetPath(allPagePaths,htblColNameValue,t);
			//System.out.println(targetPath);
			if(targetPath.equals("")) {
				t.setP(t.getP()+1);
				 String filename = t.getTableName() + t.getP() +".csv";
				 File file = new File("Src/"+filename);
				 String filepath = file.toString();
				 addToPageMeta(filename, filepath,t);
				 insertHelper(values,filepath,t);
			}
			else {
				 ArrayList<Object> page = getPage(targetPath);
				 emptyPage(targetPath);
				 for(int j=0;j<values.size();j++) {
					    if(values.get(j).getClass().getName().equals("java.util.Date")) {
							 SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
							 Date date=(Date) values.get(j);
							 String d = formatter.format(date);
							 values.set(j, d);
					    }else {
					    	values.set(j, ""+values.get(j));
					    }
				 }
				 page.add(values);
				 sortPage(page,t);
				 insertHelper(page,targetPath,t);
			}
		}

		updateIndexInsertorDelete(t);

		
	}
	public void insertHelper(ArrayList<Object> values, String filepath,Table t) throws IOException, DBAppException, ParseException{
		ArrayList<String> allPagePaths= getAllPagePaths(t.getTableName());
		BufferedWriter writer = new BufferedWriter(new FileWriter(filepath, true));
		String sb="";
		int x = 0;
		int sz =0;
		while(!values.isEmpty()){
			if(x>=this.MaximumRowsCountinTablePage && t.getP()!=0) {
				while(!allPagePaths.get(0).equals(filepath)) {
					allPagePaths.remove(allPagePaths.get(0));
				}
				allPagePaths.remove(filepath);
				if(allPagePaths.isEmpty()) {
					 t.setP(t.getP()+1);
					 String filename = t.getTableName() + t.getP() +".csv";
					 File file = new File("Src/"+filename);
					 filepath = file.toString();
					 addToPageMeta(filename, filepath,t);
					 allPagePaths.add(filepath);
				     writer.close();
				     writer = new BufferedWriter(new FileWriter(filepath, true));
				     x = 0;
				}else {
					filepath =allPagePaths.get(0);
					ArrayList<Object> newPage = this.getPage(filepath);
					this.emptyPage(filepath);
					while(!newPage.isEmpty()) {
						values.add(newPage.remove(0));
					}
					sortPage(values,t);
				     writer.close();
				     writer = new BufferedWriter(new FileWriter(filepath, true));
				     x = 0;
				}
				 
			}
			if(values.get(0).getClass().getName().equals("java.util.ArrayList")) {///takes a page as input
				String tmpS ="";
				ArrayList<String> tmp =  (ArrayList<String>) values.get(0); 
				for(int j=0;j<tmp.size();j++) {
					tmpS+=tmp.get(j)+",";
				}
				tmpS = tmpS.substring(0, tmpS.length()-1);
			     writer.write(tmpS);
			     writer.newLine();
			     values.remove(0);
			     x++;
			       				
			}else {///only for 1st insertion
				for(int j=0;j<values.size();j++) {
				    if(values.get(j).getClass().getName().equals("java.util.Date")) {
						 SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
						 Date date=(Date) values.get(j);
						 String d = formatter.format(date);
						 sb+=d+",";
				    }else {
				    	sb+=values.get(j).toString()+",";
				    }
				}
				break;

			}
			
		}
		if(!sb.equals("")) {
			sb = sb.substring(0, sb.length()-1);
			writer.write(sb);
	        writer.newLine();
	        
		}
		System.out.println("Successful Insert in Table "+t.getTableName());
        writer.close();
	}
	
	public ArrayList<String> getAllPagePaths(String tableName) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader("Src/files.csv"));
		String line = br.readLine();
		ArrayList<String> result = new ArrayList<String>();
		while (line != null) {
			String[] content = line.split(", ");
			if(content[0].equals(tableName)&&content[1].equals("Table"))
				result.add(content[3]);
			line = br.readLine();
		}
		br.close();
		return result;
	}
	
	public String getTargetPath(ArrayList<String> allPagePaths, Hashtable<String,Object> htblColNameValue, Table t) throws IOException, DBAppException, ParseException {
		for(String path:allPagePaths) {
			ArrayList<Object> currentPageData = getPage(path);
			if(!currentPageData.isEmpty()) {
			ArrayList<String> minStringArray = (ArrayList<String>) currentPageData.get(0);
			ArrayList<String> maxStringArray = (ArrayList<String>) currentPageData.get(currentPageData.size()-1);
			ArrayList<Object> currentPageMin = stringToOriginal(minStringArray,t);
			ArrayList<Object> currentPageMax = stringToOriginal(maxStringArray,t);
			if(COMPARE(htblColNameValue.get(t.getPk()),currentPageMin.get(t.pkIndex()))>=0 || COMPARE(htblColNameValue.get(t.getPk()),currentPageMax.get(t.pkIndex()))<=0)
				return path;
		
		}
		}
		return "";
	}
	
	public static ArrayList<Object> stringToOriginal(ArrayList<String>stringArray, Table t) throws ParseException{
		ArrayList<Object> result = new ArrayList<Object> ();
		for(int i=0;i<t.getColumns().size();i++) {
			switch(t.getColumns().get(i).getType()) {
			case "java.lang.Integer": result.add(Integer.parseInt(stringArray.get(i)));break;
            case "java.lang.Double": result.add(Double.parseDouble(stringArray.get(i)));break;
            case "java.lang.String": result.add(stringArray.get(i));break;
            case "java.util.Date":	
            	SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
            	Date date=formatter.parse(stringArray.get(i));
            	result.add(date);break;
			}
		}
		return result;	
	}
	
	
	public static int COMPARE(Object a, Object b) throws DBAppException {
		if(a.getClass().getName().equals(b.getClass().getName())) {
			switch(a.getClass().getName()) {
			case "java.lang.Integer": 
				int intx = (int)a;
				int inty = (int)b;
				if(intx>inty)
					return 1;
				if(intx<inty)
					return -1;
				return 0;
            case "java.lang.Double": 
				double dblx = (double)a;
				double dbly = (double)b;
				if(dblx>dbly)
					return 1;
				if(dblx<dbly)
					return -1;
				return 0;
            case "java.lang.String":
				String stringx = a.toString();
				String stringy = b.toString();
				return stringx.compareTo(stringy);
            case "java.util.Date":
            	Date dateA=(Date)a;
            	Date dateB=(Date)b;
            	return dateA.compareTo(dateB);
			}
		}
		throw new DBAppException("Incompatible types for comparison");
	}
	
	public int countPagesInMeta(String tablename) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader("Src/files.csv"));
		String line = br.readLine();
		int c=0;
		while (line != null) {
			String[] content = line.split(", ");
			if(content[0].equals(tablename))
			c++;
			line = br.readLine();
		}
		br.close();
		return c;
	}
	
	
	public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String,Object> htblColNameValue ) throws DBAppException, NumberFormatException, IOException, ParseException{
		Table t = loadTable(strTableName);
		ArrayList<Object> page;
		Object clusterKey=null;
		String colName = "";
		 int z=0;
		 for (Map.Entry<String, Object> e : htblColNameValue.entrySet()) {
			 if(e.getKey().equals(t.getColumns().get(z).getName())) {
				 if(!e.getValue().getClass().getName().equals(t.getColumns().get(z).getType())) {
					 throw new DBAppException("Error During Update "+e.getKey() +"(Column Data Types Don't Match)");
	             }
				 colName =e.getKey();
            }
			 z++;
        }
		 
		switch(t.getColumns().get(t.pkIndex()).getType()) {
		case "java.lang.Integer": clusterKey = Integer.parseInt(strClusteringKeyValue);break;
		case "java.lang.Double": clusterKey = Double.parseDouble(strClusteringKeyValue);break;
		case "java.lang.String": clusterKey =strClusteringKeyValue;break;
		case "java.util.Date": 
			SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
        	Date date=formatter.parse(strClusteringKeyValue);
        	clusterKey = date;
			break;
		}
		if(checkIndexExists(strTableName, t.getPk())) {
			String filepath ="";
			//int position =0;
			String sparseIndexFilepath = this.getSparseFilepath(t.getTableName(),t.getPk());
			ArrayList<Object> indexPage = getPage(sparseIndexFilepath);
			ArrayList<String> stringRow = new ArrayList<String>();
			for(int j=0;j<indexPage.size();j++) {
				stringRow = (ArrayList<String>) indexPage.get(j);
					if(stringRow.get(0).compareTo(strClusteringKeyValue)==0) {
						filepath=stringRow.get(1);
						break;
					}
					if(j+1>=indexPage.size()) {
						filepath=stringRow.get(1);
						break;
					}
					ArrayList<String> Next = (ArrayList<String>) indexPage.get(j+1);
						if(stringRow.get(0).compareTo(strClusteringKeyValue)<=0 && Next.get(0).compareTo(strClusteringKeyValue)>=1) {
							filepath=stringRow.get(1);
							break;
						}
					
			}
			if(stringRow.isEmpty())
				 throw new DBAppException("Error During Update Couldn't find filepath from index");
			//filepath=filepath.substring(1);
			ArrayList<Object> pageData =getPage(filepath);
			for(int i=0;i<pageData.size();i++) {
				ArrayList<String> rowTemp = (ArrayList<String>) pageData.get(i);
				if(rowTemp.get(t.pkIndex()).equals(strClusteringKeyValue)) {
					rowTemp.set(t.getIndex(colName), ""+htblColNameValue.get(colName));
					pageData.set(i, rowTemp);
					emptyPage(filepath);
					insertHelper(pageData,filepath,t);
					this.updateIndexUpdate(t,rowTemp,""+i,filepath);
					System.out.println("Index was used for update");
					System.out.println("Successful Update Occured In Table "+t.getTableName());
					return;
				}
			}
			System.out.println("Error during update while using index");
			return;

		}
		ArrayList<String> allPagePaths = getAllPagePaths(strTableName);
		ArrayList<String> tempRowForIndex= new ArrayList<String>();String tempIndex="";String tempPath="";
		for(String path: allPagePaths) {
			ArrayList<Object> currPage = getPage(path);
			boolean pageFlag = false;
			for(int i=0;i<currPage.size();i++) {
				ArrayList<String> stringRow = (ArrayList<String>)currPage.get(i);
				ArrayList<Object> objRow = stringToOriginal(stringRow,t);
				boolean rowflag=false;
				 for (Map.Entry<String, Object> e : htblColNameValue.entrySet()) {
					 if(COMPARE(objRow.get(t.pkIndex()),clusterKey)==0) {
						 for (Map.Entry<String, Object> x : htblColNameValue.entrySet()) {
							 switch(x.getValue().getClass().getName()) {
								case "java.lang.Integer": stringRow.set(t.getIndex(x.getKey()), ""+x.getValue());break;
								case "java.lang.Double": stringRow.set(t.getIndex(x.getKey()), ""+x.getValue());break;
								case "java.lang.String": stringRow.set(t.getIndex(x.getKey()), ""+x.getValue());break;
								case "java.util.Date": 
									SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
						        	Date date=(Date)x.getValue();
						        	String dateS = formatter.format(date);
						        	stringRow.set(t.getIndex(x.getKey()), dateS);
									break;
							 }
							 
						 }
						 currPage.set(i, stringRow);
						 tempRowForIndex=stringRow;tempIndex+=i;tempPath=path;
						 pageFlag=true;break;//doesn't match all deletion criteria
					 }						 
		         }
				}
			if(pageFlag) {
					emptyPage(path);
					insertHelper(currPage,path,t);
					this.updateIndexUpdate(t,tempRowForIndex,tempIndex,tempPath);
					System.out.println("Successful Update Occured In Table "+t.getTableName());
			}
		}
		
	}
	
	
	
	public void deleteFromTable(String strTableName,  Hashtable<String,Object> htblColNameValue) throws DBAppException, NumberFormatException, IOException, ParseException {
		Table t = loadTable(strTableName);
		 int z=0;
		 ArrayList<String>columnNames = new ArrayList<String>();
		 ArrayList<String>values = new ArrayList<String>();
		 for (Map.Entry<String, Object> e : htblColNameValue.entrySet()) {
			 if(e.getKey().equals(t.getColumns().get(z).getName())) {
				 if(!e.getValue().getClass().getName().equals(t.getColumns().get(z).getType())) {
					 throw new DBAppException("Error During Delete "+e.getKey() +"(Column Data Types Don't Match)");
	             }
				 columnNames.add(e.getKey());
				 values.add(""+e.getValue());
           }
			 z++;
       }
			String indexedColumn = "";
			List<Column> tableColumns = t.getColumns();
			for(Column x:tableColumns) {
				if(checkIndexExists(t.getTableName(),x.getName())) {
					if(columnNames.contains(x.getName()))
						indexedColumn=x.getName();
				}
					
			}
		if(!indexedColumn.equals("")) {
			String Densefilepath ="";
			int position=0;
			String sparseIndexFilepath = this.getSparseFilepath(t.getTableName(),indexedColumn);
//			System.out.println(sparseIndexFilepath);
			ArrayList<Object> indexPage = getPage(sparseIndexFilepath);
//			System.out.println(indexPage.toString());
			ArrayList<String> stringRow = new ArrayList<String>();
			for(int j=0;j<indexPage.size();j++) {
				stringRow = (ArrayList<String>) indexPage.get(j);
					if(stringRow.get(0).compareTo(values.get(columnNames.indexOf(indexedColumn)))==0) {
						Densefilepath=stringRow.get(1);
						position = Integer.parseInt(stringRow.get(2));
						break;
					}
					if(j+1>=indexPage.size()) {
//						System.out.println(stringRow.toString());
						Densefilepath=stringRow.get(1);
						position = Integer.parseInt(stringRow.get(2));
						break;
					}
					ArrayList<String> Next = (ArrayList<String>) indexPage.get(j+1);
						if(stringRow.get(0).compareTo(values.get(columnNames.indexOf(indexedColumn)))<=0 && Next.get(0).compareTo(values.get(columnNames.indexOf(indexedColumn)))>=1) {
							Densefilepath=stringRow.get(1);
							position = Integer.parseInt(stringRow.get(2));
							break;
						}
			}
			////////////////In dense index:
			ArrayList<Object> densePage = (ArrayList<Object>) getPage(Densefilepath);
			List<Object >dense=densePage.subList(position, Math.min(position+this.MaxRowsInDense,densePage.size()-position));
			ArrayList<Object> toBeDeleted = new ArrayList<Object>();
			for(int i=0;i<dense.size();i++) {
				ArrayList<String> rowTemp = (ArrayList<String>) dense.get(i);
				if(rowTemp.get(0).equals(values.get(columnNames.indexOf(indexedColumn))))
					toBeDeleted.add(dense.get(i));
			}
			boolean deleteflag = false;
			for(Object toBeDeletedrow:toBeDeleted) {
				ArrayList<String> toBeDeletedrowTemp = (ArrayList<String>)toBeDeletedrow;
				int pos = Integer.parseInt(toBeDeletedrowTemp.get(1));// NB: position of index and file path are revresed on sparse and dende
				String path = toBeDeletedrowTemp.get(2);//////////////////////////////////////////////////////////////////////
			//////get page and go to position
				ArrayList<Object> tempPage = getPage(path);
				ArrayList<String> rowTemp = (ArrayList<String>)tempPage.get(pos);
			//////remove if satisfies all conditions				
				boolean flag = true;
				for(String x:columnNames) {
					if(!rowTemp.get(t.getIndex(x)).equals(values.get(columnNames.indexOf(x))))
							flag=false;
				}
				if(flag){
					deleteflag=true;
					tempPage.remove(pos);
				}
				if(deleteflag) {
					System.out.println("Index was used for deletion");
					emptyPage(path);
					if(tempPage.isEmpty()) {
						deleteFromMeta(path);
					    File file = new File(path);
					    file.delete();
					}else {
						insertHelper(tempPage,path,t);
					}
					updateIndexInsertorDelete(t);
				}
			}

			
			

		}
		ArrayList<String> allPagePaths = getAllPagePaths(strTableName);
		for(String path: allPagePaths) {
			ArrayList<Object> currPage = getPage(path);
			boolean pageFlag = false;
			for(int i=0;i<currPage.size();i++) {
				ArrayList<String> stringRow = (ArrayList<String>)currPage.get(i);
				ArrayList<Object> objRow = stringToOriginal(stringRow,t);
				boolean rowflag=true;
				 for (Map.Entry<String, Object> e : htblColNameValue.entrySet()) {
					 int colIndex = t.getIndex(e.getKey());
					 if(COMPARE(objRow.get(colIndex),e.getValue())!=0) {
						 rowflag=false;break;//doesn't match all deletion criteria
					 }						 
		         }
				 if(rowflag) {//if matches all deletion criteria
					 pageFlag=true;
					 if(!currPage.isEmpty()) {
						 currPage.remove(i);
						 i--;
					 }
					 
				 }
			}
			if(pageFlag) {
				emptyPage(path);
				if(currPage.isEmpty()) {
					deleteFromMeta(path);
				    File file = new File(path);
				    file.delete();
				}else {
					insertHelper(currPage,path,t);
				}
				updateIndexInsertorDelete(t);
			}
		}
		System.out.println("Successful Deletion Occured In Table "+t.getTableName());
	}
	


	public ArrayList<Object> getPage(String filepath) throws IOException, DBAppException {
		BufferedReader br = new BufferedReader(new FileReader(filepath));
		String line = br.readLine();
		ArrayList<Object> result = new ArrayList<Object>();
		while (line != null) {
			ArrayList<String> lineArray = new ArrayList<String>();
			String[] content = line.split(",");
			   for(int i=0;i<content.length;i++) {
				     lineArray.add(content[i]);
				   }
			   result.add(lineArray);
			   line = br.readLine();
		}
		br.close();
		return result;
	}

	public static void emptyPage(String filepath) throws IOException {
		FileWriter writer = new FileWriter(filepath);
        writer.write("");
        writer.flush();
        writer.close();
	}
		
	public static void sortPage(ArrayList<Object> list, Table t) throws ParseException, DBAppException {
		//System.out.println(list.toString());
		int pki = t.pkIndex();
	    int n = list.size();
	    for (int i = 1; i < n; ++i) {
	    	ArrayList<String> key = (ArrayList<String>)list.get(i);
	    	ArrayList<Object> keyObj = stringToOriginal(key,t);
	        int j = i-1;
	        ArrayList<String> var2 = (ArrayList<String>)list.get(j);
	        ArrayList<Object> var2Obj = stringToOriginal(var2,t);
		        while (j >= 0 && COMPARE(var2Obj.get(pki),keyObj.get(pki))>0) {
		            list.set(j + 1, var2);
		            j = j - 1;
		            if(j>=0) {
		            	var2 = (ArrayList<String>)list.get(j);
		     	        var2Obj = stringToOriginal(var2,t);
		            }
		            	
		        }
		        list.set(j + 1, key);
	    }
	}
	
	
	public void addToPageMeta(String filename, String filepath, Table t) throws IOException {
		String s = t.getTableName()+", "+"Table"+", "+filename+", "+filepath;
		String r = "Src/files.csv";
		BufferedWriter writer = new BufferedWriter(new FileWriter(r, true));
		writer.write(s);
        writer.newLine();
        writer.close(); 
		
	}
	
	public static void deleteFromMeta(String path) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader("Src/files.csv"));
		String line = br.readLine();
		ArrayList<String>temp = new ArrayList<String>();
		while (line != null) {
			String[] content = line.split(", ");
			if(!content[3].equals(path))
				temp.add(line);
			line = br.readLine();
		}
		br.close();
		emptyPage("Src/files.csv");
		BufferedWriter writer = new BufferedWriter(new FileWriter("Src/files.csv", true));
		while(!temp.isEmpty()) {
			writer.write(temp.remove(0));
	        writer.newLine();
		}
		writer.close();
	}
	
	public void updateTableMeta(String strTableName,String strColName ,String indexType) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader("Src/metadata.csv"));
		String line = br.readLine();
		ArrayList<String>temp = new ArrayList<String>();
		while (line != null) {
			String[] content = line.split(", ");
			if(content[0].equals(strTableName)&&content[1].equals(strColName)) {
				content[4]=strColName+"Index";
				content[5]=indexType;
			}
			temp.add(line);
			line = br.readLine();
		}
		br.close();
		emptyPage("Src/metadata.csv");
		BufferedWriter writer = new BufferedWriter(new FileWriter("Src/metadata.csv", true));
		while(!temp.isEmpty()) {
			writer.write(temp.remove(0));
	        writer.newLine();
		}
		writer.close();
	}
	
	
	
	public Table loadTable(String strTableName) throws NumberFormatException, DBAppException, IOException, ParseException { 
		BufferedReader br = new BufferedReader(new FileReader("Src/metadata.csv"));
		String line = br.readLine();
		ArrayList<Column> columns = new ArrayList<Column>();
		Column ctemp;
		String pk = "";
		while (line != null) {
			String[] content = line.split(", ");
			if(content[0].equals(strTableName)) {
				if(content[3].equals("true"))
					pk=content[1];
				switch(content[2]) {
				case "java.lang.Integer": ctemp = new Column(content[1],content[2],Integer.parseInt(content[6]),Integer.parseInt(content[7]));
				              columns.add(ctemp);break;
				case "java.lang.String": ctemp = new Column(content[1],content[2],content[6],content[7]);
	              columns.add(ctemp);break;
				case "java.lang.Double": ctemp = new Column(content[1],content[2],Double.parseDouble(content[6]),Double.parseDouble(content[7]));
	              columns.add(ctemp);break;
				case "java.util.Date":	
					 SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
				     Date dateMin=formatter.parse(content[6]);
				     Date dateMax=formatter.parse(content[7]);
				     ctemp = new Column(content[1],content[2],dateMin,dateMax);
		              columns.add(ctemp);break;
				}
			}
			line = br.readLine();
		}
		if(columns.isEmpty())
			throw new DBAppException("Table Not Found!");
		br.close();
		Table t = new Table(strTableName,columns,pk);
		//System.out.println("Name "+t.getTableName()+" PK "+t.getPk()+" "+" cols "+t.getColumns());
		System.out.println("Successful Load of Table "+t.getTableName());
		return t;
	}

	public void createIndex(String strTableName,String strColName ) throws DBAppException, IOException, NumberFormatException, ParseException{
		Table t = loadTable(strTableName);
		t.tableHas(strColName);
		if(checkIndexExists(strTableName,strColName))
			throw new DBAppException("Index already exists on this column");
		if(t.getPk().equals(strColName)) {
			//Column is primary key
			String filename = (strTableName+strColName).toLowerCase();
			File pkIndex = new File("Src/"+filename+".csv");
			pkIndex.createNewFile();
			addSparseToMeta(t,filename,strColName);
			ArrayList<String> pages = getAllPagePaths(strTableName);
			
			if(pages.isEmpty()) {
				System.out.println("Successful Index Creation for Table "+strTableName+" on column "+strColName);
				return;
			}
			for(String x:pages) {
				String s="";
				BufferedReader br = new BufferedReader(new FileReader(x));
				String line = br.readLine();
				int i=0;
				while (i<1) {
					String[] content = line.split(",");
					s=content[t.getIndex(strColName)]+","+x;
					i++;
				}
				br.close();
				
				BufferedWriter writer = new BufferedWriter(new FileWriter(pkIndex.getPath(), true));
				writer.write(s);
		        writer.newLine();
		        writer.close(); 
			}
		}else {//not primary key
			String filename = (strTableName+strColName).toLowerCase();
			File denseIndex = new File("Src/"+filename+"Dense"+".csv");
			denseIndex.createNewFile();
			addDenseToMeta(t,filename,strColName);
			File sparseIndex = new File("Src/"+filename+".csv");
			sparseIndex.createNewFile();
			addSparseToMeta(t,filename,strColName);
			ArrayList<String> pages = getAllPagePaths(strTableName);
			ArrayList<Object> tempAll = new ArrayList<Object>();
			ArrayList<Object> tempPage;
			if(pages.isEmpty()) {
				System.out.println("Successful Index Creation for Table "+strTableName+" on column "+strColName);
				return;
			}
			//ArrayList<String> allKeys = new ArrayList<String> ();
			for(String pagePath : pages) {
				tempPage = getPage(pagePath);
				int i=0;
				while(!tempPage.isEmpty()) {
					ArrayList<String> lineArray= (ArrayList<String>) tempPage.remove(0);
					String wantedCol =  (String) lineArray.get(t.getIndex(strColName));
					ArrayList<String> toBeAddedTemp = new ArrayList<String>();
					toBeAddedTemp.add(wantedCol);toBeAddedTemp.add(""+i);toBeAddedTemp.add(pagePath);
					tempAll.add(toBeAddedTemp);
					i++;
				}
			}
			sortOnCol(tempAll,t,strColName);
			BufferedWriter writer = new BufferedWriter(new FileWriter(denseIndex.getPath(), true));
			while(!tempAll.isEmpty()) {
				String tmpS ="";
				ArrayList<String> tmp =  (ArrayList<String>) tempAll.get(0); 
				for(int j=0;j<tmp.size();j++) {
					tmpS+=tmp.get(j)+",";
				}
				tmpS = tmpS.substring(0, tmpS.length()-1);
			     writer.write(tmpS);
			     writer.newLine();
			     tempAll.remove(0);			       		
			}
			writer.close();
			BufferedReader br = new BufferedReader(new FileReader(denseIndex));
			writer = new BufferedWriter(new FileWriter(sparseIndex.getPath(), true));
			String s="";
			String line = br.readLine();
			int i=1;
			while (line!=null) {
				String[] content = line.split(",");
				s=content[0]+","+denseIndex.getPath();
		        if(this.MaxRowsInDense%i==0) {
					writer.write(s);
			        writer.newLine();
			    }
		        line = br.readLine();
		        i++;
			}
			br.close();
			writer.close();
		}
		System.out.println("Successful Index Creation for Table "+strTableName+" on column "+strColName);	
	}
	
	public boolean checkIndexExists(String strTableName, String tableCol) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader("Src/files.csv"));
		String line = br.readLine();
		boolean flag = false;
		while (line != null) {
			String[] content = line.split(",");
			if(content[1].equals("SparseIndex")) {
				if(content[0].equals(strTableName+"_"+tableCol))
					flag = true;
			}
			line = br.readLine();
		}
		br.close();
		return flag;
	}
	
	public void addSparseToMeta(Table t,String filename, String strColName) throws IOException{
		String s = t.getTableName()+"_"+strColName+","+"SparseIndex"+","+filename+".csv"+","+"Src/"+filename+".csv";
		BufferedWriter writer = new BufferedWriter(new FileWriter("Src/files.csv", true));
		writer.write(s);
        writer.newLine();
        writer.close(); 
	}
	public void addDenseToMeta(Table t,String filename, String strColName) throws IOException{
		String s = t.getTableName()+"_"+strColName+","+"DenseIndex"+","+filename+"Dense"+".csv"+","+"Src/"+filename+"Dense"+".csv";
		BufferedWriter writer = new BufferedWriter(new FileWriter("Src/files.csv", true));
		writer.write(s);
        writer.newLine();
        writer.close(); 
	}
	public static void sortOnCol(ArrayList<Object> list, Table t, String strColName) throws ParseException, DBAppException {
		//System.out.println(list.toString());
		//int pki = t.getIndex(strColName);
	    int n = list.size();
	    for (int i = 1; i < n; ++i) {
	    	ArrayList<String> key = (ArrayList<String>)list.get(i);
	    	//ArrayList<Object> keyObj = stringToOriginal(key,t);
	        int j = i-1;
	        ArrayList<String> var2 = (ArrayList<String>)list.get(j);
	       // ArrayList<Object> var2Obj = stringToOriginal(var2,t);
	        Object k = stringToOriginal(key.get(0),t,strColName);
	        Object v = stringToOriginal(var2.get(0),t,strColName);
		        while (j >= 0 && COMPARE(v,k)>0) {///
		            list.set(j + 1, var2);
		            j = j - 1;
		            if(j>=0) {
		            	var2 = (ArrayList<String>)list.get(j);
		            	v = stringToOriginal(var2.get(0),t,strColName);
		     	        //var2Obj = stringToOriginal(var2,t);
		            }
		            	
		        }
		        list.set(j + 1, key);
	    }
	}
	
	public String getSparseFilepath(String strTableName, String strColName) throws IOException {
		String result="";
		String indexName = strTableName+"_"+strColName;
		BufferedReader br = new BufferedReader(new FileReader("Src/files.csv"));
		String line = br.readLine();
		while (line != null) {
			ArrayList<String> lineArray = new ArrayList<String>();
			String[] content = line.split(",");
			   for(int i=0;i<content.length;i++) {
				     if(content[1].equals("SparseIndex") &&content[0].equals(indexName)) {
				    	 result = content[3];
				     }
				    	 
				   }
			   line = br.readLine();
		}
		br.close();
		return result;
	}
	
	public String getDenseFilepath(String strTableName, String strColName) throws IOException {
		String result="";
		String indexName = strTableName+"_"+strColName;
		BufferedReader br = new BufferedReader(new FileReader("Src/files.csv"));
		String line = br.readLine();
		while (line != null) {
			ArrayList<String> lineArray = new ArrayList<String>();
			String[] content = line.split(",");
			   for(int i=0;i<content.length;i++) {
				     if(content[1].equals("DenseIndex") &&content[0].equals(indexName)) {
				    	 result = content[3];
				     }
				    	 
				   }
			   line = br.readLine();
		}
		br.close();
		return result;
	}
	
//	public String getPageStoredIn(Table t,ArrayList<Object> values) throws IOException, DBAppException, ParseException {
//		String filepath="";
//		 ArrayList<String> pages = getAllPagePaths(t.getTableName());
//		 for(String x:pages) {
//			 ArrayList<Object> pageValues = getPage(x);
//			 System.out.println("page: "+pageValues.toString()+"  Value "+values.toString());
//			 while(!pageValues.isEmpty()) {
//				 ArrayList<String> temp =(ArrayList<String>) pageValues.remove(0);
//				 if(temp.equals(values))
//					 return x;
//			 }
//				 
//		 }
//		 return filepath;
//	}
//	public int getPositionInPage(Table t,ArrayList<Object> values) throws IOException, DBAppException, ParseException{
//		int pos=0;
//		 ArrayList<String> pages = getAllPagePaths(t.getTableName());
//		 for(String x:pages) {
//			 int i=0;
//			 ArrayList<Object> pageValues = getPage(x);
//			System.out.println("to get pos "+pageValues.toString()+"   "+values.toString());
//			 while(!pageValues.isEmpty()) {
	
	
	public static Object stringToOriginal(String stringObj, Table t,String strColName) throws ParseException, DBAppException{
		List<Column> col = t.getColumns();
		Column colTemp = col.get(t.getIndex(strColName));
		String dataType= colTemp.getType();
		Object result = null;
			switch(dataType) {
			case "java.lang.Integer": result = Integer.parseInt(stringObj);break;
            case "java.lang.Double": result = Double.parseDouble(stringObj);break;
            case "java.lang.String": result = (stringObj);break;
            case "java.util.Date":	
            	SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
            	Date date=formatter.parse(stringObj);
            	result = (date);break;
			}
		
		return result;	
	}
	
//				 ArrayList<String> temp =(ArrayList<String>) pageValues.remove(0);
//				 if(temp.equals(values))
//					 return i;
//				 i++;
//			 }
//				 
//		 }
//		 return pos;
//	}
	
	
	public void updateIndexInsertorDelete(Table t) throws IOException, DBAppException, ParseException {
		ArrayList<String> indexedColumns = new ArrayList<String>();
		List<Column> tableColumns = t.getColumns();
		for(Column x:tableColumns) {
			if(checkIndexExists(t.getTableName(),x.getName()))
				indexedColumns.add(x.getName());
		}
		for(String x: indexedColumns) {
			if(t.getPk().equals(x)) {/// if primary key
				String indexFilepath = this.getSparseFilepath(t.getTableName(), x);
				emptyPage(indexFilepath);
				ArrayList<String> allPages = getAllPagePaths(t.getTableName());
				for(String y:allPages) {
					String s="";
					BufferedReader br = new BufferedReader(new FileReader(y));
					String line = br.readLine();
					int i=0;
					while (i<1) {
						String[] content = line.split(",");
						s=content[t.getIndex(x)]+","+y;
						i++;
					}
					br.close();
					
					BufferedWriter writer = new BufferedWriter(new FileWriter(indexFilepath, true));
					writer.write(s);
			        writer.newLine();
			        writer.close();
				}

				
			}else {/// if not primary key
				String sparseIndexFilepath = this.getSparseFilepath(t.getTableName(), x);
				String denseIndexFilepath = this.getDenseFilepath(t.getTableName(), x);
				ArrayList<String> allPages = this.getAllPagePaths(t.getTableName());
				ArrayList<Object> denseTemp = new ArrayList<Object>();
				//ArrayList<String> allKeys = new ArrayList<String> ();
				for(String pagePath:allPages) {
					ArrayList<Object> pageTemp = getPage(pagePath);
					int i=0;
					while(!pageTemp.isEmpty()) {
						ArrayList<Object> rowTemp= (ArrayList<Object>) pageTemp.remove(0);
						String wantedCol =  (String) rowTemp.get(t.getIndex(x));
						//if(!allKeys.contains(wantedCol)) {
						//allKeys.add(wantedCol);
						ArrayList<String> toBeAddedTemp = new ArrayList<String>();
						toBeAddedTemp.add(wantedCol);toBeAddedTemp.add(""+i);toBeAddedTemp.add(pagePath);
						denseTemp.add(toBeAddedTemp);
						//}
						i++;
					}
				}
				emptyPage(denseIndexFilepath);emptyPage(sparseIndexFilepath);
				sortOnCol(denseTemp, t, x);
				BufferedWriter writer = new BufferedWriter(new FileWriter(denseIndexFilepath, true));
				while(!denseTemp.isEmpty()) {
					String tmpS ="";
					ArrayList<String> tmp =  (ArrayList<String>) denseTemp.get(0); 
					for(int j=0;j<tmp.size();j++) {
						tmpS+=tmp.get(j)+",";
					}
					tmpS = tmpS.substring(0, tmpS.length()-1);
				     writer.write(tmpS);
				     writer.newLine();
				     denseTemp.remove(0);			       		
				}
				writer.close();
				
				BufferedReader br = new BufferedReader(new FileReader(denseIndexFilepath));
				writer = new BufferedWriter(new FileWriter(sparseIndexFilepath, true));
				String s="";
				String line = br.readLine();
				int i=1;
				while (line!=null) {
					String[] content = line.split(",");
					s=content[0]+","+denseIndexFilepath;
			        if(this.MaxRowsInDense/i<0||i==1) {
			        	s+=","+(i-1);
						writer.write(s);
				        writer.newLine();
				    }
			        line = br.readLine();
			        i++;
				}
				br.close();
				writer.close();
			}
		}
		if(!indexedColumns.isEmpty())
		System.out.println("Successful update of index after insertion/deletion");
	}
	
	
	
	public void updateIndexUpdate(Table t, ArrayList<String> row, String position, String path) throws IOException, DBAppException, ParseException{
		ArrayList<String> indexedColumns = new ArrayList<String>();
		List<Column> tableColumns = t.getColumns();
		for(Column x:tableColumns) {
			if(checkIndexExists(t.getTableName(),x.getName()))
				indexedColumns.add(x.getName());
		}
		for(String x: indexedColumns) {
			if(t.getPk().equals(x)) {/// if primary key
				String indexFilepath = this.getSparseFilepath(t.getTableName(), x);
				emptyPage(indexFilepath);
				ArrayList<String> allPages = getAllPagePaths(t.getTableName());
				for(String y:allPages) {
					String s="";
					BufferedReader br = new BufferedReader(new FileReader(y));
					String line = br.readLine();
					int i=0;
					while (i<1) {
						String[] content = line.split(",");
						s=content[t.getIndex(x)]+","+y;
						i++;
					}
					br.close();
					
					BufferedWriter writer = new BufferedWriter(new FileWriter(indexFilepath, true));
					writer.write(s);
			        writer.newLine();
			        writer.close();
				}

				
			}else {/// if not primary key
				String sparseIndexFilepath = this.getSparseFilepath(t.getTableName(), x);
				String denseIndexFilepath = this.getDenseFilepath(t.getTableName(), x);
				ArrayList<String> allPages = this.getAllPagePaths(t.getTableName());
				ArrayList<Object> denseTemp = getPage(denseIndexFilepath);
				
				String wantedCol =  (String) row.get(t.getIndex(x));
				ArrayList<String> toBeAddedTemp = new ArrayList<String>();
				toBeAddedTemp.add(wantedCol);toBeAddedTemp.add(position);toBeAddedTemp.add(path);
				int count=0;
				for(Object rowDense: denseTemp) {
					ArrayList<String> stringRow = (ArrayList<String>) rowDense;
					if(stringRow.get(1).equals(position)&&stringRow.get(2).equals(path)) {
						denseTemp.set(count, toBeAddedTemp);
						break;
					}
					count++;
				}
				emptyPage(denseIndexFilepath);emptyPage(sparseIndexFilepath);
				BufferedWriter writer = new BufferedWriter(new FileWriter(denseIndexFilepath, true));
				while(!denseTemp.isEmpty()) {
					String tmpS ="";
					ArrayList<String> tmp =  (ArrayList<String>) denseTemp.get(0); 
					for(int j=0;j<tmp.size();j++) {
						tmpS+=tmp.get(j)+",";
					}
					tmpS = tmpS.substring(0, tmpS.length()-1);
				     writer.write(tmpS);
				     writer.newLine();
				     denseTemp.remove(0);			       		
				}
				writer.close();
				
				BufferedReader br = new BufferedReader(new FileReader(denseIndexFilepath));
				writer = new BufferedWriter(new FileWriter(sparseIndexFilepath, true));
				String s="";
				String line = br.readLine();
				int i=1;
				while (line!=null) {
					String[] content = line.split(",");
					s=content[0]+", "+denseIndexFilepath;
			        if(this.MaxRowsInDense/i<0||i==1) {
			        	s+=","+(i-1);
						writer.write(s);
				        writer.newLine();
				    }
			        line = br.readLine();
			        i++;
				}
				br.close();
				writer.close();
			}
		}
		if(!indexedColumns.isEmpty())
		System.out.println("Successful update of index after insertion");
	}
	
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,  String[] strarrOperators) throws DBAppException, NumberFormatException, IOException, ParseException{
		ArrayList<Object> allRowResults = new ArrayList<Object>();
		ArrayList<Object> resultTemp = new ArrayList<Object>();
		ArrayList<Object> resultList = new ArrayList<Object>();
		for(SQLTerm sqlterm:arrSQLTerms) {
			//System.out.println(arrSQLTerms[0].getStrTableName()+" "+arrSQLTerms[0].getStrColumnName()+" "+arrSQLTerms[0].getStrOperator()+" "+arrSQLTerms[0].getObjValue());
			if(this.checkIndexExists(sqlterm.getStrTableName(), sqlterm.getStrColumnName())) 
				resultTemp = getRowsUsingIndex(sqlterm.getStrTableName(),sqlterm.getStrColumnName(),sqlterm.getStrOperator(),""+sqlterm.getObjValue());
				else
					resultTemp =getRows(sqlterm.getStrTableName(),sqlterm.getStrColumnName(),sqlterm.getStrOperator(),""+sqlterm.getObjValue());
			allRowResults.add(resultTemp);
		}
//		System.out.println("All row results "+allRowResults.toString());
		ArrayList<Object> temp = (ArrayList<Object>) allRowResults.remove(0);
		for(Object x:temp) {
			ArrayList<Object> tempx = (ArrayList<Object>)x;
			resultList.add(tempx);
			//allRowResults.remove(x);
		}
		for(String operator:strarrOperators) {
			switch(operator) {
			case "AND": 
				ArrayList<Object> resultListTemp = new ArrayList<Object>();
				if(!allRowResults.isEmpty()) {
					temp = (ArrayList<Object>) allRowResults.remove(0);
					for(Object x:temp) {
						ArrayList<Object> tempx = (ArrayList<Object>)x;
					if(resultList.contains(tempx))
						resultListTemp.add(tempx);
				}
				}
				resultList=resultListTemp;
				break;
			case "OR": 
				if(!allRowResults.isEmpty()) {
					temp = (ArrayList<Object>) allRowResults.remove(0);
					for(Object x:temp) {
						ArrayList<Object> tempx = (ArrayList<Object>)x;
						if(!resultList.contains(tempx))
						resultList.add(tempx);
						//allRowResults.remove(x);
					}
				}
				break;
			case "XOR": 
				if(!allRowResults.isEmpty()) {
					temp = (ArrayList<Object>) allRowResults.remove(0);
					for(Object x:temp) {
						ArrayList<Object> tempx = (ArrayList<Object>)x;
					if(resultList.contains(tempx))
						resultList.remove(tempx);
					else
						resultList.add(tempx);
				    }
				}
				break;
				default:throw new DBAppException("Operator Not "+operator +" Accepted");
			}
		}
		Iterator x = resultList.iterator();
		return x;
		
	}
	public ArrayList<Object> getRows(String strTableName, String strColName, String strOperator, String value) throws IOException, DBAppException, NumberFormatException, ParseException{
		Table t = loadTable(strTableName);
		ArrayList<String> allPages = getAllPagePaths(strTableName);
		ArrayList<Object> result = new ArrayList<Object>();
		for(String pagePath:allPages) {
			ArrayList<Object> pageTemp = getPage(pagePath);
			//System.out.println(pageTemp.toString());
			ArrayList<String> rowTemp = new ArrayList<String>();
			while(!pageTemp.isEmpty()) {
				rowTemp = (ArrayList<String>)pageTemp.remove(0);
				switch(strOperator) {
				case ">":			
					if(rowTemp.get(t.getIndex(strColName)).compareTo(value)>0) {
					//System.out.println("row "+rowTemp.get(t.getIndex(strColName))+" "+value );
					result.add(rowTemp);
				}
					break;
			    
				case ">=":
					if(rowTemp.get(t.getIndex(strColName)).compareTo(value)>=0) {
					//System.out.println("row "+rowTemp.get(t.getIndex(strColName))+" "+value );
					result.add(rowTemp);
				}
					break;
				
				case "<":
					if(rowTemp.get(t.getIndex(strColName)).compareTo(value)<0) {
					//System.out.println("row "+rowTemp.get(t.getIndex(strColName))+" "+value );
					result.add(rowTemp);
				}
					break;
					
				case "<=":
					if(rowTemp.get(t.getIndex(strColName)).compareTo(value)<=0) {
					//System.out.println("row "+rowTemp.get(t.getIndex(strColName))+" "+value );
					result.add(rowTemp);
				}
					break;
				case "!=":
					if(!rowTemp.get(t.getIndex(strColName)).equals(value)) {
					//System.out.println("row "+rowTemp.get(t.getIndex(strColName))+" "+value );
					result.add(rowTemp);
				}
					break;
				case "=":
					if(rowTemp.get(t.getIndex(strColName)).equals(value)) {
					//System.out.println("row "+rowTemp.get(t.getIndex(strColName))+" "+value );
					result.add(rowTemp);
				}
					break;
				
				default:throw new DBAppException("This operator is not supported");
				}
//				if(rowTemp.get(t.getIndex(strColName)).equals(value)) {
//					//System.out.println("row "+rowTemp.get(t.getIndex(strColName))+" "+value );
//					result.add(rowTemp);
//				}

			}
		}
		return result;
 	}
	
	public ArrayList<Object> getRowsUsingIndex(String strTableName, String strColName, String strOperator, String value) throws IOException, DBAppException, NumberFormatException, ParseException{
		System.out.println("Used index during selection");
		Table t = loadTable(strTableName);
		ArrayList<Object> result = new ArrayList<Object>();
		if(t.getPk().equals(strColName)) {
			String sparseIndexFilepath = this.getSparseFilepath(strTableName, strColName);
			ArrayList<Object> indexPage = getPage(sparseIndexFilepath);
			ArrayList<String> stringRow = new ArrayList<String>();
			String pageFilepath="";
			ArrayList<String> pagePaths = new ArrayList<String> ();
			switch(strOperator) {
			case ">":
				for(int j=0;j<indexPage.size();j++) {
					stringRow = (ArrayList<String>) indexPage.get(j);
					if(stringRow.get(0).compareTo(value)>0)
					pagePaths.add(stringRow.get(1));
				}
				break;
			case ">=":
				for(int j=0;j<indexPage.size();j++) {
					stringRow = (ArrayList<String>) indexPage.get(j);
					if(stringRow.get(0).compareTo(value)>=0)
					pagePaths.add(stringRow.get(1));
				}
				break;
			case "<":
				for(int j=0;j<indexPage.size();j++) {
					stringRow = (ArrayList<String>) indexPage.get(j);
					if(stringRow.get(0).compareTo(value)<0)
					pagePaths.add(stringRow.get(1));
				}
				break;
			case "<=":
				for(int j=0;j<indexPage.size();j++) {
					stringRow = (ArrayList<String>) indexPage.get(j);
					if(stringRow.get(0).compareTo(value)<=0)
					pagePaths.add(stringRow.get(1));
				}
				break;
			case "!=":
				for(int j=0;j<indexPage.size();j++) {
				stringRow = (ArrayList<String>) indexPage.get(j);
				pagePaths.add(stringRow.get(1));
			}
				break;
			case "=":
				for(int j=0;j<indexPage.size();j++) {
					stringRow = (ArrayList<String>) indexPage.get(j);
						if(stringRow.get(0).compareTo(value)==0) {
							pagePaths.add(stringRow.get(1));
							//pageFilepath=stringRow.get(1);
							break;
						}
						if(j+1>=indexPage.size()) {
							pagePaths.add(stringRow.get(1));
							//pageFilepath=stringRow.get(1);
							break;
						}
						ArrayList<String> Next = (ArrayList<String>) indexPage.get(j+1);
							if(stringRow.get(0).compareTo(value)<=0 && Next.get(0).compareTo(value)>=1) {
								pagePaths.add(stringRow.get(1));
								//pageFilepath=stringRow.get(1);
								break;
							}
						
				}
				break;
			default:throw new DBAppException("Opertor "+strOperator+" is not supported");
			}
//			for(int j=0;j<indexPage.size();j++) {
//				stringRow = (ArrayList<String>) indexPage.get(j);
//					if(stringRow.get(0).compareTo(value)==0) {
//						pageFilepath=stringRow.get(1);
//						break;
//					}
//					if(j+1>=indexPage.size()) {
//						pageFilepath=stringRow.get(1);
//						break;
//					}
//					ArrayList<String> Next = (ArrayList<String>) indexPage.get(j+1);
//						if(stringRow.get(0).compareTo(value)<=0 && Next.get(0).compareTo(value)>=1) {
//							pageFilepath=stringRow.get(1);
//							break;
//						}
//					
//			}
			if(stringRow.isEmpty())
				 throw new DBAppException("Error During Update Couldn't find filepath from index");
			//pageFilepath=pageFilepath.substring(1);
			int pki = t.pkIndex();
			for(String path:pagePaths) {
			ArrayList<Object> pageData =getPage(path);
				ArrayList<String> rowTemp = new ArrayList<String>();
				while(!pageData.isEmpty()) {
					rowTemp = (ArrayList<String>)pageData.remove(0);
					switch(strOperator) {
					case ">":			
						if(rowTemp.get(pki).compareTo(value)>0) {
						//System.out.println("row "+rowTemp.get(t.getIndex(strColName))+" "+value );
							result.add(rowTemp);
					}
						break;
				    
					case ">=":
						if(rowTemp.get(pki).compareTo(value)>=0) {
						//System.out.println("row "+rowTemp.get(t.getIndex(strColName))+" "+value );
							result.add(rowTemp);
					}
						break;
					
					case "<":
						if(rowTemp.get(pki).compareTo(value)<0) {
						//System.out.println("row "+rowTemp.get(t.getIndex(strColName))+" "+value );
							result.add(rowTemp);
					}
						break;
						
					case "<=":
						if(rowTemp.get(pki).compareTo(value)<=0) {
						//System.out.println("row "+rowTemp.get(t.getIndex(strColName))+" "+value );
							result.add(rowTemp);
					}
						break;
					case "!=":
						if(!rowTemp.get(pki).equals(value)) {
						//System.out.println("row "+rowTemp.get(t.getIndex(strColName))+" "+value );
							result.add(rowTemp);
					}
						break;
					case "=":
						if(rowTemp.get(pki).equals(value)) {
						//System.out.println("row "+rowTemp.get(t.getIndex(strColName))+" "+value );
							result.add(rowTemp);
					}
						break;
					
					default:throw new DBAppException("This operator is not supported");
					}
//					if(rowTemp.get(t.getIndex(strColName)).equals(value)) {
//						//System.out.println("row "+rowTemp.get(t.getIndex(strColName))+" "+value );
//						result.add(rowTemp);
//					}

				}
		}
			
		}else {
			String Densefilepath ="";
			int position=0;
			String sparseIndexFilepath = this.getSparseFilepath(t.getTableName(),strColName);
//			System.out.println(sparseIndexFilepath);
			ArrayList<Object> indexPage = getPage(sparseIndexFilepath);
//			System.out.println(indexPage.toString());
			ArrayList<String> stringRow = new ArrayList<String>();
			for(int j=0;j<indexPage.size();j++) {
				stringRow = (ArrayList<String>) indexPage.get(j);
					if(stringRow.get(0).compareTo(value)==0) {
						Densefilepath=stringRow.get(1);
						position = Integer.parseInt(stringRow.get(2));
						break;
					}
					if(j+1>=indexPage.size()) {
//						System.out.println(stringRow.toString());
						Densefilepath=stringRow.get(1);
						position = Integer.parseInt(stringRow.get(2));
						break;
					}
					ArrayList<String> Next = (ArrayList<String>) indexPage.get(j+1);
						if(stringRow.get(0).compareTo(value)<=0 && Next.get(0).compareTo(value)>=1) {
							Densefilepath=stringRow.get(1);
							position = Integer.parseInt(stringRow.get(2));
							break;
						}
			}
			////////////////In dense index:
			ArrayList<Object> densePage = (ArrayList<Object>) getPage(Densefilepath);
			List<Object >dense=densePage.subList(position, Math.min(position+this.MaxRowsInDense,densePage.size()-position));
			ArrayList<Object> toBeLookedFor = new ArrayList<Object>();
			for(int i=0;i<dense.size();i++) {
				ArrayList<String> rowTemp = (ArrayList<String>) dense.get(i);
				switch(strOperator) {
				case ">":			
					if(rowTemp.get(0).compareTo(value)>0) {
					//System.out.println("row "+rowTemp.get(t.getIndex(strColName))+" "+value );
						toBeLookedFor.add(dense.get(i));
				}
					break;
			    
				case ">=":
					if(rowTemp.get(0).compareTo(value)>=0) {
					//System.out.println("row "+rowTemp.get(t.getIndex(strColName))+" "+value );
						toBeLookedFor.add(dense.get(i));
				}
					break;
				
				case "<":
					if(rowTemp.get(0).compareTo(value)<0) {
					//System.out.println("row "+rowTemp.get(t.getIndex(strColName))+" "+value );
						toBeLookedFor.add(dense.get(i));
				}
					break;
					
				case "<=":
					if(rowTemp.get(0).compareTo(value)<=0) {
					//System.out.println("row "+rowTemp.get(t.getIndex(strColName))+" "+value );
						toBeLookedFor.add(dense.get(i));
				}
					break;
				case "!=":
					if(!rowTemp.get(0).equals(value)) {
					//System.out.println("row "+rowTemp.get(t.getIndex(strColName))+" "+value );
						toBeLookedFor.add(dense.get(i));
				}
					break;
				case "=":
					if(rowTemp.get(0).equals(value)) {
					//System.out.println("row "+rowTemp.get(t.getIndex(strColName))+" "+value );
						toBeLookedFor.add(dense.get(i));
				}
					break;
				
				default:throw new DBAppException("This operator is not supported");
				}
//				if(rowTemp.get(0).equals(value))
//					toBeLookedFor.add(dense.get(i));
			}
			for(Object toBeLookedForRow:toBeLookedFor) {
				ArrayList<String> toBeDeletedrowTemp = (ArrayList<String>)toBeLookedForRow;
				int pos = Integer.parseInt(toBeDeletedrowTemp.get(1));// NB: position of index and file path are revresed on sparse and dende
				String path = toBeDeletedrowTemp.get(2);//////////////////////////////////////////////////////////////////////
			//////get page and go to position
				ArrayList<Object> tempPage = getPage(path);
				//ArrayList<String> rowTemp = (ArrayList<String>)tempPage.get(pos);
				result.add((ArrayList<String>)tempPage.get(pos));
			}
			
			
		}
		
		return result;
	}
	private static void  insertCoursesRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader coursesTable = new BufferedReader(new FileReader("E:\\Eclipse\\Workspace\\Database2MS1\\src\\courses_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = coursesTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");


            int year = Integer.parseInt(fields[0].trim().substring(0, 4));
            int month = Integer.parseInt(fields[0].trim().substring(5, 7));
            int day = Integer.parseInt(fields[0].trim().substring(8));

            Date dateAdded = new Date(year - 1900, month - 1, day);

            row.put("date_added", dateAdded);

            row.put("course_id", fields[1]);
            row.put("course_name", fields[2]);
            row.put("hours", Integer.parseInt(fields[3]));

            dbApp.insertIntoTable("courses", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        coursesTable.close();
    }

 private static void  insertStudentRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader studentsTable = new BufferedReader(new FileReader("E:\\Eclipse\\Workspace\\Database2MS1\\src\\students_table.csv"));
        String record;
        int c = limit;
        if (limit == -1) {
            c = 1;
        }

        Hashtable<String, Object> row = new Hashtable<>();
        while ((record = studentsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("id", fields[0]);
            row.put("first_name", fields[1]);
            row.put("last_name", fields[2]);

            int year = Integer.parseInt(fields[3].trim().substring(0, 4));
            int month = Integer.parseInt(fields[3].trim().substring(5, 7));
            int day = Integer.parseInt(fields[3].trim().substring(8));

            Date dob = new Date(year - 1900, month - 1, day);
            row.put("dob", dob);

            double gpa = Double.parseDouble(fields[4].trim());

            row.put("gpa", gpa);

            dbApp.insertIntoTable("students", row);
            row.clear();
            if (limit != -1) {
                c--;
            }
        }
        studentsTable.close();
    }
 private static void insertTranscriptsRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader transcriptsTable = new BufferedReader(new FileReader("E:\\Eclipse\\Workspace\\Database2MS1\\src\\transcripts_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = transcriptsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("gpa", Double.parseDouble(fields[0].trim()));
            row.put("student_id", fields[1].trim());
            row.put("course_name", fields[2].trim());

            String date = fields[3].trim();
            int year = Integer.parseInt(date.substring(0, 4));
            int month = Integer.parseInt(date.substring(5, 7));
            int day = Integer.parseInt(date.substring(8));

            Date dateUsed = new Date(year - 1900, month - 1, day);
            row.put("date_passed", dateUsed);

            dbApp.insertIntoTable("transcripts", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        transcriptsTable.close();
    }
 private static void insertPCsRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader pcsTable = new BufferedReader(new FileReader("E:\\Eclipse\\Workspace\\Database2MS1\\src\\pcs_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = pcsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("pc_id", Integer.parseInt(fields[0].trim()));
            row.put("student_id", fields[1].trim());

            dbApp.insertIntoTable("pcs", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        pcsTable.close();
    }
 private static void createTranscriptsTable(DBApp dbApp) throws Exception {
        // Double CK
        String tableName = "transcripts";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("gpa", "java.lang.Double");
        htblColNameType.put("student_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("date_passed", "java.util.Date");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("gpa", "0.7");
        minValues.put("student_id", "43-0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("date_passed", "1990-01-01");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("gpa", "5.0");
        maxValues.put("student_id", "99-9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("date_passed", "2020-12-31");

        dbApp.createTable(tableName, "gpa", htblColNameType, minValues, maxValues);
    }

    private static void createStudentTable(DBApp dbApp) throws Exception {
        // String CK
        String tableName = "students";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("id", "java.lang.String");
        htblColNameType.put("first_name", "java.lang.String");
        htblColNameType.put("last_name", "java.lang.String");
        htblColNameType.put("dob", "java.util.Date");
        htblColNameType.put("gpa", "java.lang.Double");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("id", "43-0000");
        minValues.put("first_name", "AAAAAA");
        minValues.put("last_name", "AAAAAA");
        minValues.put("dob", "1990-01-01");
        minValues.put("gpa", "0.7");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("id", "99-9999");
        maxValues.put("first_name", "zzzzzz");
        maxValues.put("last_name", "zzzzzz");
        maxValues.put("dob", "2000-12-31");
        maxValues.put("gpa", "5.0");

        dbApp.createTable(tableName, "id", htblColNameType, minValues, maxValues);
    }
    private static void createPCsTable(DBApp dbApp) throws Exception {
        // Integer CK
        String tableName = "pcs";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("pc_id", "java.lang.Integer");
        htblColNameType.put("student_id", "java.lang.String");


        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("pc_id", "0");
        minValues.put("student_id", "43-0000");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("pc_id", "20000");
        maxValues.put("student_id", "99-9999");

        dbApp.createTable(tableName, "pc_id", htblColNameType, minValues, maxValues);
    }
    private static void createCoursesTable(DBApp dbApp) throws Exception {
        // Date CK
        String tableName = "courses";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("date_added", "java.util.Date");
        htblColNameType.put("course_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("hours", "java.lang.Integer");


        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("date_added", "1901-01-01");
        minValues.put("course_id", "0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("hours", "1");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("date_added", "2020-12-31");
        maxValues.put("course_id", "9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("hours", "24");

        dbApp.createTable(tableName, "date_added", htblColNameType, minValues, maxValues);

    }
//    public void testWrongStudentsKeyInsertion() {
//        final DBApp dbApp = new DBApp();
//        dbApp.init();
//
//        String table = "students";
//        Hashtable<String, Object> row = new Hashtable();
//        row.put("id", 123);
//        
//        row.put("first_name", "foo");
//        row.put("last_name", "bar");
//
//        Date dob = new Date(1995 - 1900, 4 - 1, 1);
//        row.put("dob", dob);
//        row.put("gpa", 1.1);
//
//        Assertions.assertThrows(DBAppException.class, () -> {
//                    dbApp.insertIntoTable(table, row);
//                }
//        );
//
//    }
//    public void testExtraTranscriptsInsertion() {
//        final DBApp dbApp = new DBApp();
//        dbApp.init();
//
//        String table = "transcripts";
//        Hashtable<String, Object> row = new Hashtable();
//        row.put("gpa", 1.5);
//        row.put("student_id", "34-9874");
//        row.put("course_name", "bar");
//        row.put("elective", true);
//
//
//        Date date_passed = new Date(2011 - 1900, 4 - 1, 1);
//        row.put("date_passed", date_passed);
//
//
//        Assertions.assertThrows(DBAppException.class, () -> {
//                    dbApp.insertIntoTable(table, row);
//                }
//        );
//    }
  
    public static void main(String[] args) throws Exception {
	      DBApp db = new DBApp();
	      db.init();

//	        SQLTerm[] arrSQLTerms;
//	        arrSQLTerms = new SQLTerm[2];
//	        arrSQLTerms[0] = new SQLTerm();
//	        arrSQLTerms[0]._strTableName = "students";
//	        arrSQLTerms[0]._strColumnName= "first_name";
//	        arrSQLTerms[0]._strOperator = "=";
//	        arrSQLTerms[0]._objValue =row.get("first_name");
//
//	        arrSQLTerms[1] = new SQLTerm();
//	        arrSQLTerms[1]._strTableName = "students";
//	        arrSQLTerms[1]._strColumnName= "gpa";
//	        arrSQLTerms[1]._strOperator = "<=";
//	        arrSQLTerms[1]._objValue = row.get("gpa");
//
//	        String[]strarrOperators = new String[1];
//	        strarrOperators[0] = "OR";
//	      String table = "students";
//
//	        row.put("first_name", "fooooo");
//	        row.put("last_name", "baaaar");
//
//	        Date dob = new Date(1992 - 1900, 9 - 1, 8);
//	        row.put("dob", dob);
//	        row.put("gpa", 1.1);
//
//	        dbApp.updateTable(table, clusteringKey, row);
//	      createCoursesTable(db);
//	      createPCsTable(db);
//	      createTranscriptsTable(db);
//	      createStudentTable(db);
//	      insertPCsRecords(db,200);
//	      insertTranscriptsRecords(db,200);
//	      insertStudentRecords(db,200);
//	      insertCoursesRecords(db,200);
	      
	      

	      
//			 Hashtable<String,Object> s1 = new Hashtable<>();
////			 s1.put("gpa", 1.9);
////			 s1.put("last_name", "Sawi");
//			 s1.put("first_name", "Hany");
////			 Date date1 = new Date("2000/11/03");
////			 s1.put("dob", date1);
////			 s1.put("id", "43-5551");
//			 db.deleteFromTable("students", s1);
			
	      
//			 db.createIndex("students","id");
//			 db.createIndex("students","first_name");
	      
			 SQLTerm[] arrSQLTerms; 
			 arrSQLTerms = new SQLTerm[3]; 
			 SQLTerm a = new SQLTerm("students","gpa",">",0.7);
			 arrSQLTerms[0]=a;
			 
			 SQLTerm b = new SQLTerm("students","id","=","43-6778");
			 arrSQLTerms[1]=b;
			 
			 SQLTerm c = new SQLTerm("students","first_name","=","EpTjWp");
			 arrSQLTerms[2]=c;
//			 
//			 SQLTerm d = new SQLTerm("Students","Name","=","Ayman");
//			 arrSQLTerms[1]=d;
//			 
//			 SQLTerm e = new SQLTerm("Students","ID","<=",9500);    
//			 arrSQLTerms[1]=e;
//			 
//			 SQLTerm f = new SQLTerm("Students","ID",">",1000);    
//			 arrSQLTerms[2]=f;
		 
			 String[]strarrOperators = new String[2]; 
			 strarrOperators[0] = "AND";
			 strarrOperators[1] = "OR";
			 
			 Iterator resultSet = db.selectFromTable(arrSQLTerms , strarrOperators);
				while (resultSet.hasNext()) {
        // Returns the next element.
        System.out.println(resultSet.next().toString());
    }
		  
	  }

//	public static void main(String[] args) throws IOException, DBAppException, ParseException {
//		 try {
//			 		 
//			 DBApp dba = new DBApp();
//			
//			 Hashtable<String, String> nt = new Hashtable<>();
//			 nt.put("ID", "java.lang.Integer");
//			 nt.put("Name", "java.lang.String");
//			 nt.put("GPA", "java.lang.Double");
//			 nt.put("DateOfBirth", "java.util.Date");
//			 Hashtable<String, String> nmin = new Hashtable<>();
//			 nmin.put("ID", "0");
//			 nmin.put("Name", "A");
//			 nmin.put("GPA", "0.7");
//			 nmin.put("DateOfBirth", "0000-01-01");
//			 Hashtable<String, String> nmax = new Hashtable<>();
//			 nmax.put("ID", "10000");
//			 nmax.put("Name", "ZZZZZZZZZZZ");
//			 nmax.put("GPA", "6.0");
//			 nmax.put("DateOfBirth", "9999-12-31");
//			 dba.createTable("Students", "ID", nt, nmin, nmax);
////			 
////			 
////			 
//			 Hashtable<String,Object> s1 = new Hashtable<>();
//			 s1.put("ID", 1234);
//			 s1.put("Name", "Sawi");
//			 s1.put("GPA", 0.9);
//			 Date date1 = new Date("2002/11/03");
//			 s1.put("DateOfBirth", date1);
//			 dba.insertIntoTable("Students", s1);
////			 
//			 dba.createIndex("Students","ID");
////			 
////			 Hashtable<String,Object> s2 = new Hashtable<>();
////			 s2.put("ID", 9639);
////			 s2.put("Name", "Ayman");
////			 s2.put("GPA", 0.9);
////			 Date date2 = new Date("2002/11/03");
////			 s2.put("DateOfBirth", date2);
////			 dba.insertIntoTable("Students", s2);
////			 
////			 Hashtable<String,Object> s3 = new Hashtable<>();
////			 s3.put("ID", 3004);
////			 s3.put("Name", "SUIII");
////			 s3.put("GPA", 0.9);
////			 Date date3 = new Date("2002/11/03");
////			 s3.put("DateOfBirth", date3);
////			 dba.insertIntoTable("Students", s3);
////			 
////			 
////			 Hashtable<String,Object> s4 = new Hashtable<>();
////			 s4.put("ID", 1002);
////			 s4.put("Name", "Dalia");
////			 s4.put("GPA", 3.9);
////			 Date date4 = new Date("2002/06/13");
////			 s4.put("DateOfBirth", date4);
////			 dba.insertIntoTable("Students", s4);
////			 
////			 Hashtable<String,Object> s5 = new Hashtable<>();
////			 s5.put("ID", 200);
////			 s5.put("Name", "Messi");
////			 s5.put("GPA", 2.9);
////			 Date date5 = new Date("2012/06/13");
////			 s5.put("DateOfBirth", date5);
////			 dba.insertIntoTable("Students", s5);
////			 
////			 Hashtable<String,Object> s6 = new Hashtable<>();
////			 s6.put("ID", 9999);
////			 s6.put("Name", "Ahmed Mohsen");
////			 s6.put("GPA", 1.9);
////			 Date date6 = new Date("2000/06/13");
////			 s6.put("DateOfBirth", date6);
////			 dba.insertIntoTable("Students", s6);
////			 
////			 Hashtable<String,Object> d1 = new Hashtable<>();
////			 d1.put("ID", 1234);
////			 dba.deleteFromTable("Students",d1);
////			 
////			 Hashtable<String,Object> d4 = new Hashtable<>();
////			 d4.put("ID", 1002);
////			 dba.deleteFromTable("Students",d4);
////
////			 Hashtable<String,Object> d2 = new Hashtable<>();
////			 Date dateDelete = new Date("2002/11/03");
////			 d2.put("DateOfBirth", dateDelete);
////			 dba.deleteFromTable("Students",d2);
////			 
//////			 Hashtable<String,Object> d2 = new Hashtable<>();
//////			 d2.put("GPA", 0.9);
//////			 dba.deleteFromTable("Students",d2);
////			 
////			 Hashtable<String,Object> d3 = new Hashtable<>();
////			 d3.put("Name", "Messi");
////			 dba.deleteFromTable("Students",d3);
////			 
////			 Hashtable<String,Object> u1 = new Hashtable<>();
////			 u1.put("Name", "Adel Shakal");
////			 dba.updateTable("Students","1234",u1);
////			
////			 Hashtable<String,Object> u1 = new Hashtable<>();
////			 u1.put("Name", "Adel Shakal");
////			 dba.updateTable("Students","3004",u1);
////			 
////			 dba.createIndex("Students","Name");
////			 
//			 SQLTerm[] arrSQLTerms; 
//			 arrSQLTerms = new SQLTerm[3]; 
//			 SQLTerm a = new SQLTerm("Students","Name","=","Sawi");
//			 arrSQLTerms[0]=a;
//			 
//			 SQLTerm b = new SQLTerm("Students","ID","!=",1234);
//			 arrSQLTerms[0]=b;
//			 
//			 SQLTerm c = new SQLTerm("Students","GPA","=",0.9);
//			 arrSQLTerms[0]=c;
//			 
//			 SQLTerm d = new SQLTerm("Students","Name","=","Ayman");
//			 arrSQLTerms[1]=d;
//			 
//			 SQLTerm e = new SQLTerm("Students","ID","<=",9500);    
//			 arrSQLTerms[1]=e;
//			 
//			 SQLTerm f = new SQLTerm("Students","ID",">",1000);    
//			 arrSQLTerms[2]=f;
//		 
//			 String[]strarrOperators = new String[2]; 
//			 strarrOperators[0] = "OR";
//			 strarrOperators[1] = "AND";
//			 
//			 Iterator resultSet = dba.selectFromTable(arrSQLTerms , strarrOperators);
//				while (resultSet.hasNext()) {
//           // Returns the next element.
//           System.out.println(resultSet.next().toString());
//       }
//
////			 ArrayList<Column> col= new ArrayList<Column>();
////			 col.add(new Column("ID", "java.lang.Integer",0,100));
////			 col.add(new Column("Name", "java.lang.String", "A", "ZZZZZZZZZZZ" ));
////			 col.add(new Column("Age", "java.lang.Integer",0,100));
////			 Table t = new Table("Strudents",col,"ID");
////			 ArrayList<Object> list= new ArrayList<Object> (); 
////			 ArrayList<String> a= new ArrayList<String> ();
////			 a.add("100");a.add("0");a.add("de7ka.com");
////			 ArrayList<String> b= new ArrayList<String> ();
////			 b.add("20");b.add("5");b.add("de7kb.com");
////			 ArrayList<String> c= new ArrayList<String> ();
////			 c.add("1");c.add("3");c.add("de7kc.com");
////			 list.add(a);list.add(b);list.add(c);
////			 System.out.println("list before sort "+list.toString());
////			 sortOnCol(list, t, "Age");
////			 System.out.println("list after sort "+list.toString());
//
//		
//		 }
//		 catch(DBAppException e){
//			 System.out.println(e.getMessage());
//		 }
//
//	}

}
