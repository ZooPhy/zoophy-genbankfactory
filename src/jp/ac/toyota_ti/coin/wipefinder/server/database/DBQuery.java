	/**
	 * This wrapper around a database query provides for a generic way to 
	 * initialize a statement, execute a query, convert the
	 * results to Java objects.  Subclasses cater for 
	 * update queries and for select queries producing different types of 
	 * result.
	 * @author dw
	 *
	 */
package jp.ac.toyota_ti.coin.wipefinder.server.database;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * @author  dw
 */
public class DBQuery 
{
	private static final Logger log = Logger.getLogger("DBQuery");
	
	//according to the type of the query the statement changed, this constant enumerate them
	public static final int QT_INSERT_WITH_AUTOINCREMENT  				= 1;
	public static final int QT_UPDATE_WITH_AUTOINCREMENT  				= 2;
	public static final int QT_SELECT_ONE_ROW  		   					= 3;
	public static final int QT_SELECT_MULTIPLE_ROWS				  		= 4;
	public static final int QT_DELETE	  		   		   				= 5;
	public static final int QT_INSERT					  				= 6;
	public static final int QT_INSERT_BATCH					  			= 7;
	public static final int QT_CREATE_FUNCTION				  			= 8;
	public static final int QT_CREATE						  			= 9;
	public static final int QT_DROP							  			= 10;
	
		Connection c;
		PreparedStatement pstmt;
		ResultSet rs;
		List<Object> params;
		/**
		 * @uml.property  name="queryType"
		 */
		int queryType=-1;
	
		/**
		 * Creation of a query object requires a DBHelper which supplies the connection, 
		 * the text of a query, and a list of query parameters.
		 * @param pQueryType the type of the query SQL (define as class constants)
		 * @param query The SQL query to execute.
		 * @param params A List of Objects, each of which should at runtime be one of
		 *  
		 * Integer, Long, Date, String.
		 */
		//TODO complete the list of format of data
		public DBQuery(Connection con, int pQueryType, String query, List<Object> params) throws Exception
		{
			c = con;
			try 
			{
				if(pQueryType==DBQuery.QT_INSERT_WITH_AUTOINCREMENT||pQueryType==DBQuery.QT_UPDATE_WITH_AUTOINCREMENT)
					pstmt=c.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
				else if(pQueryType==DBQuery.QT_SELECT_ONE_ROW||pQueryType==DBQuery.QT_DELETE||pQueryType==DBQuery.QT_INSERT||pQueryType==DBQuery.QT_CREATE||pQueryType==QT_SELECT_MULTIPLE_ROWS)
					pstmt=c.prepareStatement(query);
				else
					throw new SQLException("The predifined type of query is unknown from "+this.getClass().getName());
				
				queryType = pQueryType;
			}catch (SQLException e) 
			{
				log.log(Level.SEVERE, "Impossible to prepare the statement for the query: "+e.getMessage());
				close();
				throw e;
			}catch(Exception e)
			{
				log.log(Level.SEVERE, "Other error occured when creating the query to insert:"+e.getMessage());
				close();
				throw e;
			}
			this.params=params;
		}
		/**
		 * Creation of a query object for a Batched operation, requires a DBHelper which supplies the connection, 
		 * the text of a query, the list of query parameters are added by the {@link #addBatch(List Params)}.
		 * @param pQueryType the type of the query SQL (define as class constants)
		 * @param query The SQL query to execute.
		 */
		public DBQuery(Connection con, int pQueryType, String query) throws Exception
		{
			c = con;
			try 
			{
				if(pQueryType==DBQuery.QT_INSERT_BATCH)
					pstmt=c.prepareStatement(query);//seems that Batch does NOT support getGenerateKey
				else if(pQueryType==DBQuery.QT_CREATE_FUNCTION||pQueryType==DBQuery.QT_CREATE||pQueryType==DBQuery.QT_DROP)
					pstmt=c.prepareStatement(query);
				else
				{
					log.log(Level.SEVERE, "The predifined type of query should be QT_INSERT_BATCH, QT_CREATE_FUNCTION, QT_DROP OR QT_CREATE for this constructor.");
					throw new SQLException("The predifined type of query should be QT_INSERT_BATCH for this constructor.");
				}
				queryType = pQueryType;
			}catch (SQLException e) 
			{
				log.log(Level.SEVERE, "Impossible to prepare the statement for the query: "+e.getMessage());
				close();
				throw e;
			}catch(Exception e)
			{
				log.log(Level.SEVERE, "Other error occured when creating the query to insert:"+e.getMessage());
				close();
				throw e;
			}
		}
		public void close() 
		{
			try
			{
				if(pstmt!=null)
				{
					if(rs!=null)
						rs.close();
					if(!pstmt.isClosed())
						pstmt.close();
				}
			}catch(Exception e)
			{
				log.warning("Impossible to close the ressource for the query ["+pstmt.toString()+"]: "+e.getMessage());
			}
			c = null;
		}
		
		/**
		 * Instantiate the parameters of a query, converting from the Java classes to 
		 * the corresponding SQL ones.
		 * +1 is mentioned for the key auto-incremented
		 * @throws SQLException 
		 */
		public void instantiateParams() throws SQLException 
		{
			//logger.info("Reading list of "+params.size()+" parameters.");
			for (int i=0;i<params.size();i++) 
			{
				try
				{
					Object param=params.get(i);
					//logger.info("Instantiating parameter: "+param.toString());
					//logger.info("Parameter's class is "+param.getClass().toString());
					
					if (param==null)
					{
						pstmt.setNull(i+1, java.sql.Types.NULL);
						continue;
					}
					
					Class<? extends Object> paramClass = param.getClass();		
					if (paramClass.equals(java.lang.Long.class)) {
						pstmt.setLong(i+1, ((Long) param).longValue());
							
					} else if (paramClass.equals(java.lang.Double.class)) {
						pstmt.setDouble(i+1, ((Double) param).doubleValue());

					} else if (paramClass.equals(java.lang.Float.class)) {
						pstmt.setFloat(i+1, ((Float) param).floatValue());
						
					} else if (paramClass.equals(java.lang.Integer.class)) {
						pstmt.setInt(i+1, ((Integer) param).intValue());
							
					} else if (paramClass.equals(java.lang.String.class)) {
						pstmt.setString(i+1, (String) param);
							
					} else if (paramClass.equals(java.util.Date.class)) {
						pstmt.setDate(i+1, new java.sql.Date(((java.util.Date)param).getTime()));

					}else if(paramClass.equals(java.io.FileInputStream.class)) {
						pstmt.setBinaryStream(i+1, (FileInputStream)param, ((FileInputStream)param).available());
						
					}else if(paramClass.equals(java.io.ByteArrayInputStream.class)) {
							pstmt.setBinaryStream(i+1, (ByteArrayInputStream)param, ((ByteArrayInputStream)param).available());
							
					}else if (param instanceof java.sql.Array){
						pstmt.setArray(i+1, ((Array) param));
					}else if (param instanceof java.lang.Boolean) {
						pstmt.setBoolean(i+1, (Boolean) param);
				}else 
				{
					log.log(Level.SEVERE, "Parameter's class is unexpected: "+param.getClass().toString());
					close();
					throw new SQLException("Error with the parameter received.");
				}
				}catch(SQLException e)
				{
					log.log(Level.SEVERE, "SQL Error when parsing the parameters of the query:"+e.getMessage());
					close();
					throw e;
				}catch(Exception e)
				{
					log.log(Level.SEVERE, "Unexpected error during the parsing of the parameters for a query:"+e.getMessage()+", "+e.toString());
					close();
					throw new SQLException();
				}
			}
		}
		
		/**
		 * Add the prepared Statement in the Batch
		 * @param Params the list of parameters for the query added 
		 * @throws SQLException
		 */
		public void addBatch(List<Object> Params) throws SQLException
		{
			this.params=Params;
			instantiateParams();
			try 
			{
				pstmt.addBatch();
			}catch(SQLException e){
				log.log(Level.SEVERE, "SQL error during adding operation in the batch: "+e.getMessage());
				close();
				throw e;
			}
		}
		/**
		 * Execute an existing batch and close it 
		 * @throws SQLException write the possible Exceptions raised during the execution
		 */
		public void executeBatch() throws SQLException
		{
			try
			{
				//log.info("Instantiated query:\n"+pstmt.toString());
				pstmt.executeBatch();
			}catch(BatchUpdateException be)
			{
				log.log(Level.SEVERE, "Error(s) occurres when executing the Batch: ");
				SQLException se = be.getNextException();
				log.log(Level.SEVERE, "Error: "+se.getMessage());
				throw new SQLException("Error(s) occurres when executing the Batch: "+be.getMessage()+"->"+se.getMessage());
			}catch (SQLException e) {
				log.log(Level.SEVERE, "SQL error when executing the batch: "+e.getMessage());
				throw e;
			}catch(Exception e)
			{
				log.log(Level.SEVERE, "Other exceptions occured when executing the SQL batch: "+e.getMessage());
				throw new SQLException("Other exceptions occured when executing the SQL batch: "+e.getMessage());
			}
			finally
			{
				close();
			}
		}
		/**
		 * Create a Function and store it in the DB
		 * @return The result of the SQL statement
		 */
		public boolean executeCreateFunction() throws SQLException
		{
			try
			{
				return pstmt.execute();
			} catch (SQLException e) {
				//the error message should be handle by the object which call the DBQuery
				log.warning("SQL error during deleting operation:"+e.getMessage());
				throw e;
			}
			finally
			{
				close();				
			}
		}
		/**
		 * Create a Table in the DB
		 * @return The result of the SQL statement
		 */
		public boolean executeCreate() throws SQLException
		{
			try
			{
				if(this.params!=null && !params.isEmpty())
					instantiateParams();
				return pstmt.execute();
			} catch (SQLException e) {
				log.log(Level.SEVERE, "SQL error during deleting operation:"+e.getMessage());
				throw e;
			}
			finally
			{
				close();				
			}
		}
		/**
		 * Drop a Table in the DB
		 * @return The result of the SQL statement
		 */
		public boolean executeDrop() throws SQLException
		{
			try
			{
				return pstmt.execute();
			} catch (SQLException e) {
				log.log(Level.SEVERE, "SQL error during droping operation:"+e.getMessage());
				throw e;
			}
			finally
			{
				close();				
			}
		}
		/**
		 * Insert a row with an auto-incremented key. Release the statement.
		 * @return the key of the row just inserted.
		 */
		public Object executeInsert() throws SQLException
		{
			Object result=-1L;
			instantiateParams();
			try
			{
				//log.info("Instantiated query:\n"+pstmt.toString());
				pstmt.executeUpdate();
				rs = pstmt.getGeneratedKeys();
				if (rs.next()) 
					result = rs.getObject(1);
			} catch (SQLException e) {
				//the error message should be handle by the object which call the DBQuery
				log.warning("SQL error during inserting operation:"+e.getMessage());
				throw e;
			}
			finally
			{
				close();
			}
		return result;
		}
		
		/**
		 * Delete rows according to the parameters given
		 * @return the number of rows deleted
		 */
		public int executeDelete() throws SQLException
		{
			int result=0;
			instantiateParams();
			try
			{
				//log.info("Instantiated query:\n"+pstmt.toString());
				result = pstmt.executeUpdate();
			} catch (SQLException e) {
				//the error message should be handle by the object which call the DBQuery
				log.warning("SQL error during deleting operation:"+e.getMessage());
				throw e;
			}
			finally
			{
				close();				
			}
		return result;
		}
		
		/**
		 * Select a row in the DB according to the parameters given
		 * @return the {@link ResultSet} open on the only result, the {@link PreparedStatement} has to be closed
		 */
		public ResultSet executeSelectedRow() throws SQLException
		{
			instantiateParams();
			try 
			{
				rs = pstmt.executeQuery();
				if (rs.next()) 
					return rs;
				else
					throw new SQLException("No row has been returned when one was expected...");
			}catch(SQLException e){
				//TODO set the error level when finish the implementation (to avoid annoying message)
				//log.warning("SQL error during the fetching of the unique row:"+e.getMessage());
				close();
				throw e;
			}
		}
		
		/**
		 * Select multiple rows in the DB according to the parameters given
		 * @return the result set for the query, the {@link PreparedStatement} has to be closed
		 */
		public ResultSet executeSelect_MultiRows() throws SQLException
		{
			instantiateParams();
			try 
			{
				rs = pstmt.executeQuery();
			return rs;
			}catch(SQLException e){
				log.log(Level.SEVERE, "SQL error during the select query:"+e.getMessage());
				close();
				throw e;
			}
		}
		
		/**
		 * Return the type of the query defined as class Constant
		 * @return  queryType
		 */
		public int getQueryType() {
			return queryType;
		}		
		/**
		 * Return a String representation of the query.  Primarily used for debugging
		 * to show how parameters have been instantiated.
		 * @return a string representation of the query instantiate
		 */
		public String toString() {
			return pstmt.toString();
		}
}
