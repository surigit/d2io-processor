/**
 * 
 */
package com.suren.mbr.d2io.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import com.suren.mbr.d2io.intf.ProducerDatasource;
import com.suren.mbr.d2io.intf.RecordData;
import com.suren.mbr.d2io.intf.impl.InputRecordData;

/**
 * @author Surendra Myneni
 *
 */
public class LoadSampleOracleDatasource implements ProducerDatasource {

	private String url = null;
	private String driver = null;
	private String username = null;
	private String password = null;
	private Connection conn = null;
	private Statement stmt = null;
	private String sql = "select content_id,business_id,content_value from citi_cm_content";
	//private String sql = "select business_id,business_name,iso_country from JFP_BUSINESS";
	ResultSet rs = null;
	
	/* (non-Javadoc)
	 * @see com.suren.mbr.d2io.intf.ProducerDatasource#init(java.util.Map)
	 */
	@Override
	public void init(Map<String, String> configMap) throws Exception {

		this.url = configMap.get("url");
		this.driver = configMap.get("driver");
		this.username = configMap.get("username");
		this.password = configMap.get("password");
	
		System.out.println(configMap);
		
		conn = getConnection();
		stmt = conn.createStatement();
		rs = stmt.executeQuery(sql);
		
	}

	private Connection getConnection() throws Exception{
		try {
			   Class.forName(driver);
			}
			catch(ClassNotFoundException ex) {
			   System.out.println("Error: unable to load driver class!");
			   System.exit(1);
			}
		
		return DriverManager.getConnection(url,username,password);
	}
	
	
	/* (non-Javadoc)
	 * @see com.suren.mbr.d2io.intf.ProducerDatasource#hasNext()
	 */
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		try {
			return rs.next();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return false;
	}

	/* (non-Javadoc)
	 * @see com.suren.mbr.d2io.intf.ProducerDatasource#next()
	 */
	@Override
	public RecordData next() {
		// TODO Auto-generated method stub
		try {
			Object obj = rs.getObject(3);
			//System.out.println("OBJ ="+obj);
			return  new InputRecordData(obj, new Integer(obj.hashCode()).toString());
		} catch (SQLException e) {
		}
		
		return null;
	}

	/* (non-Javadoc)
	 * @see com.suren.mbr.d2io.intf.ProducerDatasource#close()
	 */
	@Override
	public void close() {

			try {
				if(rs!= null){
					rs.close();
					System.out.println("Result Set Closed");
				}
				if(stmt!= null){
					stmt.close();
					System.out.println("Statement Closed");
				}
				if(conn!= null){
					conn.close();
					System.out.println("Conn Closed");
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		
	}

}
