package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RDao {
	private static final Logger LOG = LogManager.getLogger(RDao.class);
	
	private String rdbUrl;
	private String rdbUser;
	private String rdbPasswd;
	private String dbType;
	public String getRdbUrl() {
		return rdbUrl;
	}

	public void setRdbUrlNdbType(String rdbUrl) {
		this.rdbUrl = rdbUrl;
		String dbType =null;
		if (rdbUrl.startsWith("jdbc:postgresql:")){
			dbType="postgresql";
		}else if (rdbUrl.startsWith("jdbc:oracle:")) {
			dbType="oracle";
		}else{
			LOG.fatal("Can't find right JDBC. please check you config.xml");
			System.exit(0);
		}
		this.dbType=dbType;
	}

	public String getRdbUser() {
		return rdbUser;
	}

	public void setRdbUser(String rdbUser) {
		this.rdbUser = rdbUser;
	}

	public String getRdbPasswd() {
		return rdbPasswd;
	}

	public void setRdbPasswd(String rdbPasswd) {
		this.rdbPasswd = rdbPasswd;
	}
	public String getDBType() {
		return dbType;
	}

	public static void printSQLException(SQLException e) {
		while (e != null) {
			LOG.error("\n----- SQLException -----");
			LOG.error("  SQL State:  " + e.getSQLState());
			LOG.error("  Error Code: " + e.getErrorCode());
			LOG.error("  Message:    " + e.getMessage());
			e = e.getNextException();
		}
	}

	public Connection getConnection() {
		LOG.trace("DB_URL="+rdbUrl);
		Connection con = null;
		try {
			if (rdbUrl.startsWith("jdbc:postgresql:")) {
				Class.forName("org.postgresql.Driver");
			} else if (rdbUrl.startsWith("jdbc:oracle:")) {
				Class.forName("oracle.jdbc.driver.OracleDriver");
			}
		} catch (ClassNotFoundException e) {
			LOG.error("DB Driver loading error!");
			e.printStackTrace();
		}
		try {
			con = DriverManager.getConnection(rdbUrl, rdbUser, rdbPasswd);
		} catch (SQLException e) {
			LOG.error("getConn Exception)");
			e.printStackTrace();
		}
		return con;
	}

	public void disconnect(Connection conn) {
		try {
			conn.close();
		} catch (SQLException e) {
			LOG.error("disConn Exception)");
			printSQLException(e);
		}
	}

	public int getTotalHostNo(Connection con){
		Statement stmt;
		int NoOfHost=0;
		try{
			String sql = "SELECT DISTINCT MAX(HOST_NO) FROM HOST_INFOS ";
			stmt = con.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()){
				NoOfHost=rs.getInt(1);
				LOG.info("Total hosts="+NoOfHost);
			}
			rs.close();
			stmt.close();
		} catch (SQLException e){
			printSQLException(e);
		}
		return NoOfHost;
	}
	
	
	public ArrayList<String> getHostsTest(Connection conR){
		ArrayList<String> host = new ArrayList<String>();
		host.add("localhost.localdomain");
		return host;
	}
	
	
	

	public ArrayList<String> getBrokers(Connection con) {
		Statement stmt;
		ArrayList<String> hostList = new ArrayList<String>();
		try{
			String sql="SELECT DISTINCT BROKER FROM BROKER_LIST  WHERE STATUS ='up'";
			stmt = con.createStatement();
			ResultSet rs= stmt.executeQuery(sql);
			while (rs.next()){
				String host = rs.getString("BROKER");
				hostList.add(host);
			}
			rs.close();
			stmt.close();
		} catch (SQLException e){
			printSQLException(e);
		}
		return hostList;
	}

	public int getOpID(Connection con) {
		Statement stmt;
		int jobID=-1;
		try{
			String sql=null;
			if (dbType.matches("postgresql")){
				sql="SELECT nextval('jobid_seq')";
			}else if (dbType.matches("oracle")){
				sql="SELECT jobid_seq.NEXTVAL FROM dual ";
			}
			stmt = con.createStatement();
			ResultSet rs= stmt.executeQuery(sql);
			while (rs.next()){
				jobID = rs.getInt(1);
			}
			rs.close();
			stmt.close();
		} catch (SQLException e){
			printSQLException(e);
		}
		return jobID;
	}
	public int getHostGrpInfo(Connection con) {
		Statement stmt;
		int hostGrpID=-1;
		try{
			String sql=null;
			if (dbType.matches("postgresql")){
				sql="SELECT nextval('host_grp_id_seq')";
			}else if (dbType.matches("oracle")){
				sql="SELECT host_grp_id_seq.NEXTVAL FROM dual ";
			}
			stmt = con.createStatement();
			ResultSet rs= stmt.executeQuery(sql);
			while (rs.next()){
				hostGrpID = rs.getInt(1);
			}
			rs.close();
			stmt.close();
		} catch (SQLException e){
			printSQLException(e);
		}
		return hostGrpID;
	}

	public void setJobInfo(Connection con,int opID,int host_grp_id, String CMD,String executor) {
		try{
			con.setAutoCommit(false);
			String sql = "INSERT INTO JOB_RUNNING "+
					"       (JOBID,HOST_GROUP_ID,CMD,START_TIME,EXECUTOR,STATUS) "+
					"VALUES ("+opID+",'"+host_grp_id+"','"+CMD+"',CURRENT_TIMESTAMP,'"+executor+"','PENDING')";
			LOG.trace(sql);
			PreparedStatement pst = con.prepareStatement(sql);
			pst.executeUpdate();
			con.commit();
			pst.close();
		}catch (SQLException e){
			printSQLException(e);
		}
	}
	public void setSvrGrpInfo(Connection con, int hostGrpID, String host,
			String broker) {
		try{
			con.setAutoCommit(false);
			String sql = "INSERT INTO HOST_GRP "+
					"       (HOST_GRP_ID,HOSTNAME,BROKER) "+
					"VALUES ("+hostGrpID+",'"+host+"','"+broker+"')";
			LOG.trace(sql);
			PreparedStatement pst = con.prepareStatement(sql);
			pst.executeUpdate();
			con.commit();
			pst.close();
		}catch (SQLException e){
			printSQLException(e);
		}
		
	}
	
	
	
	/////////////////
	/*
	public ArrayList<String> getHostsMT(Connection con,int seq,int Total){
		Statement stmt;
		ArrayList<String> hostList = new ArrayList<String>();
		try{
			int NoOfHost=getTotalHostNo(con);
			int sliceTerm=(int) Math.ceil(NoOfHost/(Total*1.0));
			LOG.info("GAP=>"+sliceTerm);
			int sliceStart=0;
			int sliceEnd=0;
			sliceStart=sliceStart+sliceTerm*seq;
			sliceEnd=sliceStart+sliceTerm-1;
			LOG.info(seq+":"+sliceStart+"~"+sliceEnd);
			String sql="SELECT DISTINCT HOSTNAME FROM HOST_INFOS WHERE STATUS ='up' AND HOST_NO > "+sliceStart+" and HOST_NO <"+sliceEnd ;
			stmt = con.createStatement();
			ResultSet rs= stmt.executeQuery(sql);
			while (rs.next()){
				String host = rs.getString("HOSTNAME");
				LOG.info(seq+":hostname"+host);
				hostList.add(host);
			}
			rs.close();
			stmt.close();
		} catch (SQLException e){
			printSQLException(e);
		}
		return hostList;
	}
	
	public void setWorkingTimestamp(Connection conR, String dbURL,int thNo){
		PreparedStatement pst =null;
		try{
			conR.setAutoCommit(false);
			
			String sqlLastUpdateTime =null;
			
			if (dbURL.startsWith("jdbc:postgresql:")){
				sqlLastUpdateTime = "UPDATE MANAGER_SERVICE_HEALTH_CHECK SET LAST_UPDATED_TIME=NOW() WHERE SERVICE_NAME='EventCollector"+thNo+"'"; //pgsql
			}else if (dbURL.startsWith("jdbc:oracle:")) {
				sqlLastUpdateTime = "UPDATE MANAGER_SERVICE_HEALTH_CHECK SET LAST_UPDATED_TIME=SYSDATE WHERE SERVICE_NAME='EventCollector"+thNo+"'";//oracle
			}else{LOG.fatal("Can't find right JDBC. please check you config.xml");
				System.exit(0);
			}
			
			LOG.trace(sqlLastUpdateTime);
			pst = conR.prepareStatement(sqlLastUpdateTime);
			pst.executeUpdate();
			conR.commit();
			pst.close();
		}catch (SQLException sqle) {
			printSQLException(sqle);
		}catch (ArrayIndexOutOfBoundsException e){
			
		}finally {
			try {
				if (pst != null)
					pst.close();
			} catch (SQLException e) {
				printSQLException(e);
			}
			pst = null;
		}
	}
	

	public boolean insertEvent(Connection conR, String host, String allLines,String dbType) {
		PreparedStatement pst = null;
		if (allLines.length()<=0){
			return false;
		}
		try {
			conR.setAutoCommit(false);
			String[] lines=allLines.split("\n");
			for (String line:lines){
				LOG.info(line);
				int repeatCnt=0;
				int localID=-1;
				int rows=-1;
				String[] items=line.toString().split(",");
				String sql=null;
				if (dbType.matches("oracle")){
					LOG.info(items[4]);
					String[] time=items[4].split("\\.");
					LOG.info(time[0]);
					sql = "SELECT DISTINCT REPEAT_CNT,LOCAL_ID FROM HOST_EVENT WHERE HOSTNAME='"+host+"' AND "
							+ " EVENT_CODE='"+items[1]+"' AND (to_date ('"+time[0]+"','YYYY-MM-DD HH24:MI:SS')- "
							+ "LAST_EVENT_TIME < NUMTODSINTERVAL('10','MINUTE') )"; //oracle
				}else if (dbType.matches("postgresql")){
					sql = "SELECT DISTINCT REPEAT_CNT,LOCAL_ID FROM HOST_EVENT WHERE HOSTNAME='"+host+"' AND "
						+ " EVENT_CODE='"+items[1]+"' AND (timestamp '"+items[4]+"'- LAST_EVENT_TIME  < time '0:10')"; //pgsql
				}else{
					LOG.fatal("Can't find right JDBC. please check you config.xml");
					System.exit(0);
				}
				
				LOG.info("sql="+sql);
				Statement stmt = conR.createStatement();
				ResultSet rs = stmt.executeQuery(sql);
				while (rs.next()) {
					rows=rs.getRow();
					repeatCnt=rs.getInt("REPEAT_CNT");
					localID=rs.getInt("LOCAL_ID");
					LOG.info("hostnanme="+items[1]+":"+repeatCnt+":"+localID);
					
				}
				rs.close();
				stmt.close();
				LOG.info(rows);
				if (rows<=0){ // new arrival event
					//START_TIME TIMESTAMP, LAST_EVENT_TIME TIMESTAMP, REPEAT_CNT BIGINT
				 	//FIRST INTPUT
					sql = "INSERT INTO HOST_EVENT "+
							"       (HOSTNAME,ARRIVAL_TIME,LOCAL_ID,EVENT_CODE,SEVERITY,MESSAGE,START_TIME,LAST_EVENT_TIME,REPEAT_CNT) "+
							"VALUES ('"+host+"',CURRENT_TIMESTAMP,"+items[0]+",'"+items[1]+"','"+items[2]+"','"+items[3]+"','"+items[4]+"','"+items[4]+"',"+repeatCnt+")";
					LOG.trace(sql);
					pst = conR.prepareStatement(sql);
					pst.executeUpdate();
					conR.commit();
					pst.close();
				}else{
					repeatCnt++;
					sql = "UPDATE HOST_EVENT "+
							"SET LOCAL_ID="+items[0]+",MESSAGE='"+items[3]+"',LAST_EVENT_TIME='"+items[4]+"',REPEAT_CNT="+repeatCnt+" "+
						    "WHERE HOSTNAME='"+host+"' AND EVENT_CODE='"+items[1]+"' AND LOCAL_ID="+localID;
					LOG.trace(sql);
					pst = conR.prepareStatement(sql);
					pst.executeUpdate();
					conR.commit();
					pst.close();
					
				}
			}
		}catch (SQLException sqle) {
			printSQLException(sqle);
		}catch (ArrayIndexOutOfBoundsException e){
			LOG.error("IndexOut "+host+" "+allLines);
			
		}finally {
			try {
				if (pst != null)
					pst.close();
			} catch (SQLException e) {
				printSQLException(e);
			}
			pst = null;
		}
		return true;
	}
	*/
}