package main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import util.Conf;
import util.FileControl;
import util.RDao;


public class Manager {
	private static final Logger LOG = LogManager.getLogger(Manager.class);
	public static final String RUNMESSAGE = null;
	public static String VERSION = "0.1";
	public static long timeLimit=0;

	// private static final String HOST = "127.0.0.1";
	private static int PORT = 38478;
	//public static int MESSAGE_SIZE = 256;

	
	@SuppressWarnings("null")
	public static void main(String[] args) {
		
		//CREATE TABLE BROKER_LIST (BROKER_NO BIGINT,BROKER VARCHAR(100),STATUS VARCHAR(100))
		//INSERT INTO BROKER_LIST VALUES(0,'localhost.localdomain','up')
		//CREATE TABLE HOST_GRP (HOST_GRP_ID BIGINT, BROKER VARCHAR(1000),HOSTNAME VARCHAR(1000))
		//CREATE TABLE JOB_RUNNING (JOBID BIGINT,HOST_GROUP_ID BIGINT,CMD VARCHAR (4000),START_TIME TIMESTAMP, EXECUTOR VARCHAR(100), STATUS VARCHAR(20)) 
		//CREATE TABLE JOB_HIST (JOBID BIGINT,HOST_GROUP_ID BIGINT,CMD VARCHAR (4000),START_TIME TIMESTAMP,HIST_TIME TIMESTAMP, EXECUTOR VARCHAR(100), STATUS VARCHAR(20)) 
		//CREATE TABLE JOB_RESULT (JOBID BIGINT, HOSTNAME VARCHAR(1000), RESULT VARCHAR(4000),STATUS VARCHAR(20),START_TIME TIMESTAMP,END_TIME TIMESTAMP)
		//DROP TABLE BROKER_LIST
		//DROP TABLE HOST_GRP
		//DROP TABLE JOB_RUNNING
		//DROP TABLE JOB_HIST
		//DROP TABLE JOB_RESULT 

		//CREATE SEQUENCE HOST_GRP_ID_SEQ;
		//CREATE SEQUENCE JOB_ID_SEQ;
		//select nextval('host_grp_id_seq');
		//select setval('host_grp_id_seq',1,false);               

		Manager mgr=new Manager();
		
		ArrayList<String> hosts=FileControl.getHostList(args[2]);
		
		Conf cf = new Conf();
		cf.setConfFile(args[0]);
		String CMD=args[1];
		if (args.length>=5){
			if(args[3].matches("-limitsec")){
				try{
					timeLimit=Long.parseLong(args[4]);
					if (timeLimit <2){
						System.out.println("running_time_limit:"+timeLimit+" sec");
					}else{
						System.out.println("running_time_limit:"+timeLimit+" secs");
					}
				}catch(NumberFormatException e){
					LOG.error("Time Limit value:"+args[4]+" must be number");
					System.exit(0);
				}
			}
		}

		PORT = cf.getSinglefValue("port");
		
		RDao rDao=new RDao();
		rDao.setRdbPasswd(cf.getSingleString("password"));
		rDao.setRdbUrlNdbType(cf.getDbURL());
		rDao.setRdbUser(cf.getSingleString("user"));
		Connection con=rDao.getConnection();
		
		
		ArrayList <String> brokers= rDao.getBrokers(con);
		String[] arrBroker = new String[cf.getSinglefValue("max_agent")];
		int i=0;
		for (String broker:brokers){
			LOG.trace("broker="+broker);
			arrBroker[i]=broker;
			i++;
		}
		int modBroker=brokers.size();
		LOG.trace(modBroker);
		

		int opID=rDao.getOpID(con);
		int hostGrpID=rDao.getHostGrpInfo(con);
		System.out.println("JOB_ID <"+opID+"> is submitted");
		LOG.info("JOB_ID <"+opID+"> is submitted");


		String userName=System.getProperty("user.name");


		rDao.setJobInfo(con,opID,hostGrpID,CMD,userName);
		i=0;
		int rNum;
		for (String host:hosts){
			rNum=i%modBroker;
			rDao.setSvrGrpInfo(con,hostGrpID,host,arrBroker[rNum]);
			LOG.trace("setBroker="+hostGrpID+","+arrBroker[rNum]+"->"+host);
			i++;
		}
		
		try {
			con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		//TODO send message password+JobID+Mode(lock,force,norm)+cmd)
		
		//String runMessage=cf.getSingleString("cmd_auth")+"::norm::run::"+opID;
		
		Socket socket = null;
		String ip = "localhost";
		BufferedReader insert = null;  
		BufferedReader br = null;
		InputStream is = null;
		OutputStream os = null;
		PrintWriter pw = null;
		String sendMsg = cf.getSingleString("cmd_auth")+"::norm::run::"+opID+"::"+timeLimit+"::";
		String revMsg = null; 
		int rprtCnt=cf.getSinglefValue("retry_cnt");
		for (String broker:brokers){
			int tryCnt=0;
			while (revMsg==null || !sendMsg.matches(revMsg )){
				try {
					socket = new Socket(broker,PORT);
					os = socket.getOutputStream();
					is = socket.getInputStream();
					pw = new PrintWriter(new OutputStreamWriter(os));
					br = new BufferedReader(new InputStreamReader(is)); 
					pw.println(sendMsg);
					pw.flush();
					revMsg = br.readLine();
					LOG.trace("rev=" + revMsg);
					if (revMsg== null){
						LOG.error("Broker:"+broker+" encount Error");
						break;
					}
					if(sendMsg.matches(revMsg)){
						LOG.info("Sending complete to "+broker);
					}
					pw.close();
					br.close();
					socket.close();
				}catch (ConnectException e){
					LOG.error("Connection Error to "+broker);
					tryCnt++;
					if (rprtCnt < tryCnt){
						LOG.error("connection re-try count:"+tryCnt+" exceed to "+broker);
						break;
					}
					try{
						Thread.sleep(cf.getSinglefValue("retry_interval_sec")*1000);
					}catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				}catch (SocketException e){
					LOG.error("SocketException to "+broker);
					tryCnt++;
					if (rprtCnt < tryCnt){
						LOG.error("connection re-try count:"+tryCnt+" exceed to "+broker);
						break;
					}
					try{
						Thread.sleep(cf.getSinglefValue("retry_interval_sec")*1000);
					}catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				}catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}