package util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import main.Manager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FileControl {
	private static final Logger LOG = LogManager.getLogger(FileControl.class);
	public static ArrayList<String> getHostList(String fileName) {
		ArrayList <String> hostList=new ArrayList<String>();
		try {

			BufferedReader in = new BufferedReader(new FileReader(fileName));
			String s;
			while ((s = in.readLine()) != null) {
				hostList.add(s);
				LOG.trace("host="+s);
			}
			in.close();

		} catch (IOException e) {
			System.err.println(e);
			System.exit(1);
		}
		return hostList;
	}

}
