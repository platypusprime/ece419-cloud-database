package app_kvServer.persistence;

import java.io.*;

public class KVPersistenceManager implements IKVPersistenceManager{

	private BufferedReader rfp;
	private FileOutputStream wfp;

	public KVPersistenceManager(){
		// filereader
		File file = new File("kv.txt");
		rfp = new BufferedReader(new FileReader(file));
		rfp.mark(file.length());
		// filewriter
		wfp = new FileOutputStream("kv.txt");
	}
	public boolean containsKey(String key);

	public String get(String key){
		String line;
		String[] items;
		while ((line = rfp.readLine()) != null) {
			items = line.trim().split(":");
			if(items[0].equals(key)){
				rfp.reset();
				return items[1];
				// TODO:return in KVMessage format??
			}
		}
	};

	public String put(String key, String value){
		// add code for updating kv pair
		wfp.write(key + ":" + value);
	};

	public void clear();

}
