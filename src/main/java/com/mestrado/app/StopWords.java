package main.java.com.mestrado.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class StopWords implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Set<String> stopWords;

	public StopWords() {
		super();
		this.stopWords = new HashSet<String>();
	}

	public static void stopWordFileToObject(String filePath) {
		File file = new File(filePath);
		StopWords sw = new StopWords();
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String linha;
			while ((linha = br.readLine()) != null) {
				sw.stopWords.add(linha.trim());
			}
			saveObject(sw);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	// Salva o objeto em um arquivo localmente
	private static void saveObject(StopWords sw) throws IOException {
		FileOutputStream fout = new FileOutputStream("stopwords.ser");
		ObjectOutputStream oos = new ObjectOutputStream(fout);
		oos.writeObject(sw);
	}

	// Le o objeto no HDFS
	public static Set<String> readObject(String filePath, Configuration c) {
		// http://stackoverflow.com/questions/11491542/how-to-read-a-serialized-object-from-a-file-in-hdfs-in-hadoop
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			InputStream in = fs.open(new Path(filePath));
			ObjectInputStream objReader = new ObjectInputStream(in);
			StopWords sw = (StopWords) objReader.readObject();
			return sw.stopWords;
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}

	public Set<String> getStopWords() {
		return stopWords;
	}

	public void setStopWords(Set<String> stopWords) {
		this.stopWords = stopWords;
	}

}
