package main.java.com.mestrado.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import main.scala.master.spark.main.MainSpark;

public class MrUtils {
	public static void mergeOutputFiles(String inDir, String outFile) {
		Path input = new Path(inDir);
		Path outputFile = new Path(outFile);
		Path tmp = new Path(inDir.substring(0, inDir.length() - 1) + "-tmp");
		Configuration c = new Configuration();
		c.set("fs.defaultFS", MainSpark.clusterUrl());
		
		//Copia os arquivos do input para um diretório temporário
		try {
			FileSystem fs = FileSystem.get(c);
			checkDir(input,fs);
			copyAllDirFilesToTmpDir(input, tmp, fs);
			
			checkDir(tmp,fs);
			
			//Exclui o diretório de input
			if (fs.exists(input)) {
				fs.delete(input, true);
			}
			
			if (FileUtil.copyMerge(fs, tmp, fs, outputFile, false, c, null)) {
				System.out.println("Arquivos copiados com sucesso!");
				fs.delete(tmp, true);
			} else {
				System.out.println("Erro ao copiar os arquivos");
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void checkDir(Path input, FileSystem fs) {
		System.out.println("\n\n**********************************CHECK FILE**************************************\n");
		try {
			if(fs.exists(input)){
				System.out.println("Arquivo \'"+input.getName()+"\' existe!");
				System.out.println("Arquivo dentro do diretório ("+fs.listStatus(input).length+"): ");
				for(FileStatus f:fs.listStatus(input)){
					System.out.println(f.getPath().getName()+", size: "+f.getLen());
				}
			}else{
				System.out.println("Arquivo \'"+input.getName()+"\' NÃO existe!");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("\n\n**********************************CHECK END**************************************\n");
	}

	public static void copyAllDirFilesToTmpDir(Path origem, Path destino, FileSystem fs) {
		try {
			if(fs.exists(destino) || fs.isDirectory(destino) || fs.isFile(destino)){
				fs.delete(destino, true);
			}
			fs.rename(origem, destino);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param data
	 * @param fileName
	 */
	public static void saveFileInLocal(String data, String fileName) {
		File file = new File(fileName);

		FileWriter fw;
		BufferedWriter bw;
		try {
			fw = new FileWriter(file.getAbsoluteFile(), true);
			bw = new BufferedWriter(fw);
			bw.write(data);
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
