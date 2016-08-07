package main.java.com.mestrado.utils;

import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.spark.mllib.regression.LabeledPoint;

import main.scala.master.spark.main.MainSpark;


public class EvaluationPrediction {

	private static HashMap<String, String> predictedHash = null;
	public static HashMap<String, Integer> realClassCount = null;

	public static void startEvaluation(ArrayList<Double> predictedArray, ArrayList<Double> originalArray) {
		predictedHash = new HashMap<String, String>();
		realClassCount = new HashMap<String, Integer>();

		/* Create hash for predicted instances */

		/* Read original test file, line by line */
		HashMap<String, Integer> realAndPredictedClass = new HashMap<>();// Real
																			// class
		HashMap<String, Integer> predictedClassCount = new HashMap<>();// Predicted
		createHashForPredictedText(predictedArray, predictedClassCount);
																	// class
//		HashMap<String, Double> precision = new HashMap<>();
//		HashMap<String, Double> recall = new HashMap<>();
//		HashMap<String, Double> accuracy = new HashMap<>();
//		HashMap<String, Double> fMensurement = new HashMap<>();

		int[] eqDif = origialTestFileComparationText(originalArray, realAndPredictedClass);
		int equals = eqDif[0];
		int different = eqDif[1];

//		Integer NT = 0; // Nt
		Double somaOrigAndPred = 0.0;
		Double somaPred = 0.0;
		Double somaOrig = 0.0;
		Double somaFmen = 0.0;
		double somaP = 0;
		double somaR = 0;

//		for (String classID : realAndPredictedClass.keySet()) {
//			NT += documentQtyClass.get(classID);
//		}

		StringBuilder sb = new StringBuilder();
		sb.append(LocalDateTime.now()).append("\n");
//		sb.append("Classe\tPrecision\tRecall\t\tF\n");
		DecimalFormat format = new DecimalFormat("##0.00");
		for (String classID : realClassCount.keySet()) {
			
			double origAndPred = realAndPredictedClass.get(classID) == null ? 0 :realAndPredictedClass.get(classID); // nf,t
			double pred = predictedClassCount.get(classID) == null ? 0 :predictedClassCount.get(classID); // nf
			double orig = realClassCount.get(classID); // nt

			Double p = origAndPred / pred;
			Double r = origAndPred / orig;
//			Double acc = (origAndPred + (NT - pred - orig + origAndPred)) / Double.valueOf(NT);
			Double fmen = origAndPred == 0 ? 0 : (2 * p * r) / (p + r);

//			precision.put(classID, p);
//			recall.put(classID, r);
//			accuracy.put(classID, acc);
//			fMensurement.put(classID, fmen);

			somaOrigAndPred += origAndPred;
			somaPred += pred;
			somaOrig += orig;
			somaFmen += fmen;
			somaP += pred == 0? 0 :p;
			somaR += r;
//			sb.append(classID).append(":\t").append(format.format(p)).append("\t\t").append(format.format(r)).append("\t\t")
//					.append(format.format(fmen)).append("\n");
		}

		// StringBuilder sbPrecisao = new StringBuilder();
		//
		// for (String p : precision.keySet()) {
		// sbPrecisao.append(p + " " + precision.get(p) + "\n");
		// }
		// System.out.println("Precisao");
		// System.out.println(sbPrecisao);
		//
		// StringBuilder sbRevocacao = new StringBuilder();
		//
		// for (String p : recall.keySet()) {
		// sbRevocacao.append(p + " " + recall.get(p) + "\n");
		// }
		// System.out.println("Revocacao");
		// System.out.println(sbRevocacao);
		//
		// StringBuilder sbAcuracy = new StringBuilder();
		//
		// for (String p : accuracy.keySet()) {
		// sbAcuracy.append(p + " " + accuracy.get(p) + "\n");
		// }
		// System.out.println("Acuracy");
		// System.out.println(sbAcuracy);
		//
		// StringBuilder sbMensurement = new StringBuilder();
		//
		// Double macro = 0.0;
		// for (String p : fMensurement.keySet()) {
		// sbMensurement.append(p + " " + fMensurement.get(p) + "\n");
		// macro += fMensurement.get(p);
		// }
		//
		// macro = macro / fMensurement.keySet().size();
		// System.out.println(macro);
		// System.out.println("Fmensurement");
		// System.out.println(sbMensurement);

		Double pMicro = somaOrigAndPred / somaPred;
		Double rMicro = somaOrigAndPred / somaOrig;
		Double micro = (2 * pMicro * rMicro) / (pMicro + rMicro);
		double avePrecision = (somaP / realClassCount.size());
		double aveRecall = (somaR / realClassCount.size());
		double macro = (somaFmen / realClassCount.size());
		sb.append("\nEquals:\t\t"+equals);
		sb.append("\nDifferent:\t\t"+different);
		sb.append("\nPrecision:\t\t" + avePrecision).append("\n");
		sb.append("Recall:\t\t" + aveRecall).append("\n");
		sb.append("Macro:\t\t" + macro).append("\n");
		sb.append("Micro:\t\t" + micro).append("\n");
		sb.append("Precistion\tRecall\tMacroF1\tMicroF1\n");
		sb.append(format.format(avePrecision*100)).append("%\t")
		.append(format.format(aveRecall*100)).append("%\t")
		.append(format.format(macro*100)).append("%\t")
		.append(format.format(micro*100)).append("%\t\n\n");
		System.out.println(sb.toString());
		MrUtils.saveFileInLocal(sb.toString(), MainSpark.evaluationFile());
	}

	private static int[] origialTestFileComparationText(ArrayList<Double> originalArray, HashMap<String, Integer> realAndPredictedClass) {
		String ofertaId;
		String classeOriginal;
		String classePredicted;
		int equals = 0;
		int different = 0;
		
		for(int i = 0; i < originalArray.size(); i++){
			ofertaId = Integer.toString(i);
			classeOriginal = Double.toString(originalArray.get(i));
			classePredicted = predictedHash.get(ofertaId);
			
			if (classeOriginal.equals(classePredicted)) {
				equals++;
				if (realAndPredictedClass.containsKey(classeOriginal)) {
					realAndPredictedClass.put(classeOriginal, realAndPredictedClass.get(classeOriginal) + 1);
				} else {
					realAndPredictedClass.put(classeOriginal, 1);
				}
			} else {
				different++;
			}

			/* Create total hash class count */
			if (realClassCount.containsKey(classeOriginal)) {
				realClassCount.put(classeOriginal, realClassCount.get(classeOriginal) + 1);
			} else {
				realClassCount.put(classeOriginal, 1);
			}
		}
		return new int[] { equals, different };
	}

	private static void createHashForPredictedText(ArrayList<Double> predictedArray, HashMap<String, Integer> predictedClassCount) {
		String classe;
		String ofertaId;
		
		for (int i = 0; i < predictedArray.size(); i++){
			ofertaId = Integer.toString(i);
			classe = Double.toString(predictedArray.get(i));
			if (predictedHash.put(ofertaId, classe) != null) {
				System.out.println("It's not possible!!!");
			}
			if (predictedClassCount.containsKey(classe)) {
				predictedClassCount.put(classe, predictedClassCount.get(classe) + 1);
			} else {
				predictedClassCount.put(classe, 1);
			}
		}
	}
}
