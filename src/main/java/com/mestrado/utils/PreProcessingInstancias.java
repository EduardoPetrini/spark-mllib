package main.java.com.mestrado.utils;



import java.io.IOException;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;


/**
 *
 * @author joao
 */
public class PreProcessingInstancias {

    /*
     *  Transformar MP e o megapixels em mp padronizado. 
     Ex16 MP em 16mp e 16.1 MP em 16.1mp
     * Converte algumas palavras para o sinônimo
     */
    public static String tratarDescription(String titulo) throws IOException {

        // transforma todas as palavras para minusculo
        titulo = titulo.toLowerCase();

        //remove acentos das palavras
        titulo = removeAcentos(titulo);

        //faz a limpeza de acordo com o vocabulário
//        titulo = replacePalavras(titulo, Projeto.vetVocabulario);
        // falta tratar isso aqui no arquivo, ainda não consegui
        titulo = titulo.replaceAll(",", " ");
        titulo = titulo.replaceAll(";", " ");

        //substitui a (/) por (-)
        titulo = titulo.replaceAll("/", "-");

        return titulo;
    }

    public static String removeAcentos(String str) {

        str = Normalizer.normalize(str, Normalizer.Form.NFD);
        str = str.replaceAll("[^\\p{ASCII}]", "");
        return str;
    }

    /**
     * Eduardo
     *
     * Uma extensão do método String.replaceAll, faz uma sequência de replaces
     * na string. A ordem de replace obedece a ordem do vetor.
     *
     * @param string - String a qual será aplicado a sequência de replaces
     * @param replaces - um vetor contendo as strings a ser subtituirdas e seu
     * valores, uma substituicao e um valor, uma substituico e um valor e assim
     * em diante, por consequência o tamnho deste vetor é par. ex: vet =
     * {replace,for,replace,for,......}
     * @return a string de entrada com todos os replaces no vetor aplicado
     */
    public String replacePalavras(String string, String[] replaces) {
        //System.out.println("size "+replaces.length);

//         if (string.contains("16.MP")){
//            System.out.println("tem ssim");
//        }
        if (replaces.length % 2 == 1) {
            throw new IllegalArgumentException("Replaces invalidos");
        }

        for (int i = 0; i < replaces.length; i += 2) {
            //System.out.println("This: \""+replaces[i]+"\" for: \""+replaces[i+1]+"\"");
            string = string.replaceAll(replaces[i], replaces[i + 1]);
//            System.out.println(string);
        }

        return string;

    }

    public static List<String> processaToken(String token, Set<String> stopWords) {

        List<String> listTokens = new ArrayList<String>();

        //faz o processamento nos tokens
        token = tratarTokens(token);

        //Verifica se o token é uma StopWords. Se for não utiliza
        if (!stopWords.contains(token.intern()) && !token.isEmpty()) {
//        if (!Projeto.hw.testaStopWords(token.intern()) && !token.isEmpty()) {

            /*
             * verifica se o arrayTokens[i] possui (/) ou (-)
             * e separar em 3 tokens por exemplo:
             * nikon-d3100 vai gerar os tokens: nikon, d3100 e nikond3100
             */
            if (token.contains("-") || token.contains("/")) {
                String[] novosTokens = token.split("-");

                for (int j = 0; j < novosTokens.length; j++) {

                    //faz o tratamento nos novos tokens
                    novosTokens[j] = tratarTokens(novosTokens[j]);

                    //Verifica se o novo token é uma StopWords. Se for não utiliza
                    if (!stopWords.contains(novosTokens[j].intern()) && !novosTokens[j].isEmpty()) {
//                    if (!Projeto.hw.testaStopWords(novosTokens[j].intern()) && !novosTokens[j].isEmpty()) {

                        // adiciona na lista de retorno de tokens
                        listTokens.add(novosTokens[j]);

                    }
                }
                token = token.replace("-", "");
                token = token.replace("/", "");
                token = tratarTokens(token);

            }

            if (!token.isEmpty()) {
                listTokens.add(token.trim());
            }

        }

        return listTokens;
    }

    /*
     * Processamento dos tokens
     */
    public static String tratarTokens(String token) {

        token = token.trim();

        token = token.replaceAll("\\+", " ");

        //troca parenteses por espaco em branco
        token = token.replaceAll("\\)", " ");
        token = token.replaceAll("\\(", " ");
        token = token.replaceAll("\\+", " ");

        // $ Elimina sinais de pontuacao no final dos tokens
        token = token.replaceAll(",+$", " ");
        token = token.replaceAll("\\.+$", " ");
        token = token.replaceAll(";+$", " ");
        token = token.replaceAll(":+$", " ");
        token = token.replaceAll("!+$", " ");
        token = token.replaceAll("\\?+$", " ");
        token = token.replaceAll("\\)$", " ");
        token = token.replaceAll("\\*", " ");
        token = token.replaceAll("/+", " ");
        token = token.replaceAll("\\[$", "");
        token = token.replaceAll("\\]$", "");

        // elimina a barra no final do tokens
        token = token.replaceAll("\\\\$", "");

        // ^ define que vai remover tais caracteres no início da string
        token = token.replaceAll("^\\[", "");
        token = token.replaceAll("^\\]", "");

        token = token.replaceAll("\\]", " ");
        token = token.replaceAll("\\[", " ");

        token = token.replaceAll("^[(]", "");
        token = token.replaceAll("^-", "");
        token = token.replaceAll("^'", "");

        //substitui 1 ou mais espaços em branco por apenas 1 espaço.
        token = token.replaceAll(" +", " ");
        token = token.trim();

        // elimina ? no meio do token
        token = token.replace("?", " ");

        //converte os tokens para minusculo
        token = token.toLowerCase();

        //verifica se após dividir em tokens se não ficou apenas letras ou apenas números
        if (soContem1Caracter(token)) {
            token = "";
            return token;
        }

        return token.trim();
    }

    public static boolean soContemNumeros(String texto) {
        if (texto == null) {
            return false;
        }
        for (char letra : texto.toCharArray()) {
            if (letra < '0' || letra > '9') {
                return false;
            }
        }
        return true;

    }

    public static boolean soContem1Caracter(String texto) {

        if (texto.length() == 1) {
            return true;

        } else {
            return false;

        }

    }
}
