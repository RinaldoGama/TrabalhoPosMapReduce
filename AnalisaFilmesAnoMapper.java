package analisafilmes.mappers;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @autor Rinaldo Gama - P�s Gradua��o
 * Trabalho Tecnologias Avancadas - MapReduce em Java
 *
 */

public class AnalisaFilmesAnoMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
	
	private final static IntWritable ONE = new IntWritable( 1 ); //Define uma Constante num�rica
	
	@Override
	protected void map(LongWritable key , Text value , Context context) throws IOException, InterruptedException{
		String line = value.toString(); 
		StringTokenizer token = new StringTokenizer(line,"::"); //faz o divis�o da linha pelo caracter ::
		
		String movieId = token.nextToken(); //(primeiro campo) - c�digo do filme 
		String title = token.nextToken(); //(segundo campo) - titulo do filme 
						
		if ( (!title.isEmpty()) & (title.length() >= 6) ){
			String year = title.substring( title.length()-5 , title.length()-1 ); //separa o ano do filme a partir do titurlo
			
			try {
				int movieYear = Integer.parseInt(year); //analisa se o ano obtido  a convers�o � possivel para inteiro
				context.write(new Text(year), ONE ); //escreve o ano e o numero 1 no contexto intermediario do MapReduce	
				
			}catch (Exception e ) {
				System.out.printf("Erro ao mapear input do filme %s - texto: %s" , movieId,title);			
			}
		}
	}	
}
