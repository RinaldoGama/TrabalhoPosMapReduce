package analisafilmes.reducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @autor Rinaldo Gama - Pós Graduação
 * Trabalho Tecnologias Avancadas - MapReduce em Java
 *
 */

public class ContaFilmesAnoReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

	@Override
	protected void reduce(Text key,
						Iterable<IntWritable> values,
						Context context) throws IOException,InterruptedException{
		int sum = 0; 
		
		Iterator<IntWritable> valuesIt = values.iterator(); //iterator sobre os valores recebidos
		while (valuesIt.hasNext()) {
			sum += valuesIt.next().get(); //Faz a soma dos valores recebidos na chave key
		}
		context.write(key, new IntWritable(sum) ); //escreve a chave e a soma de volta no contexto		
	}	
}