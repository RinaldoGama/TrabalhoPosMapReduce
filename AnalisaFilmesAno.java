package analisafilmes.drivers;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import analisafilmes.mappers.AnalisaFilmesAnoMapper; //Adiciona o Package para chamada Posterior
import analisafilmes.reducers.ContaFilmesAnoReducer;

/**
 * 
 * @autor Rinaldo Gama - Pós Graduação
 * Trabalho Tecnologias Avancadas - MapReduce em Java
 *
 */


public class AnalisaFilmesAno extends Configured implements Tool {
	
	public static void main(String[] args ) throws Exception{
		int exitCode = ToolRunner.run(new AnalisaFilmesAno(), args ); //faz o chamado no executor do job
		System.exit(exitCode);
	}

	public int run(String[] arg0) throws Exception {
		if (arg0.length != 2) { //faz a validação se os dois parametros de entrada foram passados corretamente <input> <output>
			System.err.printf("Uso Correto: %s [generic options] <input> <output>", getClass().getSimpleName() );
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}		
		Job job = new Job(); //instancia o job
		job.setJarByClass(AnalisaFilmesAno.class); //informa qual a  classe responsavel pelo job
		job.setJobName("Analise Filme Ano"); //adiciona o nome do job 
		
		FileInputFormat.addInputPath(job, new Path(arg0[0]));   //define o caminho do arquivo de entrada
		FileOutputFormat.setOutputPath(job, new Path(arg0[1])); //define o caminho do arquivo de saida
		
		job.setOutputKeyClass(Text.class); // informa qual será o tipo de dado da chave no arquivo de saida
		job.setOutputValueClass(IntWritable.class); //informa qual será o tipo de dado do valor de saida
		job.setOutputFormatClass(TextOutputFormat.class); // informa qual será o formato da classe de saida
		
		job.setMapperClass(AnalisaFilmesAnoMapper.class); //informa qual será a classe mapper 
		job.setReducerClass(ContaFilmesAnoReducer.class); //informa qual será a classe reducer
		
		int returnValue = job.waitForCompletion(true) ? 0 : 1;  
		System.out.println("Job.Sucesso? " + job.isSuccessful() ); //Imprime msg caso Job for um Sucesso
		return returnValue;		
	}
}
