
// importer les bibliotheques necessaire

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;

public class TmpMaxMin {

    // la classe MaxTemperatureMapper suivante etend la class abstrait
    // Mapper avec ces qutre type generiques  <LongWritable, Text, Text, Text>

    public static class MaxTemperatureMapper extends
            Mapper<LongWritable, Text, Text, Text> {


        @Override
        public void map(LongWritable arg0, Text Value, Context context)
                throws IOException, InterruptedException {


            // convertire la donnee de la colonne en chaine de caratere
            // et stock la dans la variable line
            String line = Value.toString();

            // si la varible line est vide
            if (!(line.length() == 0)) {

                // le nombre de caractere dans la line pour atteindre la valeur de la date

                String date = line.substring(12, 22);

                // le nombre de caractere dans la line pour atteindre la valeur de la temperature
                // je declare la variable temp_val pour recuper la valeur de temperture + ou -)
                // selon la valeur recuperer je remplie les varible (temp_Max et temp_Min)

                float temp_val = Float.parseFloat(line.substring(121,128).trim());


                // si la valeur recupere par la variable temp_val superieur a 30,donc
                // journée ensolaillé

                if (temp_val > 30.0) {

                    float temp_Max = temp_val;
                    context.write(new Text("journée ensolaillé :" + date),
                            new Text(String.valueOf(temp_Max)));
                }

                // si la valeur recupere par la variable temp_val inferieur a 15,donc
                // journée froide

                if (temp_val < 15) {

                    float temp_Min = temp_val;
                    context.write(new Text("journée froide :" + date),
                            new Text(String.valueOf(temp_Min)));
                }
            }
        }

    }


    // la classe statique MaxTemperatureReducer entend la classe abstrait Reduce qui a
    // comme parametre generique 	<Text, Text, Text, Text>

    public static class MaxTemperatureReducer extends
            Reducer<Text, Text, Text, Text> {


        public void reduce(Text Key, Iterator<Text> Values, Context context)
                throws IOException, InterruptedException {

            // ajouter toutes les valeurs dans la variable temperature
            String temperature = Values.next().toString();
            context.write(Key, new Text(temperature));
        }

    }



    // notre programme d'execution main

    public static void main(String[] args) throws Exception {

        // lire la configuration initial du cluster via le fichier XML
        Configuration conf = new Configuration();

        // initialiser le job avec la configuration par defaut du cluster
        Job job = new Job(conf, "weather example");

        // affecter le nom de notre classe
        job.setJarByClass(TmpMaxMin.class);

        // type du clé mapper
        job.setMapOutputKeyClass(Text.class);

        // type de valeur
        job.setMapOutputValueClass(Text.class);

        // defeinition de la classe Mapper(MaxTemperatureMapper)
        job.setMapperClass(MaxTemperatureMapper.class);

        //  defeinition de la classe reduce(MaxTemperatureReducer)
        job.setReducerClass(MaxTemperatureReducer.class);

        // definire le format de input
        job.setInputFormatClass(TextInputFormat.class);

        //definire le format de output
        job.setOutputFormatClass(TextOutputFormat.class);



        // recupere le chemain de l'argument args
        Path OutputPath = new Path(args[1]);


        // configurer l'enter vers le system de fichier du job
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // configurer la sortie vers le system de fichier du job
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        //supprimer le chemain apartir de hdfs
        OutputPath.getFileSystem(conf).delete(OutputPath);

        //quitter job
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

