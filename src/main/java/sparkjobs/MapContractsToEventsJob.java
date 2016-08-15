package sparkjobs;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;

import org.actus.conversion.DateConverter;

import javax.time.calendar.ZonedDateTime;
import scala.Tuple2;
import java.util.Map;

public class MapContractsToEventsJob {

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Please provide the input file full path as argument");
      System.exit(0);
    }

    // for time stopping
    long start = System.currentTimeMillis();
    
    SparkConf conf = new SparkConf().setAppName("sparkjobs.MapContractsToEventsJob").setMaster("local");
    JavaSparkContext sparkContext = new JavaSparkContext(conf);
	 SQLContext sqlContext = new SQLContext(sparkContext);
    
    // import and broadcast analysis date
    JavaRDD<String> timeSpecs = sparkContext.textFile(args[2]); // analysis time specification
    String[] timeVector = timeSpecs.toArray().get(0).split(";");
    ZonedDateTime _t0 = null;
    try{
      _t0 = DateConverter.of(timeVector[0]);
    } catch(Exception e) {
      System.out.println(e.getClass().getName() + " when converting the analysis date to ZonedDateTime!");
    }
    Broadcast<ZonedDateTime> t0 = sparkContext.broadcast(_t0);
    
    // import risk factor data, map to connector and broadcast
    JavaRDD<String> riskFactorFile = sparkContext.textFile(args[1]); // contract data
    JavaPairRDD<String, String[]> riskFactorRDD = riskFactorFile.mapToPair(
      new PairFunction<String, String, String[]>() {
        public Tuple2<String, String[]> call(String s) {
          String[] temp = s.split(";");
          return new Tuple2(temp[0], temp);
        }
      });    
    Broadcast<Map<String,String[]>> riskFactors = sparkContext.broadcast(riskFactorRDD.collectAsMap());
    
	// import and map contract data to contract event results
    JavaRDD<String> contractFile = sparkContext.textFile(args[0],4); // contract data
    JavaRDD<Row> events = contractFile.map(new ContractToEventsFunction(t0, riskFactors));
    
    // convert to DataFrame
    StructType eventsSchema = DataTypes
            .createStructType(new StructField[] {
                DataTypes.createStructField("id", DataTypes.createArrayType(DataTypes.StringType), false),
                DataTypes.createStructField("date", DataTypes.createArrayType(DataTypes.StringType), false),
                DataTypes.createStructField("type", DataTypes.createArrayType(DataTypes.StringType), false),
                DataTypes.createStructField("currency", DataTypes.createArrayType(DataTypes.StringType), false),
                DataTypes.createStructField("value", DataTypes.createArrayType(DataTypes.DoubleType), false),
                DataTypes.createStructField("nominal", DataTypes.createArrayType(DataTypes.DoubleType), false),
                DataTypes.createStructField("accrued", DataTypes.createArrayType(DataTypes.DoubleType), false)});
 	 DataFrame results = sqlContext.createDataFrame(events, eventsSchema);
    results.javaRDD().saveAsTextFile(args[3]);

    // stop spark context
    sparkContext.stop();
    
    // print time measurements
    System.out.println("stopped time: " + (System.currentTimeMillis()-start));
  }
}