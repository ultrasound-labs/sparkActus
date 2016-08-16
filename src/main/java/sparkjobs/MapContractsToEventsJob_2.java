package sparkjobs;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;

import org.actus.conversion.DateConverter;

import javax.time.calendar.ZonedDateTime;
import scala.Tuple2;
import java.util.Map;
import java.util.Arrays;
import java.util.Iterator;

public class MapContractsToEventsJob_2 {

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Please provide the input file full path as argument");
      System.exit(0);
    }
    //Input Parameter:
    String contractsFile = args[0]; //hdfs://160.85.30.40/user/spark/data/contracts_10000000.csv 
    String riskfactorsFile = args[1]; //hdfs://160.85.30.40/user/spark/data/riskfactors_input.csv 
    String timespecsFile = args[2]; //hdfs://160.85.30.40/user/spark/data/timespecs_input.csv 
    String outputPath = args[3]; //hdfs://160.85.30.40/user/spark/data/output/; 
    //String way = args[3];  //count or Group by
    int nPartitions = (int) Double.parseDouble(args[4]);
    String debug = args[5];

//    // Create Spark Context (Old Version)
//    SparkConf conf = new SparkConf().setAppName("sparkjobs.MapContractsToEventsJob");//.setMaster("local");
//    JavaSparkContext sparkSession = new JavaSparkContext(conf);
//    // Create SQL Context 
//    SQLContext sqlContext = new SQLContext(sparkSession);
   
    //Create Spark Context
    SparkSession sparkSession = SparkSession
    		.builder()
    		.appName("sparkjobs.MapContractsToEventsJob")
    		.getOrCreate();
    //Create SQL Context -> obsolet?
    //SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sparkSession);
    
    // for time stopping
    long start = System.currentTimeMillis();
     
    // import and broadcast analysis date
    JavaRDD<String> timeSpecs = sparkSession.read().textFile(timespecsFile).javaRDD(); // analysis time specification
    JavaRDD<String> timeVector = timeSpecs.flatMap(line -> Arrays.asList(line.split(";")).iterator());
//    String[] timeVector = timeSpecsLine.first();
    ZonedDateTime _t0 = null;
    try{
    	_t0 = DateConverter.of(timeVector.first());  	
//      _t0 = DateConverter.of(timeVector[0]);
    } catch(Exception e) {
      System.out.println(e.getClass().getName() + " when converting the analysis date to ZonedDateTime!");
    }
    //Debug Info
    if(debug.equals("debug")){
    	System.out.println(_t0);
    }
    //Broadcast<ZonedDateTime> t0 = sparkSession.broadcast(_t0);
    
    // import risk factor data, map to connector and broadcast
    JavaRDD<String> riskFactor = sparkSession.read().textFile(riskfactorsFile).javaRDD(); // contract data
    JavaPairRDD<String, String[]> riskFactorRDD = riskFactor.mapToPair(temp -> new Tuple2<String, String[]>(temp.split(";")[0], temp.split(";")));
//    JavaPairRDD<String, String[]> riskFactorRDD = riskFactor.mapToPair(
//      new PairFunction<String, String, String[]>() {
//        public Tuple2<String, String[]> call(String s) {
//          String[] temp = s.split(";");
//          return new Tuple2(temp[0], temp);
//        }
//      });    
    //Debug Info
    if(debug.equals("debug")){
    	System.out.println(riskFactorRDD.first());
    }
    //Broadcast<Map<String,String[]>> riskFactors = riskFactorRDD.collectAsMap();
    
	// import and map contract data to contract event results
    JavaRDD<String> contractFile = sparkSession.read().textFile(contractsFile).javaRDD().repartition(nPartitions); // contract data
    JavaRDD<Row> events = contractFile.map(new ContractToEventsFunction_2(_t0, riskFactorRDD.collectAsMap()));
   // JavaRDD<Row> events = contractFile.map(new ContractToEventsFunction(t0, riskFactors));
    
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
    Dataset<Row> chachedEvents = sparkSession.createDataFrame(events, eventsSchema).cache();
 	 
    
    //For SQL Querying:
 	//chachedEvents.registerTempTable("events");
    chachedEvents.createOrReplaceTempView("events");

    //Debug Info
    if(debug.equals("debug")){
	    chachedEvents.printSchema();
	    chachedEvents.show();
    }
    
 // DataFrames can be saved as Parquet files, maintaining the schema information.
 	chachedEvents.write().parquet(outputPath);
 	//chachedEvents.select("id").write().text(outputPath);
 	
//Data is now ready and it's possible to query the data:
 	 
    
// 	results.registerTempTable("events");
// 	DataFrame dfCount = 	results
//				.sqlContext().sql("SELECT COUNT(*) "
//								+ "FROM events ");
// 	dfCount.show();
     
 	 
 	 // This is not necessary any more
// 	// count records or save file
//    if (way.equals("count")){
//    	results.registerTempTable("events");
//    	DataFrame dfCount = 	results
//				.sqlContext().sql("SELECT COUNT(*) "
//								+ "FROM events ");
//    	dfCount.show();
//    }
//    else {
// 	 results.javaRDD().saveAsTextFile(outputPath);
//    }
    
    
    
    // stop spark context
    sparkSession.stop();
    
    // print time measurements
     // System.out.println(way);
    System.out.println("stopped time in Sec.: " + (System.currentTimeMillis()-start)/1000);
  }
}
