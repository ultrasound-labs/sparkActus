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
import org.actus.conversion.PeriodConverter;
import org.actus.conversion.DoubleConverter;
import org.actus.riskfactors.RiskFactorConnector;
import org.actus.misc.riskfactormodels.SpotRateCurve;
import org.actus.misc.riskfactormodels.SimpleReferenceRate;
import org.actus.misc.riskfactormodels.SimpleReferenceIndex;
import org.actus.misc.riskfactormodels.SimpleForeignExchangeRate;

import javax.time.calendar.ZonedDateTime;
import scala.Tuple2;
import java.util.Map;

public class ExperimentalMapContractsToEventsJob {

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Please provide the input file full path as argument");
      System.exit(0);
    }

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
    Map<String,String[]> riskFactorMap = riskFactorRDD.collectAsMap();
    RiskFactorConnector rfCon = new RiskFactorConnector();
    SpotRateCurve curve = new SpotRateCurve();
    SimpleReferenceRate refRate = new SimpleReferenceRate();
    SimpleReferenceIndex refIndex = new SimpleReferenceIndex();
    SimpleForeignExchangeRate fxRate = new SimpleForeignExchangeRate();
    String[] rf;
    String[] keys = riskFactorMap.keySet().toArray(new String[riskFactorMap.size()]);
    try{
    for(int i=0; i<keys.length; i++) {
       rf = riskFactorMap.get(keys[i]);
       if(rf[2].equals("TermStructure")) {
         curve.of(_t0, PeriodConverter.of(rf[3].split("!")), DoubleConverter.ofArray(rf[4],"!"));
         rfCon.add(rf[0], curve);
       } else if(rf[2].equals("ReferenceRate")){
         refRate.of(DateConverter.of(rf[3].split("!")), DoubleConverter.ofDoubleArray(rf[4],"!"));
         rfCon.add(rf[0], refRate);         
       } else if(rf[2].equals("ReferenceIndex")){
         refIndex.of(DateConverter.of(rf[3].split("!")), DoubleConverter.ofDoubleArray(rf[4],"!"));
         rfCon.add(rf[0], refIndex);         
       } else if(rf[2].equals("ForeignExchangeRate")){
         fxRate.of(DateConverter.of(rf[3].split("!")), DoubleConverter.ofDoubleArray(rf[4],"!"));
         rfCon.add(rf[0], fxRate);         
       }
    }
    } catch(Exception e) {
      System.out.println(e.getClass().getName() + 
                         " when converting risk factor data to actus objects: " +
                        e.getMessage());
    }
    Broadcast<RiskFactorConnector> riskFactors = sparkContext.broadcast(rfCon);
    
    
	// import and map contract data to contract event results
    JavaRDD<String> contractFile = sparkContext.textFile(args[0]); // contract data
    //JavaRDD<Row> events = contractFile.map(new ContractToEventsFunction(t0, riskFactors));
    
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
 	 //DataFrame results = sqlContext.createDataFrame(events, eventsSchema);
    //results.javaRDD().saveAsTextFile(args[3]);
  }
}