package sparkjobs;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;

import org.actus.contracttypes.PrincipalAtMaturity;
import org.actus.models.PrincipalAtMaturityModel;
import org.actus.util.time.EventSeries;
import org.actus.conversion.DateConverter;
import org.actus.contractstates.StateSpace;

public class TestActusJob {

  private static PrincipalAtMaturityModel mapTerms(String[] terms) {
    		PrincipalAtMaturityModel model = new PrincipalAtMaturityModel();
    try{
    		model.setStatusDate(terms[4]);
        model.setContractRole(terms[5]);
        model.setContractID(terms[7]);
        model.setCycleAnchorDateOfInterestPayment(terms[9]);
        model.setCycleOfInterestPayment(terms[10]);
        model.setNominalInterestRate(Double.parseDouble(terms[11]));
        model.setDayCountConvention(terms[12]);
        model.setCurrency(terms[17]);
        model.setContractDealDate(terms[18]);
        model.setInitialExchangeDate(terms[19]);
        model.setMaturityDate(terms[20]);
        model.setNotionalPrincipal(Double.parseDouble(terms[21]));
    } catch (Exception e) {
      System.out.println(e.getClass().getName() + 
                         " thrown when mapping terms to ACTUS ContractModel");
    }
    return model;
  }

  private static final Function<String, Row> CONTRACT_MAPPER =
      new Function<String, Row>() {
        @Override
        public Row call(String s) throws Exception {
          
          // map input file to contract model 
          // (note, s is a single line of the input file)
          PrincipalAtMaturityModel pamModel = mapTerms(s.split(";"));
          
          // init actus contract type
          PrincipalAtMaturity pamCT = new PrincipalAtMaturity();
          pamCT.setContractModel(pamModel);
          pamCT.generateEvents(DateConverter.of("2015-04-01T00"));
          pamCT.processEvents(DateConverter.of("2015-04-01T00"));
          EventSeries events = pamCT.getProcessedEventSeries();
          
          // stucture results als array of Rows
          int nEvents = events.size();
          String[] id = new String[nEvents];
          String[] dt = new String[nEvents];
          String[] cur = new String[nEvents];
          Double[] nv = new Double[nEvents];
          Double[] na = new Double[nEvents];
          StateSpace states;
          for(int i=0;i<nEvents;i++) {
            dt[i] = events.get(i).getEventDate().toString();
            cur[i] = events.get(i).getEventCurrency().toString();
            states = events.get(i).getStates();
            nv[i] = states.getNominalValue();
            na[i] = states.getNominalAccrued();
           
          }
          Arrays.fill(id,0,nEvents,pamModel.getContractID());
          
          Row results = RowFactory.create(id,
                                         dt,
                                         events.getEventTypes(),
                                         cur,
                                         events.getEventValues(),
                                         nv,
                                         na);
          
          // return results
          return results;
        }
      }; 
  
  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Please provide the input file full path as argument");
      System.exit(0);
    }

    SparkConf conf = new SparkConf().setAppName("sparkjobs.TestActusJob").setMaster("local");
    JavaSparkContext sparkContext = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sparkContext);
    
    // load files
    JavaRDD<String> contractFile = sparkContext.textFile(args[0]); // first argument
    
	// map data to contract event results
    JavaRDD<Row> events = contractFile.map(CONTRACT_MAPPER);
    
    // convert to DataFrame
    StructType schema = DataTypes
            .createStructType(new StructField[] {
                DataTypes.createStructField("id", DataTypes.createArrayType(DataTypes.StringType), false),
                DataTypes.createStructField("date", DataTypes.createArrayType(DataTypes.StringType), false),
                DataTypes.createStructField("type", DataTypes.createArrayType(DataTypes.StringType), false),
                DataTypes.createStructField("currency", DataTypes.createArrayType(DataTypes.StringType), false),
                DataTypes.createStructField("value", DataTypes.createArrayType(DataTypes.DoubleType), false),
                DataTypes.createStructField("nominal", DataTypes.createArrayType(DataTypes.DoubleType), false),
                DataTypes.createStructField("accrued", DataTypes.createArrayType(DataTypes.DoubleType), false)});
 					 DataFrame results = sqlContext.createDataFrame(events, schema);
    results.javaRDD().saveAsTextFile(args[3]);
  }
}