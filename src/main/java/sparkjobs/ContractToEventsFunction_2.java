package sparkjobs;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import org.actus.contracttypes.PrincipalAtMaturity;
import org.actus.models.PrincipalAtMaturityModel;
import org.actus.util.time.EventSeries;
import org.actus.conversion.PeriodConverter;
import org.actus.conversion.DoubleConverter;
import org.actus.conversion.DateConverter;
import org.actus.contractstates.StateSpace;
import org.actus.riskfactors.RiskFactorConnector;
import org.actus.misc.riskfactormodels.SpotRateCurve;
import org.actus.misc.riskfactormodels.SimpleReferenceRate;
import org.actus.misc.riskfactormodels.SimpleReferenceIndex;
import org.actus.misc.riskfactormodels.SimpleForeignExchangeRate;

import javax.time.calendar.ZonedDateTime;
import java.util.Arrays;
import java.util.Map;

public class ContractToEventsFunction_2 implements Function<String,Row> {
//  Broadcast<ZonedDateTime> t0;
//  Broadcast<Map<String,String[]>> riskFactors;
	  ZonedDateTime t0;
	  Map<String,String[]> riskFactors;
  
//  public ContractToEventsFunction(Broadcast<ZonedDateTime> t0,
//                                  Broadcast<Map<String,String[]>> riskFactors) {
	  public ContractToEventsFunction_2(ZonedDateTime t0,
              Map<String,String[]> riskFactors) {
   this.t0 = t0;
    this.riskFactors = riskFactors;
 }
  
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
        // accrued interest
        // ipced
        // cey
        // ipcp
        model.setCurrency(terms[16]);
        model.setContractDealDate(terms[17]);
        model.setInitialExchangeDate(terms[18]);
        model.setMaturityDate(terms[19]);
        model.setNotionalPrincipal(Double.parseDouble(terms[20]));
        model.setPurchaseDate(terms[21]);
              model.setPriceAtPurchaseDate(Double.parseDouble(terms[22]));
        model.setTerminationDate(terms[23]);
              model.setPriceAtTerminationDate(Double.parseDouble(terms[24]));
        model.setMarketObjectCodeOfScalingIndex(terms[25]);
        model.setScalingIndexAtStatusDate(Double.parseDouble(terms[26]));
        //model.setCycleAnchorDateOfScalingIndex(terms[28]);
        //model.setCycleOfScalingIndex(terms[29]);
        //model.setScalingEffect(terms[30]);
        model.setCycleAnchorDateOfRateReset(terms[30]);
        model.setCycleOfRateReset(terms[31]);
        model.setRateSpread(Double.parseDouble(terms[32]));
        model.setMarketObjectCodeRateReset(terms[33]);
        model.setCyclePointOfRateReset(terms[34]);
        model.setFixingDays(terms[35]);
        model.setNextResetRate(Double.parseDouble(terms[36]));
        model.setRateMultiplier(Double.parseDouble(terms[37]));
        model.setRateTerm(terms[38]);
      // yield curve correction missing
        model.setPremiumDiscountAtIED(Double.parseDouble(terms[39]));
      
    } catch (Exception e) {
      System.out.println(e.getClass().getName() + 
                         " thrown when mapping terms to ACTUS ContractModel: " +
                        e.getMessage());
    }
    return model;
  }
  
  private static RiskFactorConnector mapRF(Map<String,String[]> rfData, 
                                           String marketObjectCodeRateReset,
                                           String marketObjectCodeScaling,
                                           ZonedDateTime t0) {
    RiskFactorConnector rfCon = new RiskFactorConnector();
    SpotRateCurve curve = new SpotRateCurve();
    SimpleReferenceRate refRate = new SimpleReferenceRate();
    SimpleReferenceIndex refIndex = new SimpleReferenceIndex();
    SimpleForeignExchangeRate fxRate = new SimpleForeignExchangeRate();
    String[] rf;
    String[] keys = rfData.keySet().toArray(new String[rfData.size()]);
    try{
      if(!marketObjectCodeRateReset.equals("NULL")) {
       rf = rfData.get(marketObjectCodeRateReset);
       if(rf[2].equals("TermStructure")) {
         curve.of(t0, PeriodConverter.of(rf[3].split("!")), DoubleConverter.ofArray(rf[4],"!"));
         rfCon.add(rf[0], curve);
       } else if(rf[2].equals("ReferenceRate")){
         refRate.of(DateConverter.of(rf[3].split("!")), DoubleConverter.ofDoubleArray(rf[4],"!"));
         rfCon.add(rf[0], refRate);         
       } 
      }
      if(!marketObjectCodeScaling.equals("NULL")) {
        rf = rfData.get(marketObjectCodeScaling);
         refIndex.of(DateConverter.of(rf[3].split("!")), DoubleConverter.ofDoubleArray(rf[4],"!"));
         rfCon.add(rf[0], refIndex);         
    	}
    } catch(Exception e) {
      System.out.println(e.getClass().getName() + 
                         " when converting risk factor data to actus objects: " +
                        e.getMessage());
    }
    
    return rfCon;
  }
  
        public Row call(String s) throws Exception {
          
          // map input file to contract model 
          // (note, s is a single line of the input file)
          PrincipalAtMaturityModel pamModel = mapTerms(s.split(";"));
          
          // map risk factor data to actus connector
//          RiskFactorConnector rfCon = mapRF(riskFactors.value(), 
//                                            pamModel.getMarketObjectCodeRateReset(),
//                                            pamModel.getMarketObjectCodeOfScalingIndex(),
//                                            t0.value());
          RiskFactorConnector rfCon = mapRF(riskFactors, 
                  pamModel.getMarketObjectCodeRateReset(),
                  pamModel.getMarketObjectCodeOfScalingIndex(),
                  t0);
    
          // init actus contract type
          PrincipalAtMaturity pamCT = new PrincipalAtMaturity();
          pamCT.setContractModel(pamModel);
          pamCT.setRiskFactors(rfCon);
          pamCT.generateEvents(t0);
          pamCT.processEvents(t0);
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
      } 
