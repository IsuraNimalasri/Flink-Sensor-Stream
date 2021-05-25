package io.intellisense.testproject.eng.function;

import io.intellisense.testproject.eng.model.DataPoint;
import lombok.Data;
import lombok.extern.flogger.Flogger;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.Collections;

public class AnomalousScore extends ProcessWindowFunction<Tuple3<String, String, Double>, DataPoint, Object, GlobalWindow> implements WindowFunction<Tuple3<String, String, Double>, Double, String, Window>  {
    private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    @Override
    public void process(Object o, Context context, Iterable<Tuple3<String, String, Double>> events, Collector<DataPoint> out ) throws Exception {
        ArrayList<Double> readings = new ArrayList<Double>();

        events.forEach(event ->{
            readings.add(event.f2);
        });

        double iqr = findIQR(readings);

        events.forEach(event ->{
            Instant timestapm = Instant.parse(event.f0);
            DataPoint dataPoint = new DataPoint(event.f1,timestapm,findAnomaly(event.f2,iqr));
            out.collect(dataPoint);
        });
    }

    @Override
    public void apply(String s, Window window, Iterable<Tuple3<String, String, Double>> iterable, Collector<Double> collector) throws Exception {

    }

//
    public Double findAnomaly(Double value,Double IQR){
        double anomalousScore = -1.0;
        LOGGER.info("Value :"+value + " IQR*1.5 : "+(1.5 * IQR) + " IQR*3 " + (3 * IQR) + " IQR :" +IQR );
        if (value < (1.5 * IQR)) anomalousScore = 0;
        else if( (value < (3 * IQR)) && (value >= (1.5 * IQR))) anomalousScore = 0.5;
        else if (value >= (3 * IQR)) anomalousScore = 1;
        return anomalousScore;

    }
//
    public Double findIQR(ArrayList<Double> readings) {
        double iqr ;
        int q1Pos = 0;
        int q3Pos = 0;
        if (readings.size() % 4 == 0) {
            q1Pos = (readings.size() / 4)-1;
            q3Pos= ((readings.size() / 4)*3)-1;
            LOGGER.info(" This window Q1 Value :  " + q1Pos + " | Q3 place :" +q3Pos + " | readingArraySize :" + readings.size());
            // sort sensor reading values
            Collections.sort(readings);
            LOGGER.info(" This window Q1 Value : " + readings.get(q1Pos)  + "| Q3 Value" +readings.get(q3Pos) );
            iqr = readings.get(q3Pos) - readings.get(q1Pos);
            LOGGER.info(" This window IQR = "+iqr);
            return iqr;
        }
        else {
            q1Pos = ( (readings.size()+1) / 4);
            q3Pos= (((readings.size()+1 )/ 4)*3);
            LOGGER.info(" This window Q1 Value :  " + q1Pos + " | Q3 place :" +q3Pos + " | readingArraySize :" + readings.size());
            // sort sensor reading values
            Collections.sort(readings);
            LOGGER.info(" This window Q1 Value : " + readings.get(q3Pos)+","+readings.get(q3Pos+1) + "| Q3 Value  " +readings.get(q3Pos)+",  "+readings.get(q3Pos+1));
            iqr =  ((readings.get(q3Pos) + readings.get(q3Pos+1) )/2 )- ((readings.get(q1Pos) + readings.get(q1Pos+1) )/2 );
            LOGGER.info(" This window IQR = "+iqr);
            return iqr;

        }

    }


}
