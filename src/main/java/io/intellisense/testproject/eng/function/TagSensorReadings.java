package io.intellisense.testproject.eng.function;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

// This flatMap retrun sensor wise reading.
public class TagSensorReadings extends RichFlatMapFunction<Row, Tuple3<String,String,Double>> {
    @Override
    public void flatMap(Row value, Collector<Tuple3<String, String, Double>> out) throws Exception {

        assert value.getField(0) != null;
        String timeTag = value.getField(0).toString();
        String sensor = "Sensor-";
        for (int i=value.getArity()-1; i  > 0 ; i--) {
            try {
                if (value.getField(i).toString().trim().isEmpty()){
                    value.setField(i, 0);
                }
                double reading=Double.parseDouble(value.getField(i).toString());
                Tuple3 mappedValue = Tuple3.of(timeTag,sensor + i,reading);
                out.collect(mappedValue);
            }
            catch (Exception e)
            {
                System.out.println("value :  " + value.toString());
                e.printStackTrace();

            }

        }

    }


}
