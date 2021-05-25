package io.intellisense.testproject.eng.function;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;


public class PurifyRowEvents implements FilterFunction<Row> {
    @Override
    public boolean filter(Row row) throws Exception {
//  filter events dons't have timestamp value.
        if ((row.getField(0).toString().trim().isEmpty() ) || (row.getField(0) == null) ){
            return false;
        }
        else {
            return true;
        }
    }
}


