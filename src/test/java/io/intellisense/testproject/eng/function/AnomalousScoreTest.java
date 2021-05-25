package io.intellisense.testproject.eng.function;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

class AnomalousScoreTest {

    @Test
    public void testFindAnomaly() {
        AnomalousScore anomalousScore = new AnomalousScore();

        double iqr = 1.0;

        // Basic Conditions
        double reading_1  = 0;
        assertEquals(0,anomalousScore.findAnomaly(reading_1,iqr));

        double reading_2  = -100;
        assertEquals(0,anomalousScore.findAnomaly(reading_2,iqr));

        double reading_3  = 2.25;
        assertEquals(0.5,anomalousScore.findAnomaly(reading_3,iqr));

        double reading_4  = 3.5;
        assertEquals(1,anomalousScore.findAnomaly(reading_4,iqr));

        // boundry condiction

        // value >= (3 * IQR)
        double reading_5  = 2.99999999;
        assertEquals(0.5,anomalousScore.findAnomaly(reading_5,iqr));
        double reading_6  = 2.99999999999999999999999999999999999999999999;
        assertEquals(1,anomalousScore.findAnomaly(reading_6,iqr));
        double reading_7  = 3.000000;
        assertEquals(1,anomalousScore.findAnomaly(reading_7,iqr));


        //(value < (3 * IQR)) && (value >= (1.5 * IQR))
        double reading_8  = 1.49999999999999999999999;
        assertEquals(0.5,anomalousScore.findAnomaly(reading_8,iqr));
        double reading_9  = 1.5000000000000000000;
        assertEquals(0.5,anomalousScore.findAnomaly(reading_9,iqr));







    }

    @Test
    public void testFindIQR() {
        ArrayList<Double> readings = new ArrayList();
        readings.add(0.111);
        readings.add(0.102);
        readings.add(0.113);
        readings.add(0.114);
        readings.add(0.108);
        readings.add(0.109);
        readings.add(0.110);
        readings.add(0.107);
        readings.add(0.108);
        readings.add(0.109);
        readings.add(0.110);
        readings.add(0.107);

    // create instant
        AnomalousScore anomalousScore = new AnomalousScore();
    // sensor data accuracy should have around 19/20 decimal pints
        assertEquals(0.0030000000000000027,anomalousScore.findIQR(readings));
    }
}