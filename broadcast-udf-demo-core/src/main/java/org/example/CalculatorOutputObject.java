package org.example;

import java.math.BigDecimal;
import java.sql.Timestamp;

public class CalculatorOutputObject {

    private long outputCol1;
    private String outputCol2;
    private double outputCol3;
    private double outputCol4;
    private Timestamp outputCol5;

    public CalculatorOutputObject() {}

    public long getOutputCol1() { return outputCol1; }

    public void setOutputCol1(long outputCol1) {
        this.outputCol1 = outputCol1;
    }

    public String getOutputCol2() { return outputCol2; }

    public void setOutputCol2(String outputCol2) {
        this.outputCol2 = outputCol2;
    }

    public double getOutputCol3() { return outputCol3; }

    public void setOutputCol3(double outputCol3) {
        this.outputCol3 = outputCol3;
    }

    public double getOutputCol4() { return outputCol4; }

    public void setOutputCol4(double outputCol4) {
        this.outputCol4 = outputCol4;
    }

    public Timestamp getOutputCol5() { return outputCol5; }

    public void setOutputCol5(Timestamp outputCol5) { this.outputCol5 = outputCol5; }

}
