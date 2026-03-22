package org.example;

import java.math.BigDecimal;
import java.sql.Timestamp;

public class CalculatorInputObject {

    private long inputCol1;
    private String inputCol2;
    private double inputCol3;
    private double inputCol4;
    private Timestamp inputCol5;

    public CalculatorInputObject() {}

    public long getInputCol1() { return inputCol1; }

    public void setInputCol1(long inputCol1) { this.inputCol1 = inputCol1; }

    public String getInputCol2() { return inputCol2; }

    public void setInputCol2(String inputCol2) { this.inputCol2 = inputCol2; }

    public double getInputCol3() { return inputCol3; }

    public void setInputCol3(double inputCol3) { this.inputCol3 = inputCol3; }

    public double getInputCol4() { return inputCol4; }

    public void setInputCol4(double inputCol4) { this.inputCol4 = inputCol4; }

    public Timestamp getInputCol5() { return inputCol5; }

    public void setInputCol5(Timestamp inputCol5) { this.inputCol5 = inputCol5; }

}
