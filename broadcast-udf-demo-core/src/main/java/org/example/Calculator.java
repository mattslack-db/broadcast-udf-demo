package org.example;

import java.sql.Timestamp;
import java.math.BigDecimal;

public class Calculator {

    private final ReferenceDataInput cache;
    private final CalculatorInputObject input;

    public Calculator(ReferenceDataInput cache, CalculatorInputObject input) {
        this.cache = cache;
        this.input = input;
    }

    public CalculatorOutputObject calculate() {
        CalculatorOutputObject output = new CalculatorOutputObject();

        if (this.input.getInputCol3() < 0.0) {
            throw new ArithmeticException("Cannot take square root of negative number");
        }

        output.setOutputCol1(this.input.getInputCol1() + 1);
        output.setOutputCol2(this.input.getInputCol2().concat(" (output)"));
        output.setOutputCol3(Math.sqrt(this.input.getInputCol3()));
        output.setOutputCol4(this.input.getInputCol4() * 2.0);
        //output.setOutputCol4(this.input.getInputCol4().multiply(new BigDecimal("2.0")));
        output.setOutputCol5(new Timestamp(this.input.getInputCol5().getTime() + 10000));

        return output;
    }
}
