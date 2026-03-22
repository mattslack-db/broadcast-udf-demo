package org.example;

public class CalculatorFactory {

    private final ReferenceDataInput cache;

    public CalculatorFactory(ReferenceDataInput cache) { this.cache = cache; }

    public Calculator getCalculator(CalculatorInputObject input) {
        return new Calculator(cache, input);
    }

}
