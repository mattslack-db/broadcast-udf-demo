package org.example;

public class AnExampleClassFactory {

    public static AnExampleClass1 create(String col1, String col2) {
        return new AnExampleClass1(col1, col2);
    }

}
