package com.diorsding.mesos.montecarloarea;

public class Term {
    double coefficient;
    double exponent;

    Term() {
        coefficient = exponent = 0;
    }

    Term(double coefficient, double exponent) {
        this.coefficient = coefficient;
        this.exponent = exponent;
    }

    public static Term fromString(String term) {
        double coefficient = 1;
        double exponent = 0;
        String[] splits = term.split("x", -1);

        if (splits.length > 0) {
            String coefficientString = splits[0].trim();
            if (!coefficientString.isEmpty()) {
                coefficient = Double.parseDouble(coefficientString);
            }
        }

        if (splits.length > 1) {
            exponent = 1;
            String exponentString = splits[1].trim();
            if (!exponentString.isEmpty()) {
                exponent = Double.parseDouble(exponentString);
            }
        }
        return new Term(coefficient, exponent);
    }

    @Override
    public String toString() {
        return coefficient + "x^" + exponent;
    }

    public double evaluate(double x) {
        return coefficient * Math.pow(x, exponent);
    }
}
