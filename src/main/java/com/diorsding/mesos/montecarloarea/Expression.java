package com.diorsding.mesos.montecarloarea;

import java.util.ArrayList;
import java.util.List;

public class Expression {
    List<Term> terms;

    public Expression() {
        terms = new ArrayList<Term>();
    }

    public Expression(List<Term> terms) {
        this.terms = terms;
    }

    public boolean addTerm(Term term) {
        return terms.add(term);
    }

    public double evaluate(double x) {
        double value = 0;
        for (Term term : terms) {
            value += term.evaluate(x);
        }
        return value;
    }

    public static Expression fromString(String s) {
        Expression expression = new Expression();
        String[] terms = s.split("\\+");
        for (String term : terms) {
            expression.addTerm(Term.fromString(term));
        }
        return expression;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        int i;
        for (i = 0; i < terms.size() - 1; i++) {
            builder.append(terms.get(i)).append(" + ");
        }
        builder.append(terms.get(i));
        return builder.toString();
    }
}
