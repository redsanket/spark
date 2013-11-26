package org.concordion.internal;

import java.io.IOException;

import org.concordion.api.FullOGNL;
import org.concordion.api.ResultSummary;
import org.concordion.internal.extension.FixtureExtensionLoader;

public class FixtureRunner {
    private FixtureExtensionLoader fixtureExtensionLoader = new FixtureExtensionLoader(); 
    
    public ResultSummary run(final Object fixture) throws IOException {
        ConcordionBuilder concordionBuilder = new ConcordionBuilder();
        if (fixture.getClass().isAnnotationPresent(FullOGNL.class)) {
            concordionBuilder.withEvaluatorFactory(new OgnlEvaluatorFactory());
        }
        fixtureExtensionLoader.addExtensions(fixture, concordionBuilder);
        ResultSummary resultSummary = concordionBuilder.build().process(fixture);
        resultSummary.print(System.out, fixture);
        resultSummary.assertIsSatisfied(fixture);
        return resultSummary;
    }
}