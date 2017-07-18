package com.traackr.mongo.tailer;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import lombok.Data;
import lombok.ToString;

/**
 * Unit test for simple App.
 */
public class AppTest
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    @ToString(includeFieldNames=true)
    @Data(staticConstructor="with")
    public static class ImmutableTest {
      private final Boolean one;
      private final Boolean two;
    }

    public void testApp()
    {
      ImmutableTest it = ImmutableTest.with(true, true);
      System.out.println(it);
    }
}
