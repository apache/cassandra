package org.apache.cassandra.gms;

import static org.junit.Assert.*;

import org.junit.Test;

public class ArrivalWindowTest
{
    
    @Test
    public void test()
    {
        ArrivalWindow window = new ArrivalWindow(4);
        //base readings
        window.add(111);
        window.add(222);
        window.add(333);
        window.add(444);
        window.add(555);

        //all good
        assertEquals(0.4342, window.phi(666), 0.01);
        
        //oh noes, a much higher timestamp, something went wrong!
        assertEquals(9.566, window.phi(3000), 0.01);
    }


}
