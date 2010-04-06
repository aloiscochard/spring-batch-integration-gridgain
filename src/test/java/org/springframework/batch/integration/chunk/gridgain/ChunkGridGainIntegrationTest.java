package org.springframework.batch.integration.chunk.gridgain;

import org.gridgain.grid.GridException;
import org.gridgain.grid.GridFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:simple-job-launcher-context.xml")
public class ChunkGridGainIntegrationTest {

    @AfterClass
    public static void doAfterClass() {
        GridFactory.stop(true);
    }

    @BeforeClass
    public static void doBeforeClass() throws GridException {
        GridFactory.start();
    }

    @Test
    public void test() {
        System.out.println("TEST");
    }
}
