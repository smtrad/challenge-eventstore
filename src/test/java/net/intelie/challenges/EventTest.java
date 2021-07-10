package net.intelie.challenges;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;

public class EventTest {
		
	private static final String[] TYPES = {"A","B","C","D","E","F"};
	private static final int MAX_TIMESTAMP = 86400;
	private static final int CONCURRENT_THREADS = 100000;
	private static final long TIME_INTERVAL = 100;
		
	private Random  r = new Random();
	
	private ChallengeEventStore eventStore;
	private Map<String, Long> eventCounter;
	
	@Before
	public void setUp() {
		eventStore = new ChallengeEventStore();
		eventCounter = new HashMap<>();
	}		
	
	private Event buildRandonEvent() {	     
    	return buildRandonEvent(null);
    }
	
	private synchronized Event buildRandonEvent(String type) {
		if (Objects.isNull(type))
			type = TYPES[r.nextInt(TYPES.length)]; 
    	long at = r.nextInt(MAX_TIMESTAMP);
    	eventCounter.compute(type, (key, value) -> Objects.isNull(value) ? 1 : ++value);
    	return new Event(type, at);
    }
	
	private synchronized boolean randonRemove(String type) {		
		int action = r.nextInt(2);
		if (action > 0)
			eventCounter.compute(type, (key, value) -> Objects.isNull(value) ? 1 : --value);
    	return action != 0;
    }
	
    @Test
    public void thisIsAWarning() throws Exception {
        Event event = new Event("some_type", 123L);
        assertEquals(123L, event.timestamp());
        assertEquals("some_type", event.type());
    }       
    
    @Test
    public void singleThread_eventStore() throws Exception {    	
    	Event eventA = buildRandonEvent("A");
    	Event eventB = buildRandonEvent("B");    	

    	//inserts
    	assertEquals(0L, eventStore.count());
        eventStore.insert(eventA);
        assertEquals(1L, eventStore.count());
        eventStore.insert(eventB);
        assertEquals(2L, eventStore.count());
        assertEquals(1L, eventStore.count(eventA.type()));
        
        //query
        try (EventIterator it = eventStore.query(eventA.type(), eventA.timestamp(), eventA.timestamp())){
	        while (it.moveNext()) {
	        	assertEquals(eventA, it.current());
	        	
	        	//remove event by iterator
	        	it.remove();
	        	assertEquals(0L, eventStore.count(eventA.type()));
	        	assertEquals(1L, eventStore.count(eventB.type()));
	        }
        }
        
        //remove all                        
        eventStore.removeAll(eventB.type());
        assertEquals(0L, eventStore.count());    
    }
    
    @Test
    public void multiThread_eventStore() throws InterruptedException {
    	System.out.println("Thread-safe checking, wait...");
        ExecutorService service = Executors.newSingleThreadExecutor();        
        CountDownLatch latch = new CountDownLatch(CONCURRENT_THREADS);        
        for (int i = 0; i < CONCURRENT_THREADS; i++) {
            service.submit(() -> {
                try {
                	Event event = buildRandonEvent();
					eventStore.insert(event);
					try (EventIterator it = eventStore.query(event.type(), event.timestamp() - TIME_INTERVAL, event.timestamp() + TIME_INTERVAL)){
						while (it.moveNext()) {
							if (it.current().equals(event)
									&& randonRemove(event.type())) {
								it.remove();
							}
						}
					} catch (Exception e) {					
						e.printStackTrace();
					}
                }finally {
                	latch.countDown();	
				}                
            });
        }
        latch.await();
        long globalCount = 0l;
        for (String type : eventCounter.keySet()) {
        	long count = eventCounter.getOrDefault(type, 0l);
        	globalCount += count;
        	System.out.print(String.format("Check event type '%s' on store with '%s' events...", type, count));
        	assertEquals(count, eventStore.count(type));
        	System.out.println("success ");
        }        
        assertEquals(globalCount, eventStore.count());
    }
        
    @Test(expected = UnsupportedOperationException.class)
    public void eventStoreIteratorRemoveFail() {
    	eventStore.query("any", 0, 0).remove();
    }
    
}