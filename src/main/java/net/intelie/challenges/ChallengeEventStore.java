package net.intelie.challenges;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ChallengeEventStore implements EventStore {
	
	private Map<String, Set<Event>> store = new HashMap<>(); 

	@Override
	public synchronized void insert(Event event) {
		Set<Event> events = store.computeIfAbsent(event.type() , type -> new HashSet<>());
		events.add(event);
	}

	@Override
	public synchronized void removeAll(String type) {
		store.remove(type);
	}

	@Override	
	public synchronized EventIterator query(String type, long startTime, long endTime) {								
		List<Event> events = store.getOrDefault(type, Collections.emptySet())
					.parallelStream().filter(event -> eventBetween(event, startTime, endTime))
					.collect(Collectors.toList());
		return buildIterator(events);
		
	}
	
	private EventIterator buildIterator(List<Event> listEvents) {
		return new EventIterator() {
			
			private  List<Event> events = listEvents;
			private int currentIndex = 0;

			@Override
			public void close() throws Exception {
				currentIndex = 0;
				events.clear();
			}

			@Override
			public boolean moveNext() {
				return currentIndex < events.size();				
			}

			@Override
			public Event current() {
				return events.get(currentIndex++);
			}

			@Override
			public void remove() {
				if (currentIndex == 0)
					throw new UnsupportedOperationException();
				Event event = events.remove(currentIndex-1);
				ChallengeEventStore.this.remove(event);				
			}};
	}		
	
	private boolean eventBetween(Event event, long startTime, long endTime) {
		return (event.timestamp() >= startTime) && (event.timestamp() <= endTime);
	}
		
	private synchronized void remove(Event event) {								
		store.getOrDefault(event.type(), Collections.emptySet()).remove(event);
	}
	
	public long count() {
		return count(null);
	}
	
	public synchronized long count(String type) {
		Set<Event> events = Objects.isNull(type) 
				? store.values().parallelStream().flatMap(listEvents -> listEvents.stream()).collect(Collectors.toSet())
						: store.getOrDefault(type, Collections.emptySet());
		return events.parallelStream().count();
	}	

}
