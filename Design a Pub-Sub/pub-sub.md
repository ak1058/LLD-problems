# Problem

Design a Publisher-Subscriber (Pub-Sub) system where publishers publish events of a particular event type,
and all subscribers subscribed to that event type receive the event. Subscribers can subscribe/unsubscribe.

## Requirements

### Functional requirements

1. Publisher should be able to publish an event.
2. Event should have an event type.
3. Subscriber should be able to subscribe to an event type.
4. Subscriber should be able to unsubscribe.
5. When an event is published, all subscribers of that event type should receive it.

### Non-functional requirements

For an LLD interview, initially keep it simple:
1. Maintainable
2. Extensible
3. Thread-safe if multiple publishers/subscribers exist

## Main Entities

- Event
- Publisher
- Subscriber
- PubSubManager

### PubSubManager:

It will be central corrdinator of system.
it will manage who is subscribed to what topic
For example:

    ORDER_CREATED -> [EmailService, AnalyticsService]
    PAYMENT_SUCCESS -> [NotificationService]

## Design:

### 1. Subscriber

It will be an interface because a publisher should not know which subscriber is recieving msg from him.

```java
interface Subscriber{
    void onEvent(Event event);
}
```

Now any class can become a subscriber
For example:

```java
class EmailSubscriber implements Subscriber{
    @override
    void onEvent(Event event){
        System.out.println("Event received: " + event.getData());
    }
}
```

```java
class AnalyticsService implements Subscriber{
    @override
    void onEvent(Event event){
        System.out.println("Event received: " + event.getData());
    }
}
```

This is basically an Observer Pattern.

### 2. Event

- eventType
- message/Data

```java
class Event{
    private final EventType eventType;
    private final String data;

    public Event(EventType type, String data){
        this.eventType = type;
        this.data = data;
    }

    public String getEventType() {
        return eventType;
    }

    public String getData() {
        return data;
    }
}
```

EventType would be enum

```java
enum EventType {
    ORDER_CREATED,
    ORDER_CANCELLED,
    PAYMENT_SUCCESS
}
```

### 3. Publisher

It's responsibility is to just publish a msg.
It should not maintain the subsciber list itself.

```java
class Publisher{
    private final PubSubManager manager;

    public Publisher(PubSubManager manager){
        this.manager = manager;
    }

    public void publish(Event event){
        manager.publish(event);
    }
}
```

### 4. PubSubManager

Here we maintain a list which subscriber subscribed to which topic

```java
Map<EventType, Set<Subscriber>>
```

why set?
because we don't want that a subscriber should subscribe twice to a same topic.
if EmailService subscribe 2 times to ORDER_CREATED topic it will send notification 2 times.

```java
class PubSubManager{

    private final Map<EventType, Set<Subscriber>> subscribers;

    public PubSubManager(){
        subscribers = new HashMap<>();
    }

    public void subscribe(EventType type, Subscriber subsciber){
        subscribers.computeIfAbsent(type, k-> new HashSet<>())
                    .add(subsciber);
    }

    public void unsubscribe(EventType type, Subscriber subsciber){
        Set<Subscriber> set = subscribers.get(type);
        if(set!=null){
            set.remove(subsciber);
            if(set.isEmpty()){
                subscribers.remove(type);
            }
        }
    }

    public void publish(Event event){
        Set<Subscriber> set = subscribers.get(event.eventType);

        if(set == null) return;

        for(Subscriber sub: set){
            sub.onEvent(event);
        }
    }
}
```

So this is primarly an exmaple of Observer Design Pattern.

To test: 

```java
PubSubManager manager = new PubSubManager();

Publisher publisher = new Publisher(manager);

Subscriber email = new EmailService();
Subscriber analytics = new AnalyticsService();

manager.subscribe(EventType.ORDER_CREATED, email);
manager.subscribe(EventType.ORDER_CREATED, analytics);
```

## Questions:

### 1. Why don't publishers maintain list of subscribers?

because then each publisher has to maintain whom they has to send an event.
and this will not give us loose coupling publishers should publish independent
of subscribers.

### 2. Current system we designed is synchronous.

Publisher
|
v
Subscriber A
|
v
Subscriber B
|
v
return

means if publisher A is slow suppose 5 sec it takes so publisher B
has to wait 5 sec till pub a complete task.

how to resolve then?
To prevent one slow subscriber from blocking the publisher,
I would make delivery asynchronous using a thread pool or a durable message broker such as Kafka.

code will be like this with threadpool executor

```java
private final ExecutorService executor = Executors.newFixedThreadPool(10);
public void publish(Event event) {
    Set<Subscriber> set = subscribers.get(event.getType());
    if (set == null) return;

    for (Subscriber subscriber : set) {
        executor.submit(() -> {
            try {
                subscriber.onEvent(event);
            } catch (Exception e) {
                System.out.println(
                    "Subscriber failed: " + e.getMessage()
                );
            }
        });
    }
}
```

```text
                Thread Pool
              /       |       \                
         Thread-1  Thread-2  Thread-3
             |        |        |
          Email    Analytics Notification
```

### 3. Thread safe

Suppose thread t1 subscribe() t2 unsubscribe() it and t3 at same time publish();
So instead of using a simple HashMap we can maintain a concurrentHashMap which is thread safe.

```java
public void subscribe(EventType type, Subscriber subscriber) {
    subscribers
        .computeIfAbsent(
            type,
            k -> ConcurrentHashMap.newKeySet()
        )
        .add(subscriber);
}
```

### 4. Design Improvements

in our publish function we r iterating over all subscribers,
suppose any one of them throws exception so our publish
method will recieve exception and function will be stopped for all subscriber
so better to implement try catch blocking

```java
for (Subscriber subscriber : set) {
    try {
        subscriber.onEvent(event);
    } catch (Exception e) {
        // log failure
    }
}
```

### 5. But there's an important problem here

Suppose subscriber processing takes:
5 seconds
and 1000 events come in.

Our executor queue can grow:

```text
Event
  |
Thread Pool
  |
[Task][Task][Task][Task][Task]...
```

Eventually memory/backlog becomes a problem.
So a better production design could use: KAfka like architecture

```text
Publisher
    |
    v
Event Queue / Kafka
    |
    +--------> Consumer A → Email
    |
    +--------> Consumer B → Analytics
    |
    +--------> Consumer C → Notification
```
