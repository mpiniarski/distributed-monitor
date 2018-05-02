# Distributed monitor - synchronization mechanism for distributed systems

Repository contains library itself and example, where it is used to sychronize procuder and consumer processes

## Build library

```
cd library
./gradlew shadowJar
cd ..
```
Compiled library with all it's dependencies can now be found in `library/build/libs/distributed-monitor-*-all.jar`

## Run example

You need to have kotlin installed.
To compile example:
```
cd example
./gradlew shadowJar
cd ..

kotlinc src/Producer.kt -classpath ../library/build/libs/distributed-monitor-1.0.0-all.jar
kotlinc src/Consumer.kt -classpath ../library/build/libs/distributed-monitor-1.0.0-all.jar
```
To run example
```
kotlin -classpath .:../library/build/libs/distributed-monitor-1.0.0-all.jar ProducerKt
kotlin -classpath .:../library/build/libs/distributed-monitor-1.0.0-all.jar ConsumerKt
```

