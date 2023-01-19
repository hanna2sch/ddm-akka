# ddm-akka
Akka example and homework code for the "Distributed Data Management" lecture.

## Compiling & running the system

Use Maven to build the project:
```sh
mvn package -f "pom.xml"
```

Start Master Process in Terminal:
```sh
java -jar target/ddm-akka-1.0.jar master -h localhost
```

Start Worker Process in seperate Terminal:
```sh
java -jar target/ddm-akka-1.0.jar worker -mh localhost
```

Once the Program is finished and all INDs are collected, please terminate all running processes manually
