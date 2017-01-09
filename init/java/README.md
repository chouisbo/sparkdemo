
```zsh

mvn archetype:generate -DgroupId=com.ict.golaxy -DartifactId=sparkdemo -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false

mvn clean compile package

spark-submit --class com.ict.golaxy.App ./target/sparkdemo-1.0-SNAPSHOT.jar

```
