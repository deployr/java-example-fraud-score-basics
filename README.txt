There are two example applications provided in this bundle. Both example depend on you having access to a live instance of the DeployR server. By default, the build.gradle configuration for these applications assume that your DeployR server is running on localhost at port 7400.

The source code for both of these example applications is heavily annotated with explanations of each of the steps taken in the code.


1. LoadStrategyInitBrokerExecuteTask.java

To run this example type the following at the command prompt:

gradlew run -DtestClass=com.revo.deployr.tutorial.LoadStrategyInitBrokerExecuteTask


2. CacheStrategyInitBrokerExecuteTask.java

To run this example type the following at the command prompt:

gradlew run -DtestClass=com.revo.deployr.tutorial.CacheStrategyInitBrokerExecuteTask
