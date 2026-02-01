@echo off
echo ======================================
echo   ENVIRONMENT VERSIONS (WINDOWS)
echo ======================================

echo Java:
java -version
echo.

echo SBT:
sbt --version
echo.

echo Scala:
scala -version
echo.

echo Spark:
spark-submit --version
echo.

echo Hadoop:
hadoop version
echo.

echo Zookeeper:
zookeeper-server.cmd version
echo.

pause
