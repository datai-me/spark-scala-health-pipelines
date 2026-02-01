Write-Host "======================================"
Write-Host " ENVIRONMENT VERSIONS (PowerShell)"
Write-Host "======================================"

Write-Host "`nJava:"
java -version

Write-Host "`nSBT:"
sbt --version

Write-Host "`nScala:"
scala -version

Write-Host "`nSpark:"
spark-submit --version

Write-Host "`nHadoop:"
hadoop version

Write-Host "`nZookeeper:"
zookeeper-server.cmd version
