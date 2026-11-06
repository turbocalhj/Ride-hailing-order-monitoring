@echo off

echo 正在创建 Kafka Topic...

REM 创建订单主题，4个分区
bin\windows\kafka-topics.bat --create --topic ride-sharing-orders --bootstrap-server localhost:9092 --partitions 4

REM 创建处理后主题，4个分区
bin\windows\kafka-topics.bat --create --topic processed-orders --bootstrap-server localhost:9092 --partitions 4

REM 创建异常订单主题，4个分区
bin\windows\kafka-topics.bat --create --topic anomaly-orders --bootstrap-server localhost:9092 --partitions 4

echo.
echo 查看已创建的 Topic 列表：
bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092

echo.
echo Topic 创建完成！
pause