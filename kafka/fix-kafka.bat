@echo off
echo ========================================
echo    Kafka 彻底修复脚本
echo ========================================
echo.

echo 步骤1：停止所有 Java 进程（包括 Kafka 和 Zookeeper）
echo 正在查找相关进程...
tasklist | findstr "java.exe"
echo.
echo 是否强制关闭所有 Java 进程？(y/n)
set /p choice=
if /i "%choice%"=="y" (
    taskkill /F /IM java.exe
    echo 已关闭所有 Java 进程
)

echo.
echo 步骤2：删除所有 Kafka 相关日志目录
echo 正在删除 F:\tmp\kafka-logs...
if exist "F:\tmp\kafka-logs" (
    rmdir /s /q F:\tmp\kafka-logs
    echo 已删除 F:\tmp\kafka-logs
) else (
    echo F:\tmp\kafka-logs 不存在，跳过
)

echo 正在删除 C:\tmp\kafka-logs...
if exist "C:\tmp\kafka-logs" (
    rmdir /s /q C:\tmp\kafka-logs
    echo 已删除 C:\tmp\kafka-logs
)

echo 正在删除 C:\kafka\kafka-logs...
if exist "C:\kafka\kafka-logs" (
    rmdir /s /q C:\kafka\kafka-logs
    echo 已删除 C:\kafka\kafka-logs
)

echo.
echo 步骤3：创建新的日志目录
mkdir F:\tmp\kafka-logs
echo 已创建 F:\tmp\kafka-logs

echo.
echo 步骤4：修改 Kafka 配置文件
echo 正在修改 server.properties...

REM 备份原配置文件
copy C:\kafka\config\server.properties C:\kafka\config\server.properties.bak

REM 使用 PowerShell 修改配置文件
powershell -Command "(Get-Content C:\kafka\config\server.properties) -replace 'log.dirs=.*', 'log.dirs=F:/tmp/kafka-logs' | Set-Content C:\kafka\config\server.properties"

echo 配置文件已更新

echo.
echo ========================================
echo   修复完成！
echo.
echo   现在请重新启动 Kafka：
echo   1. 启动 Zookeeper
echo      cd C:\kafka
echo      .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
echo.
echo   2. 启动 Kafka（新窗口）
echo      cd C:\kafka
echo      .\bin\windows\kafka-server-start.bat .\config\server.properties
echo ========================================
echo.
pause