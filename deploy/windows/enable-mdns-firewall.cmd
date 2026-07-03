@echo off
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0enable-mdns-firewall.ps1" %*
