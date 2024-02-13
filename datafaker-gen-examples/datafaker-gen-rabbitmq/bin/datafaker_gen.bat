@echo off
:: datafaker_gen.bat - Windows script to launch datafaker_gen
java -cp "%~dp0\..\target\*" net.datafaker.datafaker_gen.DatafakerGen %*

:: End datafaker_gen.bat
