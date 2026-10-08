@echo off
echo Starting Autonomous Synthetic Data Factory (7-Day Agile Day-Router)...
echo ======================================================================

:: 1. Directory Context Enforcement
cd /d "%~dp0"

:: 2. Environment Variable Inheritance
set HF_USERNAME=Posiedon26
set HF_WRITE_TOKEN=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

echo Workspace Locked: %CD%
echo Authenticated HF User: %HF_USERNAME%
echo ======================================================================

:: Phase 1: Initialize Database Verification
echo [1/5] Initializing Database (WAL Mode)...
python database\init_db.py
if %errorlevel% neq 0 (
    echo [ERROR] Database initialization failed! 
    pause
    exit /b %errorlevel%
)
ping 127.0.0.1 -n 2 > nul

:: Phase 2: Launch System Manager
echo [2/5] Starting SRE System Manager...
start "SRE System Manager" cmd /k "python core\sys_manager.py"
ping 127.0.0.1 -n 3 > nul

:: Phase 3: Launch Data Generator
echo [3/5] Starting High-Performance Synthetic Generator...
start "Data Generator" cmd /k "python core\generator.py"
ping 127.0.0.1 -n 3 > nul

:: Phase 4: Launch HF Streamer
echo [4/5] Starting Hugging Face Streamer...
start "HF Streamer" cmd /k "python core\hf_streamer.py"
ping 127.0.0.1 -n 3 > nul

:: Phase 5: Launch Streamlit Dashboard
echo [5/5] Starting Real-Time Analytics Dashboard...
start "Telemetry Console" cmd /k "streamlit run ui\dashboard.py"

echo.
echo ======================================================================
echo ALL SYSTEMS GO!
echo The autonomous data factory is now active in persistent diagnostic mode.
echo (All execution shells are pinned open via cmd /k for live error auditing)
echo ======================================================================
pause
