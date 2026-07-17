@echo off

call conda activate aranha

start "SERVER" cmd /k "call conda activate aranha && python -m main_beta4 server --host 127.0.0.1 --port 9000 --fragments 10 --stop-when-complete"

timeout /t 3 /nobreak >nul

start "PEER 9101" cmd /k "call conda activate aranha && python -m main_beta4 peer --host 127.0.0.1 --port 9101 --server-host 127.0.0.1 --server-port 9000 --reset-storage"
start "PEER 9102" cmd /k "call conda activate aranha && python -m main_beta4 peer --host 127.0.0.1 --port 9102 --server-host 127.0.0.1 --server-port 9000 --reset-storage"
start "PEER 9103" cmd /k "call conda activate aranha && python -m main_beta4 peer --host 127.0.0.1 --port 9103 --server-host 127.0.0.1 --server-port 9000 --reset-storage"
start "PEER 9104" cmd /k "call conda activate aranha && python -m main_beta4 peer --host 127.0.0.1 --port 9104 --server-host 127.0.0.1 --server-port 9000 --reset-storage"
start "PEER 9105" cmd /k "call conda activate aranha && python -m main_beta4 peer --host 127.0.0.1 --port 9105 --server-host 127.0.0.1 --server-port 9000 --reset-storage"