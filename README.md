# Savio Queue Helper

This tool augments the output of squeue with additional information about the state of pending jobs and explains clearly why jobs are waiting.

## Building
```bash
pip install -r requirements.txt
pyinstaller --onefile sq.py
```

Then the output binary is located at: `dist/sq`
