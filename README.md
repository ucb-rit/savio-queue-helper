# Savio Queue Helper

This tool augments the output of squeue with additional information about the state of pending jobs and explains clearly why jobs are waiting.

[![asciicast](https://asciinema.org/a/367157.svg)](https://asciinema.org/a/367157)

## Features
- Display pending jobs nicely with additional details
- Identify and recommend fixes to common problems
  - QOS limits (group, per-job, CPU, node, etc.)
  - Jobs intersect with reservations
  - Job ran but exited quickly

## Usage
Command line options available using `-h`

## Building
```bash
pip install -r requirements.txt
pyinstaller --onefile sq.py
```

Then the output binary is located at: `dist/sq`

