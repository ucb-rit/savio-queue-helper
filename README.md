# `sq`: The Savio Queue Helper

This tool augments the output of squeue with additional information about the state of pending jobs and explains clearly why jobs are waiting.

[![asciicast](https://asciinema.org/a/372014.svg)](https://asciinema.org/a/372014)

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
conda create -n sq python=3.8
conda activate sq
pip install -r requirements.txt
pyinstaller --onefile sq.py
```

Then the output binary is located at: `dist/sq`

## Debugging

The most common problem is that the tool encounters output from a Slurm command (e.g., `squeue`, `sinfo`) that it can't parse.

A couple approaches to debugging are:

1) You can clone this repo (presumably into the Savio filesystem) and then run  `python -m pdb sq.py` manually (including inserting `pdb` commands and modifying code in `sq.py`).

2) You can add `--freeze $DIRNAME`, and `sq` will create a new directory at `$DIRNAME` containing all the Slurm command outputs. You can then use the saved files to debug in the future with `--load $DIRNAME`.
