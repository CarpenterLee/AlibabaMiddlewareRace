#!/usr/bin/env bash
/home/gjl_workspace/lh_workspace/FlameGraph-master/stackcollapse-ljp.awk < traces.txt | /home/gjl_workspace/lh_workspace/FlameGraph-master/flamegraph.pl > traces.svg
