#!/bin/bash
# Script to start the Epstein Document Search Web UI

cd "$(dirname "$0")"
source epstein_env/bin/activate
python text_search_webui.py