#!/bin/sh

echo "Please configure DB hosts and credentials carefully..."
sleep 3
cp lib/db_config.sample.php lib/db_config.php
nano lib/db_config.php

curl https://jpgraph.net/download/download.php?p=57 --output jpgraph-4.4.2.tar.gz
tar -xvf jpgraph-4.4.2.tar.gz
