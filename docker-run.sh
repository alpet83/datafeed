#!/bin/sh

JPGRAPH=jpgraph-4.4.2

mkdir -p src/logs
docker rm dfsc
ln -sf ../lib server/lib
ln -sf ../$JPGRAPH/src server/jpgraph
ln -sf ../logs server/logs
docker run --name dfsc --net=host \
        -v ./server:/var/www/html:ro \
        -v ./lib:/var/www/lib:ro \
        -v ./lib:/datafeed/lib:ro \
        -v ./$JPGRAPH:/var/www/$JPGRAPH \
        -v ./alpet-libs-php:/usr/local/lib/php/lib:ro \
        -v ./src:/datafeed/src \
        -v ./src:/datafeed/sql \
        -it -t datafeed $1
