#!/bin/sh
docker run -ti -p 9997:9997 -p 9998:9998 -v ~/src/github.com/tgulacsi:/go/src/github.com/tgulacsi tgulacsi/bleve /bin/bash
