FROM golang:latest
MAINTAINER Tamás Gulácsi

RUN DPKG_FRONTEND=noninteractive apt-get -y update
RUN DPKG_FRONTEND=noninteractive apt-get -y upgrade


## bleve

RUN DPKG_FRONTEND=noninteractive apt-get -y install libleveldb-dev libstemmer-dev libicu-dev build-essential subversion
#RUN DPKG_FRONTEND=noninteractive apt-get -y install default-jre-headless
RUN go get -u -v  github.com/blevesearch/bleve
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*


RUN cd $GOPATH/src/github.com/blevesearch/bleve/analysis/token_filters/cld2 && \
	svn co http://cld2.googlecode.com/svn/trunk cld2-read-only

RUN cd $GOPATH/src/github.com/blevesearch/bleve/analysis/token_filters/cld2 && \
	cd cld2-read-only/internal/ && \
	./compile_libs.sh && \
	cp *.so /usr/local/lib && \
	ldconfig

RUN go get -u -v -tags full github.com/blevesearch/bleve && \
	go install -v -tags full github.com/blevesearch/bleve/utils/...


## Apache Tika

RUN mkdir -p /usr/local/share/java && \
	curl -o /usr/local/share/java/tika-server.jar http://xenia.sote.hu/ftp/mirrors/www.apache.org/tika/tika-server-1.6.jar
RUN DPKG_FRONTEND=noninteractive apt-get update && \
	DPKG_FRONTEND=noninteractive apt-get -y install default-jre-headless
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*


## bleve-indexer
RUN go get -u -v github.com/tgulacsi/bleve-indexer

ENTRYPOINT ["$GOPATH/bin/bleve-indexer"]
