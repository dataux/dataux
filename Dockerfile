FROM golang:1.7
MAINTAINER Aaron Raddon <araddon@gmail.com>

ADD dataux /dataux
ENTRYPOINT ["/dataux"]