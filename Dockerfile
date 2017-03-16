FROM golang:1.7
MAINTAINER Aaron Raddon <araddon@gmail.com>

ADD backends/files/tables/tables/appearances/appearances.csv /vol/baseball/appearances.csv

ADD dataux.container.conf /etc/dataux.conf

ADD dataux /dataux
ENTRYPOINT ["/dataux","--config=/etc/dataux.conf"]