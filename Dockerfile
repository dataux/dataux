FROM scratch
MAINTAINER Aaron Raddon <araddon@gmail.com>

ADD dataux /dataux
ENTRYPOINT ["/dataux"]