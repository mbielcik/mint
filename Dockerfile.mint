FROM ubuntu:18.04

ENV DEBIAN_FRONTEND noninteractive
ENV LANG C.UTF-8
ENV GOROOT /usr/local/go
ENV GOPATH /usr/local/gopath
ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH
ENV MINT_ROOT_DIR /mint

RUN apt-get --yes update && apt-get --yes upgrade && \
    apt-get --yes --quiet install wget jq curl git dnsmasq

# Invalidate cache when newer commit is available.
# https://stackoverflow.com/questions/36996046/how-to-prevent-dockerfile-caching-git-clone
ADD https://api.github.com/repos/iternity-dotcom/mint/git/refs/heads/updated-suite version.json

RUN git clone https://github.com/iternity-dotcom/mint && \
    cd /mint && git checkout --quiet "updated-suite" && /mint/release.sh

WORKDIR /mint

ENTRYPOINT ["/mint/entrypoint.sh"]
