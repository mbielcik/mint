#!/bin/bash -e
#
#  Mint (C) 2017-2022 Minio, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

source "${MINT_ROOT_DIR}"/source.sh

# install nodejs source list
if ! $WGET --output-document=- https://deb.nodesource.com/setup_20.x | bash -; then
	echo "unable to set nodejs repository"
	exit 1
fi

$APT install apt-transport-https

if ! $WGET --output-document=packages-microsoft-prod.deb https://packages.microsoft.com/config/ubuntu/22.04/packages-microsoft-prod.deb | bash -; then
	echo "unable to download dotnet packages"
	exit 1
fi

dpkg -i packages-microsoft-prod.deb
rm -f packages-microsoft-prod.deb

$APT update
$APT install gnupg ca-certificates unzip busybox

# download and install golang
download_url="https://storage.googleapis.com/golang/go${GO_VERSION}.linux-amd64.tar.gz"
if ! $WGET --output-document=- "$download_url" | tar -C "${GO_INSTALL_PATH}" -zxf -; then
	echo "unable to install go$GO_VERSION"
	exit 1
fi

xargs --arg-file="${MINT_ROOT_DIR}/install-packages.list" apt --quiet --yes install

# set python 3.10 as default
update-alternatives --install /usr/bin/python python /usr/bin/python3.10 1

mkdir -p ${GRADLE_INSTALL_PATH}
gradle_url="https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip"
if ! $WGET --output-document=- "$gradle_url" | busybox unzip -qq -d ${GRADLE_INSTALL_PATH} -; then
	echo "unable to install gradle-${GRADLE_VERSION}"
	exit 1
fi

chmod +x -v ${GRADLE_INSTALL_PATH}/gradle-${GRADLE_VERSION}/bin/gradle

sync
