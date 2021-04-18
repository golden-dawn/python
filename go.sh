#!/bin/bash

# TODO:
# 1. The script does not exit if the parameters are not setup
# 2. Python virtual environment is not installed properly:

# virtualenvwrapper.sh: There was a problem running the initialization hooks.
#
# If Python could not import the module virtualenvwrapper.hook_loader,
# check that virtualenvwrapper has been installed for
# VIRTUALENVWRAPPER_PYTHON= and that PATH is
# set properly.
# 3. Add the postgres database instantiation procedures
# 4. Ensure the environment variables file has all the required variables


# Remember the current directory to return at the end of script execution
CRT_DIR=$(dirname $0)
cd "${CRT_DIR}" && CRT_DIR=$PWD

# Print script usage
USAGE="\nUsage: $0 [--env-vars-file /path/to/env_vars_file] [--postgres-version 13]\n"

# Parse the input arguments
ops='env-vars-file:,postgres-version:'
declare {ENV_VARS_FILE,POSTGRES_VERSION}=''
OPTIONS=$(getopt --options '' --longoptions ${ops} --name "$0" -- "$@")
[[ $? != 0 ]] && exit 3

eval set -- "${OPTIONS}"

while true
do
    case "${1}" in
	--env-vars-file)
	    ENV_VARS_FILE="$2"
	    shift 2
	    ;;
	--postgres-version)
	    POSTGRES_VERSION="$2"
	    shift 2
	    ;;
	--)
	    shift
	    break
	    ;;
	*)
	    echo -e "\n\nUndefined options given!"
	    echo "$*"
	    echo -e "${USAGE}"
	    exit 3
	    ;;
    esac
done

# Check that environment variables file and Postgres version was specified
[[ "${ENV_VARS_FILE}" == '' ]] && (echo -e "\nError: no environment variables file specified! Check script usage.\n${USAGE}" && exit 1)
[[ "${POSTGRES_VERSION}" == '' ]] && (echo -e "\nError: no postgres version specified! Check script usage.\n${USAGE}" && exit 1)

echo -e "Running script with the following parameters:"
echo -e "  Environment variables file: ${ENV_VARS_FILE}"
echo -e "  Postgres version: ${POSTGRES_VERSION}"

# Install python development tools
sudo apt update
sudo apt install -y apt-transport-https ca-certificates curl software-properties-common
sudo apt install -y git python3-pip python3-dev build-essential emacs libssl-dev libffi-dev
sudo apt -y autoremove

# Install virtualenv, autoenv, and emacs python IDE packages
sudo pip3 install virtualenv virtualenvwrapper autoenv
sudo pip3 install rope jedi importmagic autopep8 flake8
mkdir -p ${HOME}/.envs
echo 'export WORKON_HOME=${HOME}/.envs' >> ${HOME}/.bashrc
echo 'source /usr/local/bin/virtualenvwrapper.sh' >> ${HOME}/.bashrc
echo 'source /usr/local/bin/activate.sh' >> ${HOME}/.bashrc

# Read the environment variables from file.  TODO: check that all the
# environment variables that are needed have been specified
source ${ENV_VARS_FILE}

# configure git global email and name
git config --global user.name "${GIT_USERNAME}"
git config --global user.email "${GIT_EMAIL}"

# Install Postgres
sudo apt -y install vim bash-completion wget
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
RELEASE=$(lsb_release -cs)
echo "deb http://apt.postgresql.org/pub/repos/apt/ ${RELEASE}"-pgdg main | sudo tee  /etc/apt/sources.list.d/pgdg.list
cat /etc/apt/sources.list.d/pgdg.list
sudo apt update
sudo apt install -y postgresql-${POSTGRES_VERSION} postgresql-client-${POSTGRES_VERSION}
systemctl status postgresql.service
systemctl status postgresql@${POSTGRES_VERSION}-main.service 

# after postgresql is installed, use this command to create a new superuser:
# sudo -u postgres createuser -P -s -e z2z2z2
# also, create the database
# sudo -u postgres createdb stx
