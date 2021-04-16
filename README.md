## Instructions for Re-installing Ubuntu

0. Ubuntu installation will override all the data previously stored. Backup:
   a. SSH keys,
   b. Mozilla history,
   c. Any data that was not backed up.
1. Create a bootup disk for the version of Ubuntu that we want to install.
2. Enter the BIOS (for Lenovo, push F2 during bootup).
3. Inside the BIOS, select the Boot procedure (FEIC to boot from the
bootup USB).
4. Exit the BIOS, saving the changes.
5. When the OS boots up, select the option to install Ubuntu.
6. Ubuntu installation will override all the data previously stored.


### Instructions for installing Postgres:
```
sudo apt -y install vim bash-completion wget
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
RELEASE=$(lsb_release -cs)
echo "deb http://apt.postgresql.org/pub/repos/apt/ ${RELEASE}"-pgdg main | sudo tee  /etc/apt/sources.list.d/pgdg.list
cat /etc/apt/sources.list.d/pgdg.list
sudo apt update
sudo apt install postgresql-13 postgresql-client-13
systemctl status postgresql.service
systemctl status postgresql@13-main.service 
sudo su - postgres
sudo -u postgres createuser -P -s -e cma
```

Install the Postgres development library:
```
sudo apt-get install -y libpq-dev
```

Use `pg_config` to find out where the include and library files are:
```
$ pg_config --includedir
/usr/include/postgresql
$ pg_config --libdir
/usr/lib/x86_64-linux-gnu
```

Use this command line to compile C files using postgres:
```
$ gcc -o file_name file_name.c -I/usr/include/postgresql -lpq -std=c99
```

### Installing Postgres for Mac

```
brew doctor
brew update
brew install postgresql
```

This will install the latest version of postgresql, which is what I
want.  Also, there is no need to install libpq-dev, as it is
automatically installed.


### Installing Java:
```
sudo apt update
sudo apt install -y openjdk-8-jdk
echo -e "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/" >> $HOME/.bashrc
echo -e "export PATH=PATH:$JAVA_HOME/bin" >> $HOME/.bashrc
```

### Setting up the Python workspace:

Python3 is already installed on ubuntu 18.04.  So let's move to Python3.
Need to install the following: pip, virtualenv, autoenv

```
sudo apt install -y python3-pip
pip3 install virtualenv
pip3 install virtualenv virtualenvwrapper autoenv
pip3 install rope jedi importmagic autopep8 flake8
mkdir -p ${HOME}/.envs
echo "export VIRTUALENVWRAPPER_PYTHON=/usr/bin/python3" >> $/u{HOME}/.bashrc
echo "export WORKON_HOME=${HOME}/.envs" >> $/u{HOME}/.bashrc
echo "source ${HOME}/.local/bin/virtualenvwrapper.sh" >> ${HOME}/.bashrc
echo "source ${HOME}/.local/bin/activate.sh" >> ${HOME}/.bashrc
```

### Create an Environment file in the home directory

Create file `${HOME}/.env`.  This will be used by autoenv when it is
installed. Add the following variables in the file:

```
export POSTGRES_USER=...
export POSTGRES_PASSWORD=...
export POSTGRES_DB=...
export POSTGRES_CNX="user=${POSTGRES_USER} dbname=${POSTGRES_DB}"
export DB_URL="user=${POSTGRES_USER} dbname=${POSTGRES_DB}"
export NORGATE_DIR=...
export DATA_DIR=...
export DOWNLOAD_DIR=...
```

### Create a Postgres database, using a script command

```
sudo -u postgres createdb ${POSTGRES_DB} --owner ${POSTGRES_USER}
```

### Create the database tables and create indexes

```
cd ${HOME}/c
./compile.sh create_tables.c
./create_tables.exe
```

### Download and install the metastock code

Installation of the code:

```
git clone git@github.com:rudimeier/atem.git
cd atem/
sudo apt install autoconf gengetopt help2man
autoreconf -vfi
./configure 
make
sudo make install
```

Installation on Mac:

```
git clone git@github.com:rudimeier/atem.git
cd atem/
brew install autoconf gengetopt help2man automake libtool
autoreconf -vfi
./configure 
make
sudo make install
```

### Unzipping the norgate files

To unzip the norgate files, install 7z:

```
sudo apt-get install p7zip-full p7zip-rar
```

Then, copy the files from the media, and unzip them using 7z:

```
mkdir -p ${HOME}/norgate
cd ${HOME}/norgate
cp /media/.../InstallHistory.U* .
7z x InstallHistory.US.1985.20180309.exe 
7z x InstallHistory.UsDelisted.1985.20180302.exe 
```

### Unzipping the norgate files on Mac

Install keka, from here: https://www.keka.io/en/ Then drag and drop
the norgate archives into keka.

Keka extracts the files in all kinds of places.  Once extraction is
complete, make sure that the input directories mentioned in stx_ng,py
are under the norgate directory.

### Create a Python virtual environment

Create the virtual environment
```
mkvirtualenv venv-py
```

Then create `${HOME}/python/.env` file, and write workon directive in
it:
```
echo -e "workon venv-py" >> ${HOME}/python/.env
```

Finally, install the necessary packages, from here:
```
pip install -r requirements.txt
```


### Insert stocks and options data in the database

First, insert the norgate data.  Then the options data.  Finally, the
stooq data and the eoddata:
```
python stx_ng.py
python opteod.py
python stx_eod.py --stooq
python stx_eod.py --batch
```

### Generate a calendar and save it in the database

Run this file without any arguments:
```
python stxcal.py
```

This will generate a calendar from `1984-12-31` until `2025-12-31`,
and save it in the database.  To change the default values for the
`start_date` and `end_date`, edit the following line in the
`stxcal.py` code:

```
def gen_cal(start_date='1984-12-31', end_date='2025-12-31'):
```

### Curlib and cJSON (to retrieve quotes and parse them)
`curlib` is already installed on Mac.
To install `cJSON`, clone the repository, and follow the instructions:
```
git clone git@github.com:DaveGamble/cJSON.git
cd cJSON/
mkdir build
cd build
cmake ..
make
sudo make install
```

### Restart Postgresql Server 

If, after a machine restart, the postgresql server does not come back
up automatically, remove the dangling process id with the following:

```
rm -f /usr/local/var/postgres/postmaster.pid
```

### Upgrading the Postgres Server 

After upgrading the Postgres server, the following command must run,
for the database files to become compatible again with the server:
```
brew postgresql-upgrade-database
```

### Install WeasyPrint

First, might need to install these packages:
```
brew install python3 cairo pango gdk-pixbuf libffi
```

Then install WeasyPrint, as usual (cannot run with Python2; need
Python3 and Pip3).

```
pip3 install WeasyPrint
```