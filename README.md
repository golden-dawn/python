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
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
RELEASE=$(lsb_release -cs)
echo "deb http://apt.postgresql.org/pub/repos/apt/ ${RELEASE}"-pgdg main | sudo tee  /etc/apt/sources.list.d/pgdg.list
cat /etc/apt/sources.list.d/pgdg.list
sudo apt update
sudo apt -y install postgresql-11
sudo -u postgres createuser -P -s -e cma
```

### Installing Java:
```
sudo apt update
sudo apt install openjdk-8-jdk
echo -e "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/" >> $HOME/.bashrc
echo -e "export PATH=PATH:$JAVA_HOME/bin" >> $HOME/.bashrc
```

