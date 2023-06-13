#!/bin/bash

start_dir="$PWD"

# resolve links - $0 may be a softlink
this="${BASH_SOURCE-$0}"
common_bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$common_bin/$script"
# convert relative path to absolute path
config_bin=`dirname "$this"`
script=`basename "$this"`
my_home=`cd "$config_bin"; pwd`

export MY_HOME="$my_home"
export MY_LOGS="$my_home/logs/cli_log.out"
export MY_CMD="nc"

cd "$MY_HOME"

hub_new="$MY_HOME/hub_new"
if [ -d "$hub_new" ];then
  `mv $MY_HOME/hub_new $MY_HOME/hub_upgrade`
  log_time=`date +"%Y-%m-%d %H:%M:%S"`
  echo "$log_time [INFO] : completing hub upgrade" >> $MY_LOGS
  `mv $MY_HOME/hub $MY_HOME/hub_old`
  `cp -rf $MY_HOME/hub_upgrade/* $MY_HOME/`
  `rm -rf $MY_HOME/hub_upgrade`
  `rm -rf $MY_HOME/hub_old`
  log_time=`date +"%Y-%m-%d %H:%M:%S"`
  echo "$log_time [INFO] : hub upgrade completed" >> $MY_LOGS
fi

plat=amd
if [ `arch` == "aarch64" ]; then
  plat=arm
fi

declare -a array
array[0]="$MY_HOME/hub/scripts"
array[1]="$MY_HOME/hub/scripts/lib"

plat=amd
if [ `arch` == "aarch64" ]; then
  plat=arm
fi

if [ `uname` == "Linux" ]; then
  if [ -f "/etc/redhat-release" ]; then
    if [ `arch` == "aarch64" ]; then
      array[2]="$MY_HOME/hub/scripts/lib/linux/el9/arm"
    else
      array[2]="$MY_HOME/hub/scripts/lib/linux/el9/amd"
    fi
  else
    if [ -f "/etc/os-release" ] && [ `arch` == "x86_64" ]; then
      grep "22.04" /etc/os-release > /dev/null 2>&1
      rc=$?
      if [ $rc == "0" ]; then
        array[2]="$MY_HOME/hub/scripts/lib/linux/ubu22/amd"
      fi
    fi
  fi
fi

export PYTHONPATH=$(printf "%s:${PYTHONPATH}" ${array[@]})
echo PYTHONPATH=$PYTHONPATH
for var in "$@"
do
  if [ "$var" == "-v" ]; then
    echo "PYTHONPATH=$PYTHONPATH"
  fi
done

pyver=`python3 --version > /dev/null 2>&1`
rc=$?
if [ $rc == 0 ];then
  export PYTHON=python3
else
  echo "ERROR: missing python3"
  exit 1
fi

$PYTHON -u "$MY_HOME/hub/scripts/cli.py" "$@"
