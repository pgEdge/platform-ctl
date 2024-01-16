#!/bin/bash

isEL8="False"
if [ -f /etc/os-release ]; then
  PLATFORM=`cat /etc/os-release | grep PLATFORM_ID | cut -d: -f2 | tr -d '\"'`
  if [ "$PLATFORM" == "el8" ]; then
    isEL8="True"
  fi
fi

function install_pgedge {
  ./ctl install $pgV; 
  ./ctl start $pgV -y -d demo;
  ./ctl install spock32-$pgV       -d demo
  ./ctl install snowflake-$pgV     -d demo
  ./ctl install readonly-$pgV      -d demo
}


## extensions common to pg15 & pg16
function test_common_exts {

  if [ "$isEL8" == "True" ]; then
    return
  fi

  ./ctl install hypopg-$pgV        -d demo
  ./ctl install orafce-$pgV        -d demo
  ./ctl install curl-$pgV          -d demo
  ./ctl install cron-$pgV
  ./ctl install partman-$pgV       -d demo
  ./ctl install postgis-$pgV       -d demo
  ./ctl install vector-$pgV        -d demo
  ./ctl install audit-$pgV         -d demo
  ./ctl install hintplan-$pgV      -d demo
  ./ctl install timescaledb-$pgV   -d demo

  #./ctl install plv8-$pgV          -d demo
  #./ctl install pljava-$pgV        -d demo

  ## extensions that dont always play nice with others
  # ./ctl install plprofiler-$pgV
  # ./ctl install pldebugger-$pgV    -d demo
  # ./ctl install citus-$pgV         -d demo
}


function test16 {
  install_pgedge

  test_common_exts
}


function test15 {
  install_pgedge
  ./ctl install foslots-$pgV       -d demo

  test_common_exts

  #./ctl install decoderbufs-$pgV   -d demo
  #./ctl install mysqlfdw-$pgV      -d demo
  #./ctl install mongofdw-$pgV      -d demo
  #./ctl install oraclefdw-$pgV     -d demo
  #./ctl install esfdw-$pgV         -d demo
  #./ctl install multicorn2-$pgV    -d demo
}


function test14 {
  install_pgedge
  ./ctl install foslots-$pgV       -d demo
}


cd ../..
pgV="pg$1"

if [ "$pgV" == "pg16" ]; then
  test16
  exit 0
fi

if [ "$pgV" == "pg15" ]; then
  test15
  exit 0
fi

if [ "$pgV" == "pg14" ]; then
  test14
  exit 0
fi

echo "ERROR: Invalid parm, must be '14', '15' or '16'"
exit 1

