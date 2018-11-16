#!/bin/bash

if [ $# -ne 1 ]
then
    echo "Error : Expect 1 parameters: day_time:%Y%m%d%H"
    exit 1
fi
cd ../ && tar zcvf script.tar.gz script
cp script.tar.gz ./bin && cd -
if [ $? -eq 0 ]
then
    echo "pack tarball succeed"
else
    echo "Error : pack tarball failed"
    exit 1
fi
cp ../script/multiOutputClass/multiOutput.jar ./
if [ $? -eq 0 ]
then
    echo "copy jarball succeed"
else
    echo "Error : copy jarball failed"
    exit 1
fi
day_time=$1
sh start_etl_event.sh ${day_time}
