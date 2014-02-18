#!/bin/bash

if [ "X$CRAB3_CHECKOUT" = "X" ]; then

export CRAB3_CHECKOUT=~/projects

fi

export CRAB3_TEST_BASE=$CRAB3_CHECKOUT/CAFTaskWorker

export PYTHONPATH=$CRAB3_CHECKOUT/CAFTaskWorker/src/python:$CRAB3_CHECKOUT/WMCore/src/python:$CRAB3_CHECKOUT/CAFUtilities/src/python:$CRAB3_CHECKOUT/DLS/Client/LFCClient:$CRAB3_CHECKOUT/DBS/Clients/Python
export PYTHONPATH=$CRAB3_CHECKOUT/CAFTaskWorker/test/python:$PYTHONPATH
export PYTHONPATH=$CRAB3_CHECKOUT/AsyncStageout/src/python:$PYTHONPATH
export PYTHONPATH=$CRAB3_CHECKOUT/DBS/Client/src/python:$CRAB3_CHECKOUT/DBS/PycurlClient/src/python:$PYTHONPATH
export LD_LIBRARY_PATH=$CRAB3_CHECKOUT/AsyncStageout/src/python:$LD_LIBRARY_PATH

exec python2.6 "$@"
