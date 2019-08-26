#!/usr/bin/ksh

#Set BASE_DIR
export BASE_DIR="/teoco/helix/"

#set the netrac environment
source $BASE_DIR/integration/scripts/env/main_env.ksh

#Get the script path
__REAL_SCRIPTDIR=$( cd -P -- "$(dirname -- "$(command -v -- "$0")")" && pwd -P )

cd $__REAL_SCRIPTDIR

export DB_USER="psa"
export DB_PASSWORD="dHRpcGFzcw=="
export ORACLE_SID="samdev"
export DB_HOST="dc50-snd-db05:1521"

$HOME/.local/bin/pipenv run python chill.py $@
