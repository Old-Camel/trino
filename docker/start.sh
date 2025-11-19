#!/bin/bash
set -e
CONF_DIR="/app/catalog"
ENV_COMMON_DIR="/app/env/common"
/app/variable_replace.sh ${ENV_COMMON_DIR} ${CONF_DIR}
exec /usr/lib/trino/bin/run-trino
