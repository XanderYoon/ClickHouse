#!/usr/bin/env bash
# INSERT ... RETURNING returns a block only once this insert can no longer drop it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

err="${CLICKHOUSE_TMP}/returning_constraint.err"

${CLICKHOUSE_CLIENT} --query "
DROP TABLE IF EXISTS t_returning_constraint;
CREATE TABLE t_returning_constraint (x UInt64, CONSTRAINT c CHECK x < 500) ENGINE = MergeTree ORDER BY x;
"

# Blocks of 100. CHECK fails on the block 500..599. MergeTree commits the
# previous block when the next one is consumed, and drops the uncommitted
# block when the insert is cancelled, so a plain INSERT keeps 0..399.
set +e
returned=$(${CLICKHOUSE_CLIENT} --query "
INSERT INTO t_returning_constraint
RETURNING (SELECT x)
SELECT number FROM numbers(1000)
SETTINGS max_insert_block_size = 100, min_insert_block_size_rows = 100, max_block_size = 100,
         max_insert_threads = 1, max_threads = 1
FORMAT TSV
" 2>"${err}")
status=$?
set -e

[ "${status}" -ne 0 ]
grep -q 'VIOLATED_CONSTRAINT' "${err}"
[ "${returned}" = "$(seq 0 399)" ]
${CLICKHOUSE_CLIENT} --query "SELECT count(), min(x), max(x) FROM t_returning_constraint"
echo "OK mergetree constraint"

${CLICKHOUSE_CLIENT} --query "
DROP TABLE IF EXISTS t_returning_constraint_ok;
CREATE TABLE t_returning_constraint_ok (x UInt64, CONSTRAINT c CHECK x < 500) ENGINE = MergeTree ORDER BY x;
"
returned=$(${CLICKHOUSE_CLIENT} --query "
INSERT INTO t_returning_constraint_ok
RETURNING (SELECT x)
SELECT number FROM numbers(250)
SETTINGS max_insert_block_size = 100, min_insert_block_size_rows = 100, max_block_size = 100,
         max_insert_threads = 1, max_threads = 1
FORMAT TSV
")
[ "${returned}" = "$(seq 0 249)" ]
${CLICKHOUSE_CLIENT} --query "SELECT count(), min(x), max(x) FROM t_returning_constraint_ok"
echo "OK mergetree success"

# Memory publishes in onFinish. A CHECK failure never publishes, so RETURNING
# must not produce rows either.
${CLICKHOUSE_CLIENT} --query "
DROP TABLE IF EXISTS t_returning_constraint_mem;
CREATE TABLE t_returning_constraint_mem (x UInt64, CONSTRAINT c CHECK x < 500) ENGINE = Memory;
"
set +e
returned=$(${CLICKHOUSE_CLIENT} --query "
INSERT INTO t_returning_constraint_mem
RETURNING (SELECT x)
SELECT number FROM numbers(1000)
SETTINGS max_insert_block_size = 100, min_insert_block_size_rows = 100, max_block_size = 100,
         max_insert_threads = 1, max_threads = 1
FORMAT TSV
" 2>"${err}")
status=$?
set -e
[ "${status}" -ne 0 ]
grep -q 'VIOLATED_CONSTRAINT' "${err}"
[ -z "${returned}" ]
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_returning_constraint_mem"
echo "OK memory constraint"

returned=$(${CLICKHOUSE_CLIENT} --query "
INSERT INTO t_returning_constraint_mem
RETURNING (SELECT x)
SELECT number FROM numbers(10)
FORMAT TSV
")
[ "${returned}" = "$(seq 0 9)" ]
${CLICKHOUSE_CLIENT} --query "SELECT count(), min(x), max(x) FROM t_returning_constraint_mem"
echo "OK memory success"

${CLICKHOUSE_CLIENT} --query "
DROP TABLE t_returning_constraint;
DROP TABLE t_returning_constraint_ok;
DROP TABLE t_returning_constraint_mem;
"
