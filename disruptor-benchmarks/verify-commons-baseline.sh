#!/bin/sh

set -eu

reference_dir=$1
expected_commit=$2
local_repository=$3
expected_concurrent=$4
expected_base=$5
expected_disruptor=$6

actual_commit=$(git -C "$reference_dir" rev-parse HEAD)
if [ "$actual_commit" != "$expected_commit" ]; then
    echo "Commons 参考仓 HEAD $actual_commit 与固定 commit $expected_commit 不一致" >&2
    exit 1
fi
git -C "$reference_dir" diff-index --quiet HEAD --

canonical_jar_sha256() {
    jar_path=$1
    if [ ! -f "$jar_path" ]; then
        echo "缺少 Commons 参考 artifact：$jar_path" >&2
        exit 1
    fi
    jar tf "$jar_path" \
        | LC_ALL=C sort \
        | while IFS= read -r jar_entry; do
            printf '%s\0' "$jar_entry"
            unzip -p "$jar_path" "$jar_entry" 2>/dev/null || true
        done \
        | shasum -a 256 \
        | awk '{print $1}'
}

verify_jar() {
    jar_path=$1
    expected_sha256=$2
    actual_sha256=$(canonical_jar_sha256 "$jar_path")
    if [ "$actual_sha256" != "$expected_sha256" ]; then
        echo "Commons 参考 artifact 内容不匹配：$jar_path" >&2
        echo "expected=$expected_sha256 actual=$actual_sha256" >&2
        exit 1
    fi
}

verify_jar \
    "$local_repository/cn/wjybxx/commons/commons-concurrent/2.0.0/commons-concurrent-2.0.0.jar" \
    "$expected_concurrent"
verify_jar \
    "$local_repository/cn/wjybxx/commons/commons-base/2.0.0/commons-base-2.0.0.jar" \
    "$expected_base"
verify_jar \
    "$local_repository/cn/wjybxx/commons/disruptor/2.0.0/disruptor-2.0.0.jar" \
    "$expected_disruptor"
