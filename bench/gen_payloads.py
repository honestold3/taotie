#!/usr/bin/env python3
"""生成压测用的埋点数据（每行一个 URL 编码后的 base64 串）。

用法: python3 bench/gen_payloads.py [条数] [输出文件]
"""

import base64
import json
import sys
import urllib.parse


def build(i: int) -> dict:
    return {
        "identities": {"$identity_cookie_id": f"cookie-{i}"},
        "distinct_id": f"bench-{i}",
        "lib": {"$lib": "js", "$lib_method": "code", "$lib_version": "1.25.11"},
        "properties": {
            "$url": "https://dljob.xxx.com.cn/",
            "$title": "压测页面",
            "n": i,
        },
        "anonymous_id": f"anon-{i}",
        "type": "track",
        "event": "$pageview",
        "time": 1787725281606 + i,
        "_track_id": 100000 + i,
        "_flush_time": 1787725281607 + i,
    }


def main() -> None:
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    out = sys.argv[2] if len(sys.argv) > 2 else "bench/payloads.txt"

    with open(out, "w", encoding="utf-8") as f:
        for i in range(count):
            raw = json.dumps(build(i), separators=(",", ":"), ensure_ascii=False)
            encoded = base64.b64encode(raw.encode("utf-8")).decode()
            f.write(urllib.parse.quote(encoded, safe="") + "\n")

    print(f"已生成 {count} 条压测数据: {out}")


if __name__ == "__main__":
    main()
