"""キャプチャ（写真・音声）の保存と掃除。

一時保存（既定7日で消える）と記憶（ずっと残す）を、`captures.keep_until` の
NULL / 日付だけで区別する。NULL が記憶で、日付が入っていれば一時保存。
判定を1か所（store.retention_deadline）に閉じ込めてあるので、
呼ぶ側は "temp" / "permanent" を渡すだけでよい。
"""
