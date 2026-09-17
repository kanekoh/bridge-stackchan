"""楽譜（YAML/dict）→ VOICEVOX ENGINE の notes 配列への変換。

このモジュールは純粋な計算だけを行い、ENGINE にも DB にも触らない。
歌の「音程・長さ」の正しさはここだけで決まるので、ユニットテストの主対象になる。

VOICEVOX ENGINE 0.25.2 の歌唱 API は
  POST /sing_frame_audio_query?speaker=<sing用style_id>
に {"notes": [{"key": <MIDI番号 or null>, "frame_length": <int>, "lyric": "<1モーラ>"}, ...]}
を渡す。楽譜の拍数をこの形へ落とすのがここの仕事。

フレーム長の決め方が要注意で、1音ずつ round(拍 × フレーム/拍) すると
丸め誤差が音の数だけ積み上がり、曲の後半でテンポがずれる。
そこで「累積拍 → 累積フレーム」を先に丸め、各音の長さはその差分で求める。
こうすると合計フレーム数が round(総拍数 × フレーム/拍) と必ず一致する。
"""
import hashlib
import json
import re
from dataclasses import dataclass, field

# GET /engine_manifest の frame_rate 既定値。ENGINE から取得できたらそちらを使う。
DEFAULT_FRAME_RATE = 93.75

# 楽譜の先頭に必ず入れる無音の長さ（拍）。
# ENGINE は先頭ノートが無音であることを前提にしているため、楽譜側には書かせず
# 変換時にこちらで付ける（書き忘れによる事故をなくす）。
DEFAULT_LEAD_SILENCE_BEATS = 0.5

_NOTE_RE = re.compile(r"^([A-Ga-g])([#♯b♭]?)([-]?\d+)$")
_PITCH_CLASS = {"C": 0, "D": 2, "E": 4, "F": 5, "G": 7, "A": 9, "B": 11}
_ACCIDENTAL = {"": 0, "#": 1, "♯": 1, "b": -1, "♭": -1}

_REST_NAMES = {"rest", "r", "R", "-", "休符"}

# 小書きのかな。直前のかなとセットで 1 モーラになる。
_SMALL_KANA = set("ぁぃぅぇぉゃゅょゎァィゥェォャュョヮ")
_KANA_RE = re.compile(r"^[ぁ-んァ-ヶー]+$")

# MIDI ノート番号の許容範囲（ENGINE が扱えるのは概ね人の声域だが、
# ここでは明らかな入力ミス（オクターブ指定ミス等）を弾くのが目的）。
_MIDI_MIN, _MIDI_MAX = 12, 115


class ScoreError(ValueError):
    """楽譜の記述が不正なときに送出する。"""


def note_to_midi(name: str) -> int | None:
    """音名（C4, F#3, Bb4 など）を MIDI ノート番号に変換する。休符なら None。

    C4 = 60（いわゆる middle C）とする。
    """
    if name is None:
        return None
    text = str(name).strip()
    if text in _REST_NAMES or text == "":
        return None
    m = _NOTE_RE.match(text)
    if not m:
        raise ScoreError(f"音名として解釈できません: {name!r}（例: C4, F#3, Bb4, rest）")
    letter, accidental, octave = m.group(1).upper(), m.group(2), int(m.group(3))
    midi = (octave + 1) * 12 + _PITCH_CLASS[letter] + _ACCIDENTAL[accidental]
    if not (_MIDI_MIN <= midi <= _MIDI_MAX):
        raise ScoreError(f"音域外です: {name!r} (MIDI {midi})")
    return midi


def is_single_mora(lyric: str) -> bool:
    """歌詞が「ひらがな・カタカナ1モーラ」かどうか。

    「き」は 1 文字 1 モーラ、「きゃ」は 2 文字で 1 モーラ。
    ENGINE は 1 ノート 1 モーラを前提にしている。
    """
    if not lyric or not _KANA_RE.match(lyric):
        return False
    if len(lyric) == 1:
        return lyric not in _SMALL_KANA
    if len(lyric) == 2:
        return lyric[0] not in _SMALL_KANA and lyric[1] in _SMALL_KANA
    return False


@dataclass(frozen=True)
class NoteSpec:
    """楽譜上の 1 音（または休符）。"""
    note: str          # "C4" / "rest"
    beats: float       # 拍数（4分音符 = 1.0）
    lyric: str = ""    # 空なら楽譜の default_lyric を使う。休符では常に ""


@dataclass(frozen=True)
class Score:
    """1 曲分の楽譜。config/songs/*.yaml 1 ファイルに対応する。"""
    id: str
    title: str
    bpm: float
    notes: list[NoteSpec]
    transpose: int = 0
    default_lyric: str = "ん"          # 既定は鼻歌
    sing_style_id: int = 6000          # /singers の type=sing（0.25.2 では波音リツのみ）
    frame_decode_style_id: int = 3003  # /singers の type=frame_decode（声色を決める）
    mood: list[str] = field(default_factory=list)
    lead_silence_beats: float = DEFAULT_LEAD_SILENCE_BEATS
    credit: str = "VOICEVOX:ずんだもん"

    # ── 出自（音そのものには影響しないので cache_key には含めない） ──
    source: str = "builtin"      # "builtin"（config/songs の YAML）か "composed"（作った歌）
    created_by: str = ""         # 誰のために／誰と作ったか
    theme: str = ""              # 何をお題に作ったか
    created_at: str = ""

    @property
    def total_beats(self) -> float:
        return self.lead_silence_beats + sum(n.beats for n in self.notes)

    def duration_sec(self, frame_rate: float = DEFAULT_FRAME_RATE) -> float:
        return total_frames(self, frame_rate) / frame_rate


def parse_score(data: dict, *, song_id: str | None = None) -> Score:
    """YAML/JSON から読んだ dict を検証して Score にする。"""
    if not isinstance(data, dict):
        raise ScoreError("楽譜はマッピング（辞書）である必要があります")

    sid = str(data.get("id") or song_id or "").strip()
    if not sid:
        raise ScoreError("id が必要です")

    try:
        bpm = float(data.get("bpm", 0))
    except (TypeError, ValueError):
        raise ScoreError(f"bpm が数値ではありません: {data.get('bpm')!r}")
    if not (20 <= bpm <= 400):
        raise ScoreError(f"bpm は 20〜400 の範囲で指定してください: {bpm}")

    default_lyric = str(data.get("default_lyric", "ん"))
    if not is_single_mora(default_lyric):
        raise ScoreError(f"default_lyric はかな1モーラで指定してください: {default_lyric!r}")

    raw_notes = data.get("notes")
    if not isinstance(raw_notes, list) or not raw_notes:
        raise ScoreError("notes に1音以上必要です")

    notes: list[NoteSpec] = []
    for i, raw in enumerate(raw_notes):
        if not isinstance(raw, dict):
            raise ScoreError(f"notes[{i}] はマッピングである必要があります: {raw!r}")
        name = str(raw.get("note", "rest"))
        try:
            beats = float(raw.get("beats", 1))
        except (TypeError, ValueError):
            raise ScoreError(f"notes[{i}].beats が数値ではありません: {raw.get('beats')!r}")
        if beats <= 0:
            raise ScoreError(f"notes[{i}].beats は正の数である必要があります: {beats}")
        midi = note_to_midi(name)  # ここで音名の妥当性も検証される
        lyric = str(raw.get("lyric", "") or "")
        if midi is None:
            lyric = ""  # 休符に歌詞は乗らない
        else:
            lyric = lyric or default_lyric
            if not is_single_mora(lyric):
                raise ScoreError(f"notes[{i}].lyric はかな1モーラで指定してください: {lyric!r}")
        notes.append(NoteSpec(note=name, beats=beats, lyric=lyric))

    style = data.get("style") or {}
    if not isinstance(style, dict):
        raise ScoreError("style はマッピングである必要があります")

    mood = data.get("mood") or []
    if isinstance(mood, str):
        mood = [mood]
    if not isinstance(mood, list):
        raise ScoreError("mood は文字列または文字列のリストで指定してください")

    try:
        transpose = int(data.get("transpose", 0))
    except (TypeError, ValueError):
        raise ScoreError(f"transpose が整数ではありません: {data.get('transpose')!r}")
    if abs(transpose) > 24:
        raise ScoreError(f"transpose は ±24 半音以内で指定してください: {transpose}")

    lead = float(data.get("lead_silence_beats", DEFAULT_LEAD_SILENCE_BEATS))
    if lead <= 0:
        raise ScoreError("lead_silence_beats は正の数である必要があります（先頭は必ず無音）")

    score = Score(
        id=sid,
        title=str(data.get("title") or sid),
        bpm=bpm,
        notes=notes,
        transpose=transpose,
        default_lyric=default_lyric,
        sing_style_id=int(style.get("sing", 6000)),
        frame_decode_style_id=int(style.get("frame_decode", 3003)),
        mood=[str(m) for m in mood],
        lead_silence_beats=lead,
        credit=str(data.get("credit", "VOICEVOX:ずんだもん")),
        source=str(data.get("source", "builtin")),
        created_by=str(data.get("created_by", "")),
        theme=str(data.get("theme", "")),
        created_at=str(data.get("created_at", "")),
    )
    # 移調後に音域外へ出ていないか、テンポに対して短すぎる音がないかをここで確かめる。
    build_notes(score)
    return score


def score_to_dict(score: Score) -> dict:
    """parse_score にそのまま渡せる dict に戻す。

    作った歌を DB に保存するときに使う。YAML の楽譜と同じ形なので、
    書き出せばそのまま config/songs/ に置ける。
    """
    return {
        "id": score.id,
        "title": score.title,
        "bpm": score.bpm,
        "transpose": score.transpose,
        "mood": list(score.mood),
        "default_lyric": score.default_lyric,
        "lead_silence_beats": score.lead_silence_beats,
        "style": {"sing": score.sing_style_id, "frame_decode": score.frame_decode_style_id},
        "credit": score.credit,
        "notes": [
            {"note": n.note, "beats": n.beats, **({"lyric": n.lyric} if n.lyric else {})}
            for n in score.notes
        ],
    }


def build_notes(score: Score, frame_rate: float = DEFAULT_FRAME_RATE) -> list[dict]:
    """Score を /sing_frame_audio_query の notes 配列に変換する。

    先頭には必ず無音ノート（key=None, lyric=""）が入る。
    各ノートの frame_length は累積フレーム数の差分なので、丸め誤差が蓄積しない。
    """
    if frame_rate <= 0:
        raise ScoreError(f"frame_rate は正の数である必要があります: {frame_rate}")
    frames_per_beat = (60.0 / score.bpm) * frame_rate

    specs = [NoteSpec(note="rest", beats=score.lead_silence_beats, lyric="")] + list(score.notes)

    out: list[dict] = []
    acc_beats = 0.0
    prev_frame = 0
    for i, spec in enumerate(specs):
        acc_beats += spec.beats
        cur_frame = round(acc_beats * frames_per_beat)
        length = cur_frame - prev_frame
        if length < 1:
            raise ScoreError(
                f"notes[{i - 1}] が短すぎます（{spec.beats}拍 @ {score.bpm}BPM は1フレーム未満）"
            )
        midi = note_to_midi(spec.note)
        if midi is not None:
            midi += score.transpose
            if not (_MIDI_MIN <= midi <= _MIDI_MAX):
                raise ScoreError(
                    f"移調後に音域外になりました: {spec.note} + {score.transpose} 半音 (MIDI {midi})"
                )
        out.append({"key": midi, "frame_length": length, "lyric": spec.lyric})
        prev_frame = cur_frame
    return out


def total_frames(score: Score, frame_rate: float = DEFAULT_FRAME_RATE) -> int:
    """曲全体のフレーム数。build_notes の frame_length 合計と必ず一致する。"""
    return round(score.total_beats * (60.0 / score.bpm) * frame_rate)


def cache_key(score: Score, *, engine_version: str, frame_rate: float, audio_spec: str) -> str:
    """楽譜と合成条件から決まるキャッシュキー。

    楽譜・style_id・ENGINE バージョン・出力フォーマットのいずれかが変わったときだけ
    値が変わる。変わらない限り再生成はしない。

    数値は float に揃えてから並べる。揃えないと 120 と 120.0 が別のキーになり、
    YAML から読んだ曲と DB から読み直した曲が同じ音なのに作り直されてしまう。
    """
    payload = {
        "id": score.id,
        "bpm": float(score.bpm),
        "transpose": int(score.transpose),
        "lead": float(score.lead_silence_beats),
        "sing": int(score.sing_style_id),
        "frame_decode": int(score.frame_decode_style_id),
        "notes": [[n.note, float(n.beats), n.lyric] for n in score.notes],
        "engine": engine_version,
        "frame_rate": float(frame_rate),
        "audio": audio_spec,
    }
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]
