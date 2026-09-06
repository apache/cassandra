#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Generate the animated diagrams used by CEP-66-zero-copy-sstable-splitting.md.

    uv venv /tmp/gifvenv && uv pip install --python /tmp/gifvenv/bin/python pillow
    /tmp/gifvenv/bin/python cep-66-images/make_diagrams.py

Outputs (next to this script):
    01-chunk-split-N.png  chunk-preserving split and the retained prefix, one step per image
    02-reflink-N.png      sharing physical extents with byte-range reflinks, one step per image
    03-cost.gif           what each strategy actually has to do
"""

import os
from PIL import Image, ImageChops, ImageDraw, ImageFont

S = 2                                   # supersample factor
HERE = os.path.dirname(os.path.abspath(__file__))

BG        = (250, 250, 248)
INK       = (28, 30, 34)
MUTED     = (122, 126, 132)
FAINT     = (206, 209, 213)
PANEL     = (241, 241, 238)

A_EDGE,  A_FILL  = (46, 106, 190), (219, 233, 250)
B_EDGE,  B_FILL  = (196, 104, 30), (252, 231, 209)
SH_EDGE, SH_FILL = (120, 76, 176), (234, 225, 248)
OK_EDGE, OK_FILL = (30, 130, 86), (216, 240, 226)
NO_EDGE, NO_FILL = (194, 64, 60), (250, 223, 221)
AM_EDGE, AM_FILL = (176, 132, 20), (250, 240, 205)
GY_EDGE, GY_FILL = (152, 156, 162), (229, 230, 232)

_FONTS = {
    (0, 0): "/System/Library/Fonts/Supplemental/Arial.ttf",
    (1, 0): "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
    (0, 1): "/System/Library/Fonts/Menlo.ttc",
    (1, 1): "/System/Library/Fonts/Menlo.ttc",
}


def font(size, bold=False, mono=False):
    return ImageFont.truetype(_FONTS[(int(bold), int(mono))], int(size * S))


class Canvas:
    def __init__(self, w, h):
        self.w, self.h = w, h
        self.im = Image.new("RGB", (w * S, h * S), BG)
        self.d = ImageDraw.Draw(self.im)

    def rect(self, x, y, w, h, fill=None, edge=None, r=4, width=1.4):
        self.d.rounded_rectangle([x * S, y * S, (x + w) * S, (y + h) * S],
                                 radius=int(r * S), fill=fill, outline=edge,
                                 width=max(1, int(width * S)))

    def text(self, x, y, s, f, fill=INK, anchor="la"):
        self.d.text((x * S, y * S), s, font=f, fill=fill, anchor=anchor)

    def line(self, pts, fill=INK, width=1.4):
        self.d.line([(p[0] * S, p[1] * S) for p in pts], fill=fill,
                    width=max(1, int(width * S)), joint="curve")

    def vdash(self, x, y0, y1, fill=INK, width=1.4, dash=6, gap=5):
        y = y0
        while y < y1:
            self.line([(x, y), (x, min(y + dash, y1))], fill=fill, width=width)
            y += dash + gap

    def arrow(self, x0, y0, x1, y1, fill=INK, width=1.4, head=5):
        import math
        self.line([(x0, y0), (x1, y1)], fill=fill, width=width)
        a = math.atan2(y1 - y0, x1 - x0)
        for s in (2.6, -2.6):
            self.line([(x1, y1), (x1 + head * math.cos(a + s), y1 + head * math.sin(a + s))],
                      fill=fill, width=width)

    def hatch(self, x, y, w, h, color, step=8, width=1.2):
        ww, hh = max(1, int(w * S)), max(1, int(h * S))
        layer = Image.new("L", (ww, hh), 0)
        ld = ImageDraw.Draw(layer)
        st = max(2, int(step * S))
        for o in range(-hh, ww + hh, st):
            ld.line([(o, hh), (o + hh, 0)], fill=190, width=max(1, int(width * S)))
        self.im.paste(Image.new("RGB", (ww, hh), color), (int(x * S), int(y * S)), layer)

    def label(self, x, y, s, f, fill=INK, anchor="la", pad=5):
        w = self.d.textlength(s, font=f) / S
        bx = {"la": x, "ma": x - w / 2, "ra": x - w}[anchor]
        self.rect(bx - pad, y - 3, w + 2 * pad, 20, fill=BG, edge=None, r=3, width=0)
        self.text(x, y, s, f, fill=fill, anchor=anchor)

    def pill(self, x, y, s, f, edge, fill, padx=8, h=20):
        w = self.d.textlength(s, font=f) / S + 2 * padx
        self.rect(x, y, w, h, fill=fill, edge=edge, r=h / 2, width=1.2)
        self.text(x + w / 2, y + h / 2, s, f, fill=edge, anchor="mm")
        return w


# ---------------------------------------------------------------- gif plumbing

def caption_layer(w, h, y, text_, f, colour=INK, x=60):
    c = Canvas(w, h)
    c.text(x, y, text_, f, fill=colour)
    return c.im


def crossfade(a, b, p):
    return Image.blend(a, b, p)


def strip_band(im, band):
    """Remove a horizontal band (logical coordinates) that only held the animated caption."""
    y0, y1 = int(band[0] * S), int(band[1] * S)
    out = Image.new("RGB", (im.width, im.height - (y1 - y0)), BG)
    out.paste(im.crop((0, 0, im.width, y0)), (0, 0))
    out.paste(im.crop((0, y1, im.width, im.height)), (0, y0))
    return out


def stills(prefix, stages, band=(44, 84), keep=None, margin=16):
    """Write one PNG per stage, all cropped to the same box so the series stays aligned."""
    keep = list(range(len(stages))) if keep is None else list(keep)
    ims = [strip_band(stages[k][0], band) for k in keep]
    ref = Image.new("RGB", ims[0].size, BG)
    box = None
    for im in ims:
        b = ImageChops.difference(im, ref).getbbox()
        box = b if box is None else (min(box[0], b[0]), min(box[1], b[1]),
                                     max(box[2], b[2]), max(box[3], b[3]))
    m = int(margin * S)
    x0, y0 = max(0, box[0] - m), max(0, box[1] - m)
    x1 = min(ims[0].width, box[2] + m)
    # a common left, right and top keeps the horizontal scale identical across the series;
    # the bottom follows each image so the earlier steps carry no dead space
    for n, im in enumerate(ims, 1):
        y1 = min(im.height, ImageChops.difference(im, ref).getbbox()[3] + m)
        cut = (x0, y0, x1, y1)
        w, h = (x1 - x0) // S, (y1 - y0) // S
        path = "%s-%d.png" % (prefix, n)
        im.crop(cut).resize((w, h), Image.LANCZOS).save(path)
        print("%-24s %5.0f KiB  %dx%d" % (os.path.basename(path),
                                          os.path.getsize(path) / 1024, w, h))
    for k in range(len(stages)):
        if k not in keep:
            print("%-24s (skipped: %s)" % ("", stages[k][1][:56]))


def build(path, w, h, stages, cap_box, cap_font, fade_frames=9, hold_ms=1900, fps=24):
    """stages: list of (body_image, caption). Cross-fades body, hard-swaps caption."""
    blank = Image.new("RGB", (w * S, h * S), BG)
    caps = [caption_layer(w, h, cap_box[1], s[1], cap_font) for s in stages]
    frames = []
    step_ms = int(1000 / fps)
    x0, y0, x1, y1 = 0, cap_box[1] - 5, w, cap_box[1] + 28
    box = (int(x0 * S), int(y0 * S), int(x1 * S), int(y1 * S))

    def compose(body, cap):
        im = body.copy()
        im.paste(cap.crop(box), (box[0], box[1]))
        return im

    frames.append((compose(stages[0][0], caps[0]), hold_ms))
    for k in range(1, len(stages)):
        for i in range(1, fade_frames + 1):
            p = i / fade_frames
            body = crossfade(stages[k - 1][0], stages[k][0], p)
            if p < 0.5:
                cap = crossfade(caps[k - 1], blank, min(1.0, p / 0.45))
            else:
                cap = crossfade(blank, caps[k], min(1.0, (p - 0.5) / 0.45))
            frames.append((compose(body, cap), step_ms))
        frames.append((compose(stages[k][0], caps[k]), hold_ms))
    frames[-1] = (frames[-1][0], hold_ms + 1400)
    save_gif(path, w, h, frames)


def save_gif(path, w, h, frames):
    ims = [f.resize((w, h), Image.LANCZOS) for f, _ in frames]
    sample = ims[:: max(1, len(ims) // 10)]
    montage = Image.new("RGB", (w, h * len(sample)))
    for i, im in enumerate(sample):
        montage.paste(im, (0, i * h))
    master = montage.quantize(colors=160, method=Image.Quantize.MEDIANCUT)
    pal = [im.quantize(palette=master, dither=Image.Dither.NONE) for im in ims]
    pal[0].save(path, save_all=True, append_images=pal[1:],
                duration=[d for _, d in frames], loop=0, optimize=True, disposal=1)
    print("%-22s %6.0f KiB  %d frames" % (os.path.basename(path),
                                          os.path.getsize(path) / 1024, len(pal)))


# ------------------------------------------------------- 1. chunk-based split

W1, H1 = 980, 560
X0, TW, NC = 60, 860, 10
CW = TW / NC
PB = [0.00, 0.06, 0.14, 0.20, 0.29, 0.35, 0.43, 0.56, 0.62, 0.70, 0.78, 0.85, 0.93, 1.00]
SPLIT_I = 7                      # first partition owned by child B
SPLIT_X = X0 + PB[SPLIT_I] * TW
SPLIT_C = int(PB[SPLIT_I] * NC)  # chunk holding the split point
ROWS = {"p": (120, 178), "a": (272, 330), "b": (400, 458)}
CHUNK_H, STRIP_H = 52, 26


def cx(i):
    return X0 + i * CW


def chunks(c, y, lo, hi, edge, fill, shared=None, f=None):
    for i in range(lo, hi + 1):
        e, fl = (SH_EDGE, SH_FILL) if shared is not None and i == shared else (edge, fill)
        c.rect(cx(i) + 1, y, CW - 2, CHUNK_H, fill=fl, edge=e, r=3)
        c.text(cx(i) + CW / 2, y + CHUNK_H / 2, "C%d" % i, f, fill=e, anchor="mm")


def parts(c, y, lo, hi, edge, fill, f):
    for i in range(lo, hi + 1):
        x, w = X0 + PB[i] * TW, (PB[i + 1] - PB[i]) * TW
        c.rect(x + 1, y, w - 2, STRIP_H, fill=fill, edge=edge, r=3, width=1.2)
        c.text(x + w / 2, y + STRIP_H / 2, "p%d" % i, f, fill=edge, anchor="mm")


def gif1():
    f_title, f_cap = font(19, bold=True), font(15)
    f_lbl, f_ch, f_pt, f_note = font(13, bold=True), font(12, bold=True), font(10), font(11)
    f_mono = font(11, mono=True)
    stages = []

    def base(n):
        c = Canvas(W1, H1)
        c.text(60, 22, "Splitting a compressed SSTable by whole chunks", f_title)
        py, sy = ROWS["p"]
        c.text(60, py - 20, "Parent Data.db", f_lbl)
        c.text(920, py - 20, "compression chunks", f_note, fill=MUTED, anchor="ra")
        chunks(c, py, 0, NC - 1, GY_EDGE, GY_FILL, f=f_ch)
        if n == 0:
            parts(c, sy, 0, len(PB) - 2, GY_EDGE, (238, 239, 241), f_pt)
        else:
            parts(c, sy, 0, SPLIT_I - 1, A_EDGE, A_FILL, f_pt)
            parts(c, sy, SPLIT_I, len(PB) - 2, B_EDGE, B_FILL, f_pt)
        c.text(920, sy + STRIP_H + 6, "partitions, packed end to end", f_note, fill=MUTED, anchor="ra")
        if n >= 1:
            c.vdash(SPLIT_X, py - 12, ROWS["b"][1] + STRIP_H + 10 if n >= 3 else sy + STRIP_H + 12,
                    fill=NO_EDGE, width=1.6)
            c.text(SPLIT_X + 7, py - 22, "split point", f_note, fill=NO_EDGE)
        return c

    # 0 ---------------------------------------------------------------------
    c = base(0)
    stages.append((c.im, "A compressed SSTable stores its partitions inside fixed-size compression chunks."))

    # 1 ---------------------------------------------------------------------
    c = base(1)
    c.rect(cx(SPLIT_C) + 1, ROWS["p"][0] - 4, CW - 2, CHUNK_H + 8, edge=NO_EDGE, r=5, width=1.8)
    stages.append((c.im, "The split point lands in the middle of chunk C%d — and a chunk cannot be sliced." % SPLIT_C))

    def child_a(c, hatched=False):
        y, sy = ROWS["a"]
        c.text(60, y - 20, "Child A  →  chunks C0–C%d" % SPLIT_C, f_lbl, fill=A_EDGE)
        chunks(c, y, 0, SPLIT_C, A_EDGE, A_FILL, shared=SPLIT_C, f=f_ch)
        parts(c, sy, 0, SPLIT_I - 1, A_EDGE, A_FILL, f_pt)
        if hatched:
            w = cx(SPLIT_C + 1) - SPLIT_X
            c.rect(SPLIT_X, sy, w - 1, STRIP_H, fill=(243, 243, 241), edge=GY_EDGE, r=3, width=1.2)
            c.hatch(SPLIT_X + 2, sy + 2, w - 5, STRIP_H - 4, GY_EDGE, step=7)

    def child_b(c, hatched=False):
        y, sy = ROWS["b"]
        c.text(60, y - 20, "Child B  →  chunks C%d–C%d" % (SPLIT_C, NC - 1), f_lbl, fill=B_EDGE)
        chunks(c, y, SPLIT_C, NC - 1, B_EDGE, B_FILL, shared=SPLIT_C, f=f_ch)
        parts(c, sy, SPLIT_I, len(PB) - 2, B_EDGE, B_FILL, f_pt)
        if hatched:
            w = SPLIT_X - cx(SPLIT_C)
            c.rect(cx(SPLIT_C) + 1, sy, w - 2, STRIP_H, fill=(243, 243, 241), edge=GY_EDGE, r=3, width=1.2)
            c.hatch(cx(SPLIT_C) + 3, sy + 2, w - 6, STRIP_H - 4, GY_EDGE, step=7)

    # 2 ---------------------------------------------------------------------
    c = base(2)
    child_a(c)
    stages.append((c.im, "Child A keeps the whole run of chunks that covers its partitions: C0–C%d." % SPLIT_C))

    # 3 ---------------------------------------------------------------------
    c = base(3)
    child_a(c)
    child_b(c)
    c.rect(cx(SPLIT_C) + 1, ROWS["p"][0] - 4, CW - 2, CHUNK_H + 8, edge=SH_EDGE, r=5, width=1.8)
    c.text(cx(SPLIT_C) + CW / 2, ROWS["p"][0] - 30, "shared", f_note, fill=SH_EDGE, anchor="ma")
    stages.append((c.im, "Child B keeps C%d–C%d. The boundary chunk is retained by both children, byte for byte."
                   % (SPLIT_C, NC - 1)))

    # 4 ---------------------------------------------------------------------
    c = base(4)
    child_a(c, hatched=True)
    child_b(c, hatched=True)
    c.text(cx(SPLIT_C + 1) + 8, ROWS["a"][1] + 4, "unindexed tail", f_note, fill=MUTED)
    c.text(cx(SPLIT_C) - 8, ROWS["b"][1] + 4, "retained prefix", f_note, fill=MUTED, anchor="ra")
    stages.append((c.im, "The overlapping bytes stay on disk but stay out of the index — every partition is "
                         "indexed exactly once."))

    # 5 ---------------------------------------------------------------------
    c = base(5)
    child_a(c, hatched=True)
    child_b(c, hatched=True)
    sy = ROWS["b"][1]
    c.arrow(SPLIT_X, sy + STRIP_H + 22, SPLIT_X, sy + STRIP_H + 4, fill=B_EDGE, width=1.6, head=5)
    c.text(SPLIT_X + 8, sy + STRIP_H + 14, "firstIndexedPosition — scans, verify and scrub start here",
           f_mono, fill=B_EDGE)
    x = 60
    for lbl in ("Index.db", "Summary.db", "Filter.db", "Statistics.db", "TOC / Digest"):
        x += c.pill(x, H1 - 44, "rebuilt  " + lbl, f_note, GY_EDGE, PANEL) + 8
    c.pill(x + 6, H1 - 44, "Data.db  reused chunks", f_note, OK_EDGE, OK_FILL)
    stages.append((c.im, "Metadata records where each child's own data begins; the derived components are rebuilt."))

    stills(os.path.join(HERE, "01-chunk-split"), stages)


# ------------------------------------------------------------- 2. reflinks

W2, H2 = 980, 560


def gif2():
    f_title, f_cap = font(19, bold=True), font(15)
    f_lbl, f_ch, f_note = font(13, bold=True), font(12, bold=True), font(11)
    f_mono = font(11, mono=True)
    EX_Y, FILE_H, EX_H = 268, 46, 46
    P_Y, C_Y = 128, 420
    AX, AW = 60, 470          # child A box
    BX, BW = 560, 360         # child B box
    A_EX, B_EX = (0, 5), (5, 9)
    stages = []

    def extents(c, hot=None):
        c.text(60, EX_Y - 22, "Physical extents on disk", f_lbl)
        for i in range(NC):
            e, fl = (SH_EDGE, SH_FILL) if hot == i else (GY_EDGE, GY_FILL)
            c.rect(cx(i) + 1, EX_Y, CW - 2, EX_H, fill=fl, edge=e, r=3)
            c.text(cx(i) + CW / 2, EX_Y + EX_H / 2, "E%d" % i, f_ch, fill=e, anchor="mm")

    def fan(c, bx, bw, lo, hi, colour):
        n = hi - lo + 1
        for k, i in enumerate(range(lo, hi + 1)):
            sx = bx + bw * (k + 0.5) / n
            c.arrow(sx, C_Y - 2, cx(i) + CW / 2, EX_Y + EX_H + 6, fill=colour, width=1.3, head=5)

    def base(n, hot=None, parent_dim=False):
        c = Canvas(W2, H2)
        c.text(60, 22, "Reflinks: the children point at the parent's extents", f_title)
        pe, pf = (FAINT, (246, 246, 244)) if parent_dim else (GY_EDGE, (236, 237, 239))
        c.label(60, P_Y - 22, "Parent Data.db", f_lbl, fill=MUTED if parent_dim else INK)
        if parent_dim:
            c.label(920, P_Y - 22, "released by the lifecycle transaction", f_note,
                    fill=MUTED, anchor="ra")
        c.rect(X0, P_Y, TW, FILE_H, fill=pf, edge=pe, r=4)
        for i in range(1, NC):
            c.line([(cx(i), P_Y + 6), (cx(i), P_Y + FILE_H - 6)], fill=pe, width=1)
        for i in range(NC):
            c.text(cx(i) + CW / 2, P_Y + FILE_H / 2, "C%d" % i, f_ch,
                   fill=FAINT if parent_dim else GY_EDGE, anchor="mm")
            if i in (2, 5, 8):        # a few representative arrows; a full row would run
                                          # through the label below and read as clutter
                c.arrow(cx(i) + CW / 2, P_Y + FILE_H + 4, cx(i) + CW / 2, EX_Y - 6,
                        fill=pe, width=1.1, head=4)
        extents(c, hot=hot)
        return c

    # 0 ----------------------------------------------------------------------
    c = base(0)
    c.text(920, EX_Y + EX_H + 16, "63 GiB allocated", f_note, fill=MUTED, anchor="ra")
    stages.append((c.im, "A Data.db file is just a map onto physical extents."))

    def box_a(c, dim=False):
        e, fl = (FAINT, (247, 247, 245)) if dim else (A_EDGE, A_FILL)
        c.rect(AX, C_Y, AW, FILE_H, fill=fl, edge=e, r=4)
        c.text(AX + 12, C_Y + FILE_H / 2, "Child A Data.db", f_lbl, fill=e, anchor="lm")
        c.text(AX + AW - 12, C_Y + FILE_H / 2, "E0–E5", f_mono, fill=e, anchor="rm")

    def box_b(c):
        c.rect(BX, C_Y, BW, FILE_H, fill=B_FILL, edge=B_EDGE, r=4)
        c.text(BX + 12, C_Y + FILE_H / 2, "Child B Data.db", f_lbl, fill=B_EDGE, anchor="lm")
        c.text(BX + BW - 12, C_Y + FILE_H / 2, "E5–E9", f_mono, fill=B_EDGE, anchor="rm")

    # 1 ----------------------------------------------------------------------
    c = base(1)
    box_a(c)
    fan(c, AX, AW, *A_EX, A_EDGE)
    c.pill(60, C_Y + FILE_H + 16, "FICLONERANGE(parent, E0–E5)  →  0 bytes written",
           f_note, OK_EDGE, OK_FILL)
    stages.append((c.im, "A byte-range reflink hands child A the parent's extents — a metadata operation."))

    # 2 ----------------------------------------------------------------------
    c = base(2, hot=5)
    box_a(c)
    box_b(c)
    fan(c, AX, AW, *A_EX, A_EDGE)
    fan(c, BX, BW, *B_EX, B_EDGE)
    c.text(cx(5) + CW / 2 + 9, EX_Y - 22, "referenced 3×", f_note, fill=SH_EDGE)
    stages.append((c.im, "The boundary extent is simply referenced by both children. Nothing is duplicated."))

    # 3 ----------------------------------------------------------------------
    c = base(3, hot=5, parent_dim=True)
    box_a(c)
    box_b(c)
    fan(c, AX, AW, *A_EX, A_EDGE)
    fan(c, BX, BW, *B_EX, B_EDGE)
    stages.append((c.im, "When the parent is dropped the extents stay live; they are freed on the last reference."))

    # 4 ----------------------------------------------------------------------
    c = base(4, hot=5, parent_dim=True)
    box_a(c)
    box_b(c)
    fan(c, AX, AW, *A_EX, A_EDGE)
    fan(c, BX, BW, *B_EX, B_EDGE)
    c.rect(60, H2 - 78, 860, 56, fill=PANEL, edge=FAINT, r=6)
    c.text(76, H2 - 68, "df   allocated space is unchanged — the extents are shared", f_mono, fill=INK)
    c.text(76, H2 - 48, "du   may count a shared extent once per file, and so over-report", f_mono, fill=MUTED)
    stages.append((c.im, "Operators should expect df and du to disagree while parent and children coexist."))

    # 5 ----------------------------------------------------------------------
    c = base(5, hot=5, parent_dim=True)
    box_a(c)
    box_b(c)
    fan(c, AX, AW, *A_EX, A_EDGE)
    fan(c, BX, BW, *B_EX, B_EDGE)
    c.rect(60, H2 - 78, 860, 56, fill=NO_FILL, edge=NO_EDGE, r=6, width=1.2)
    c.text(76, H2 - 68, "No reflink support?  The same compressed chunks are copied instead.", f_mono, fill=INK)
    c.text(76, H2 - 48, "Still no decompress, no deserialize, no recompress — identical result on disk.",
           f_mono, fill=NO_EDGE)
    stages.append((c.im, "Reflinks are opportunistic: any filesystem falls back to copying the compressed bytes."))

    # the last two stages only add a text panel, which reads better as prose in the CEP
    stills(os.path.join(HERE, "02-reflink"), stages, keep=(0, 1, 2, 3))


# ----------------------------------------------------------------- 3. cost

W3, H3 = 940, 440
LANES = [
    ("Row rewrite (today)",  "read · decompress · deserialize · re-serialize · recompress · write",
     257.9, "148.5 s CPU · 62.6 GiB written", NO_EDGE, NO_FILL),
    ("Encoded-byte copy",    "read · copy compressed chunks · write",
     258.6, "72.6 s CPU · 54.9 GiB written", AM_EDGE, AM_FILL),
    ("Reflink + digest",     "clone extents · re-read data for Digest.crc32",
     123.2, "46.4 s CPU · 57 MiB written", A_EDGE, A_FILL),
    ("Reflink, digest off",  "clone extents",
     0.77, "0.93 s CPU · 57 MiB written", OK_EDGE, OK_FILL),
]
TMAX = 258.6


def gif3():
    f_title, f_lbl, f_sub = font(19, bold=True), font(14, bold=True), font(11)
    f_time, f_note = font(13, bold=True, mono=True), font(11)
    f_clock = font(15, bold=True, mono=True)
    TX, TW3, TH = 300, 540, 26
    LY = [112, 182, 252, 322]

    def frame(t):
        c = Canvas(W3, H3)
        c.text(60, 22, "Splitting one 63 GiB SSTable eight ways, cold cache", f_title)
        c.text(60, 52, "Every strategy produces the same eight logical SSTables.", font(15), fill=MUTED)
        c.text(880, 52, "elapsed %6.1f s" % min(t, TMAX), f_clock, fill=INK, anchor="ra")
        for (name, work, secs, cost, edge, fill), y in zip(LANES, LY):
            c.text(60, y - 4, name, f_lbl, fill=edge)
            c.text(60, y + 16, work, f_sub, fill=MUTED)
            c.rect(TX, y - 6, TW3, TH, fill=(243, 243, 241), edge=FAINT, r=TH / 2, width=1)
            done = t >= secs
            w = min(t, secs) / TMAX * TW3
            if done:
                w = max(TH, w)
            if w >= 2:
                c.rect(TX, y - 6, w, TH, fill=fill, edge=edge, r=min(TH, w) / 2, width=1.4)
            if done:
                c.text(TX + TW3 + 14, y + 7, "%.2f s" % secs if secs < 10 else "%.1f s" % secs,
                       f_time, fill=edge, anchor="lm")
                c.text(60, y + 32, cost, f_sub, fill=edge)
        c.rect(60, H3 - 64, 820, 44, fill=PANEL, edge=FAINT, r=6)
        c.text(76, H3 - 52, "With the digest disabled the split is 335× faster, uses 160× less CPU and "
                            "writes 1,125× fewer bytes.", font(12), fill=INK)
        return c.im

    fps, span = 22, 4.2
    n = int(fps * span)
    frames = [(frame(0.0), 900)]
    for i in range(1, n + 1):
        frames.append((frame(TMAX * i / n), int(1000 / fps)))
    frames.append((frame(TMAX), 2600))
    save_gif(os.path.join(HERE, "03-cost.gif"), W3, H3, frames)


if __name__ == "__main__":
    gif1()
    gif2()
    gif3()
