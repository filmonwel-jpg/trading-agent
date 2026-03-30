from pathlib import Path
import os
import struct
import zlib

root = Path('/Users/filmonghezehey/trading-agent/src/main/resources/static/icons')
root.mkdir(parents=True, exist_ok=True)

BG = (15, 23, 42, 255)
PANEL = (17, 24, 39, 255)
ACCENT = (56, 189, 248, 255)
GREEN = (34, 197, 94, 255)
WHITE = (229, 231, 235, 255)
MUTED = (148, 163, 184, 255)


def write_png(path: Path, width: int, height: int, pixels: bytearray) -> None:
    def chunk(tag: bytes, data: bytes) -> bytes:
        return (
            struct.pack('!I', len(data))
            + tag
            + data
            + struct.pack('!I', zlib.crc32(tag + data) & 0xFFFFFFFF)
        )

    raw = bytearray()
    stride = width * 4
    for y in range(height):
        raw.append(0)
        start = y * stride
        raw.extend(pixels[start:start + stride])

    ihdr = struct.pack('!IIBBBBB', width, height, 8, 6, 0, 0, 0)
    data = b'\x89PNG\r\n\x1a\n'
    data += chunk(b'IHDR', ihdr)
    data += chunk(b'IDAT', zlib.compress(bytes(raw), 9))
    data += chunk(b'IEND', b'')
    path.write_bytes(data)


def rounded_rect_mask(x: int, y: int, left: int, top: int, right: int, bottom: int, radius: int) -> bool:
    if x < left or x >= right or y < top or y >= bottom:
        return False
    if left + radius <= x < right - radius or top + radius <= y < bottom - radius:
        return True
    cx = left + radius if x < left + radius else right - radius - 1
    cy = top + radius if y < top + radius else bottom - radius - 1
    dx = x - cx
    dy = y - cy
    return dx * dx + dy * dy <= radius * radius


def set_px(buf: bytearray, size: int, x: int, y: int, color: tuple[int, int, int, int]) -> None:
    idx = (y * size + x) * 4
    buf[idx:idx + 4] = bytes(color)


def make_icon(size: int, maskable: bool = False) -> bytearray:
    pixels = bytearray(size * size * 4)
    for y in range(size):
        for x in range(size):
            set_px(pixels, size, x, y, BG)

    pad = int(size * (0.08 if maskable else 0.14))
    radius = int(size * (0.19 if maskable else 0.14))
    for y in range(size):
        for x in range(size):
            if rounded_rect_mask(x, y, pad, pad, size - pad, size - pad, radius):
                set_px(pixels, size, x, y, PANEL)

    band_top = pad + int(size * 0.08)
    band_h = int(size * 0.06)
    band_left = pad + int(size * 0.10)
    band_right = size - pad - int(size * 0.10)
    for y in range(band_top, band_top + band_h):
        for x in range(band_left, band_right):
            set_px(pixels, size, x, y, ACCENT)

    chart_left = pad + int(size * 0.16)
    chart_bottom = size - pad - int(size * 0.18)
    bar_w = int(size * 0.11)
    gap = int(size * 0.06)
    heights = [int(size * 0.18), int(size * 0.28), int(size * 0.40)]
    colors = [MUTED, GREEN, ACCENT]
    for i, (height, color) in enumerate(zip(heights, colors)):
        left = chart_left + i * (bar_w + gap)
        for y in range(chart_bottom - height, chart_bottom):
            for x in range(left, left + bar_w):
                if rounded_rect_mask(x, y, left, chart_bottom - height, left + bar_w, chart_bottom, max(2, bar_w // 4)):
                    set_px(pixels, size, x, y, color)

    cx = size - pad - int(size * 0.20)
    cy = pad + int(size * 0.24)
    r = int(size * 0.10)
    for y in range(cy - r, cy + r + 1):
        for x in range(cx - r, cx + r + 1):
            if 0 <= x < size and 0 <= y < size and (x - cx) ** 2 + (y - cy) ** 2 <= r * r:
                set_px(pixels, size, x, y, GREEN)

    for t in range(-2, 3):
        for i in range(int(r * 0.9)):
            x = cx - int(r * 0.55) + i
            y = cy + int(r * 0.05) + i // 2 + t
            if 0 <= x < size and 0 <= y < size:
                set_px(pixels, size, x, y, WHITE)
        for i in range(int(r * 1.2)):
            x = cx - int(r * 0.05) + i
            y = cy + int(r * 0.35) - i + t
            if 0 <= x < size and 0 <= y < size:
                set_px(pixels, size, x, y, WHITE)

    return pixels


for name, size, maskable in [
    ('icon-192.png', 192, False),
    ('icon-512.png', 512, False),
    ('icon-maskable-512.png', 512, True),
    ('apple-touch-icon.png', 180, False),
]:
    write_png(root / name, size, size, make_icon(size, maskable))

(root / 'icon.svg').write_text(
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512">\n'
    '  <rect width="512" height="512" rx="88" fill="#0f172a"/>\n'
    '  <rect x="72" y="72" width="368" height="368" rx="72" fill="#111827"/>\n'
    '  <rect x="118" y="116" width="276" height="28" rx="14" fill="#38bdf8"/>\n'
    '  <rect x="140" y="278" width="56" height="94" rx="14" fill="#94a3b8"/>\n'
    '  <rect x="226" y="224" width="56" height="148" rx="14" fill="#22c55e"/>\n'
    '  <rect x="312" y="168" width="56" height="204" rx="14" fill="#38bdf8"/>\n'
    '  <circle cx="372" cy="156" r="42" fill="#22c55e"/>\n'
    '  <path d="M350 157l16 16 30-34" fill="none" stroke="#e5e7eb" stroke-width="16" stroke-linecap="round" stroke-linejoin="round"/>\n'
    '</svg>\n',
    encoding='utf-8',
)

print('generated icons in', root)
for path in sorted(root.iterdir()):
    print(path.name, os.path.getsize(path))

