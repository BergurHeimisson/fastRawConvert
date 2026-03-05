#!/opt/homebrew/bin/python3.12
"""
Fast multi-threaded CR3 -> JPEG converter using LibRaw (via rawpy).

Usage:
    python3 convert_raw.py <input_dir> [options]

Options:
    --quality INT       JPEG quality 1-95  (default: 90)
    --workers INT       Thread count       (default: CPU count)
    --half-size         Decode at half resolution for extra speed
    --linear            Use linear demosaicing (fastest, lower quality)
    --recursive         Search subdirectories for CR3 files
"""

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import rawpy
from PIL import Image


def output_path(src: Path, jpeg_root: Path) -> Path:
    return jpeg_root / src.with_suffix(".jpg").name


def convert_one(src: Path, jpeg_root: Path, quality: int, half_size: bool, use_linear: bool) -> tuple[Path, float, str | None]:
    t0 = time.perf_counter()
    dst = output_path(src, jpeg_root)
    try:
        with rawpy.imread(str(src)) as raw:
            params = rawpy.Params(
                use_camera_wb=True,
                no_auto_bright=False,
                output_bps=8,
                half_size=half_size,
                demosaic_algorithm=rawpy.DemosaicAlgorithm.LINEAR if use_linear else rawpy.DemosaicAlgorithm.AHD,
            )
            rgb = raw.postprocess(params)
        img = Image.fromarray(rgb)
        img.save(str(dst), format="JPEG", quality=quality, optimize=False, subsampling=2)
        elapsed = time.perf_counter() - t0
        return src, elapsed, None
    except Exception as exc:
        elapsed = time.perf_counter() - t0
        return src, elapsed, str(exc)


def find_cr3_files(input_dir: Path, recursive: bool) -> list[Path]:
    if recursive:
        return sorted(input_dir.rglob("*.[Cc][Rr]3"))
    return sorted(input_dir.glob("*.[Cc][Rr]3"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Fast CR3 -> JPEG batch converter")
    parser.add_argument("input_dir", help="Directory containing CR3 files")
    parser.add_argument("--quality", type=int, default=90, metavar="INT", help="JPEG quality (default: 90)")
    parser.add_argument("--workers", type=int, default=os.cpu_count(), metavar="INT", help="Thread count (default: CPU count)")
    parser.add_argument("--half-size", action="store_true", help="Decode at half resolution (2x faster decoding)")
    parser.add_argument("--linear", action="store_true", help="Use linear demosaicing (fastest, lower quality)")
    parser.add_argument("--recursive", action="store_true", help="Search subdirectories for CR3 files")
    args = parser.parse_args()

    input_dir = Path(args.input_dir).resolve()
    if not input_dir.is_dir():
        sys.exit(f"Error: '{input_dir}' is not a directory.")

    jpeg_root = input_dir / "jpeg"
    jpeg_root.mkdir(exist_ok=True)

    files = find_cr3_files(input_dir, args.recursive)
    if not files:
        sys.exit(f"No CR3 files found in '{input_dir}'.")

    total = len(files)
    workers = min(args.workers, total)

    print(f"Found {total} CR3 file(s)  |  output -> {jpeg_root}")
    print(f"Workers: {workers}  |  Quality: {args.quality}  |  Half-size: {args.half_size}  |  Linear demosaic: {args.linear}")
    print("-" * 60)

    wall_start = time.perf_counter()
    done = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(convert_one, f, jpeg_root, args.quality, args.half_size, args.linear): f
            for f in files
        }
        for fut in as_completed(futures):
            src, elapsed, err = fut.result()
            done += 1
            if err:
                errors += 1
                print(f"  [{done:>{len(str(total))}}/{total}] ERROR  {src.name}: {err}")
            else:
                size_mb = src.stat().st_size / 1_048_576
                print(f"  [{done:>{len(str(total))}}/{total}]  {elapsed:5.2f}s  {size_mb:6.1f} MB  {src.name}")

    wall = time.perf_counter() - wall_start
    rate = total / wall if wall > 0 else 0
    print("-" * 60)
    print(f"Done: {total - errors}/{total} converted in {wall:.1f}s  ({rate:.2f} files/s)")
    if errors:
        print(f"  {errors} error(s) — check messages above.")


if __name__ == "__main__":
    main()
