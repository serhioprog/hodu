"""
Perceptual hash utilities for image-based duplicate detection.

We use 64-bit pHash (16 hex chars). Hamming distance ≤ 6 is treated
as "same image" — a balance between robustness to JPEG artifacts/scaling
and avoiding false matches on visually different photos.

Smart matching (added in Partition 3):
  Some hashes are "stock photos" — beach shots, sunsets, agency logos
  that appear across many unrelated listings on the same domain.
  These are noise, not duplicate signals. PHashService.count_matching
  accepts an optional `common_to_ignore` set; hashes in this set are
  skipped on both sides of the comparison.

  The detector builds this set once per run via SQL aggregation
  (COUNT(DISTINCT property_id) > N per domain).
"""
from __future__ import annotations

from typing import Iterable, Optional

import io

from PIL import Image, UnidentifiedImageError
import imagehash

class PHashService:
    """
    Stateless utility class for perceptual hash comparison.

    All methods are static — pHash math has no instance state.
    """

    # Hamming distance threshold for "same image" verdict.
    # 6 bits out of 64 = ~9% difference — tolerant to JPEG re-compression
    # and minor cropping, but strict enough to reject distinct photos.
    HAMMING_THRESHOLD: int = 6

    @staticmethod
    def compute_from_bytes(image_bytes: bytes) -> str | None:
        """
        Compute a 64-bit perceptual hash from raw image bytes.

        Returns:
            Hex string of length 16 (e.g. "8f1e3c5a7b9d0f24") on success.
            None if the bytes are not a valid image, the image is too small
            to hash, or any other failure occurs.

        We never raise — pHash failure on a single photo should not break
        the whole download pipeline. The caller stores None and the photo
        simply doesn't participate in image-overlap comparisons.
        """
        if not image_bytes:
            return None

        try:
            with Image.open(io.BytesIO(image_bytes)) as img:
                # imagehash.phash needs a non-tiny image. Reject anything
                # that's clearly not a real photo (icon, 1x1 tracker, etc.)
                if img.width < 32 or img.height < 32:
                    return None
                # phash() returns an ImageHash; stringification gives hex.
                return str(imagehash.phash(img))
        except (UnidentifiedImageError, OSError, ValueError) as e:
            # Common: corrupted bytes, weird formats, EXIF errors
            return None
        except Exception:
            # Anything else — also swallow. We're a utility, not a watchdog.
            return None

    @staticmethod
    def _hamming_distance(hash_a: str, hash_b: str) -> int:
        """
        Hamming distance between two hex pHash strings.

        Both strings must be the same length (16 hex chars for 64-bit pHash).
        Returns int distance in bits. Returns sys.maxsize on parse failure
        so the caller's threshold check fails safely.
        """
        if not hash_a or not hash_b or len(hash_a) != len(hash_b):
            return 1 << 30  # treat as "infinitely different"
        try:
            return bin(int(hash_a, 16) ^ int(hash_b, 16)).count("1")
        except (ValueError, TypeError):
            return 1 << 30

    @classmethod
    def is_same_image(cls, hash_a: str, hash_b: str) -> bool:
        """True iff the two hashes are within HAMMING_THRESHOLD bits."""
        return cls._hamming_distance(hash_a, hash_b) <= cls.HAMMING_THRESHOLD

    @classmethod
    def count_matching(
        cls,
        hashes_a: Iterable[str],
        hashes_b: Iterable[str],
        common_to_ignore: Optional[set[str]] = None,
    ) -> int:
        """
        Count how many photos in `hashes_a` have a near-duplicate in `hashes_b`.

        Each photo from A counts at most once. Order does not matter.

        Args:
            hashes_a: pHash list of property A's photos
            hashes_b: pHash list of property B's photos
            common_to_ignore: pHashes known to be stock/template photos
                              (appearing across many unrelated listings).
                              These are skipped on BOTH sides.

        Returns:
            Number of matched pairs (capped by min(len_a, len_b)).
            0 if either side is empty.
        """
        if common_to_ignore is None:
            common_to_ignore = set()

        # Filter out garbage and stock hashes upfront
        list_a = [h for h in hashes_a if h and h not in common_to_ignore]
        list_b = [h for h in hashes_b if h and h not in common_to_ignore]

        if not list_a or not list_b:
            return 0

        used_b: set[int] = set()
        matches = 0

        for ha in list_a:
            for j, hb in enumerate(list_b):
                if j in used_b:
                    continue
                if cls.is_same_image(ha, hb):
                    used_b.add(j)
                    matches += 1
                    break  # one A-photo matches at most one B-photo

        return matches