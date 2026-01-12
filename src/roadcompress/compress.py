"""
RoadCompress - Compression Utilities for BlackRoad
Compress and decompress data with multiple algorithms.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, BinaryIO, Callable, Dict, List, Optional, Union
import base64
import gzip
import hashlib
import io
import json
import logging
import zlib

logger = logging.getLogger(__name__)


class CompressionAlgorithm(str, Enum):
    GZIP = "gzip"
    ZLIB = "zlib"
    DEFLATE = "deflate"
    LZ4 = "lz4"
    BROTLI = "brotli"


class CompressionLevel(int, Enum):
    NONE = 0
    FASTEST = 1
    FAST = 3
    DEFAULT = 6
    BEST = 9


@dataclass
class CompressionResult:
    data: bytes
    original_size: int
    compressed_size: int
    algorithm: CompressionAlgorithm
    level: CompressionLevel
    checksum: str
    ratio: float = 0.0

    def __post_init__(self):
        if self.original_size > 0:
            self.ratio = 1 - (self.compressed_size / self.original_size)


@dataclass
class DecompressionResult:
    data: bytes
    compressed_size: int
    decompressed_size: int
    checksum: str
    valid: bool = True


class Compressor:
    def compress(self, data: bytes, level: CompressionLevel = CompressionLevel.DEFAULT) -> bytes:
        raise NotImplementedError

    def decompress(self, data: bytes) -> bytes:
        raise NotImplementedError


class GzipCompressor(Compressor):
    def compress(self, data: bytes, level: CompressionLevel = CompressionLevel.DEFAULT) -> bytes:
        return gzip.compress(data, compresslevel=level.value)

    def decompress(self, data: bytes) -> bytes:
        return gzip.decompress(data)


class ZlibCompressor(Compressor):
    def compress(self, data: bytes, level: CompressionLevel = CompressionLevel.DEFAULT) -> bytes:
        return zlib.compress(data, level=level.value)

    def decompress(self, data: bytes) -> bytes:
        return zlib.decompress(data)


class DeflateCompressor(Compressor):
    def compress(self, data: bytes, level: CompressionLevel = CompressionLevel.DEFAULT) -> bytes:
        compressor = zlib.compressobj(level.value, zlib.DEFLATED, -zlib.MAX_WBITS)
        return compressor.compress(data) + compressor.flush()

    def decompress(self, data: bytes) -> bytes:
        return zlib.decompress(data, -zlib.MAX_WBITS)


class CompressionManager:
    def __init__(self):
        self.compressors: Dict[CompressionAlgorithm, Compressor] = {
            CompressionAlgorithm.GZIP: GzipCompressor(),
            CompressionAlgorithm.ZLIB: ZlibCompressor(),
            CompressionAlgorithm.DEFLATE: DeflateCompressor(),
        }

    def register(self, algorithm: CompressionAlgorithm, compressor: Compressor) -> None:
        self.compressors[algorithm] = compressor

    def compress(self, data: Union[bytes, str], algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP, level: CompressionLevel = CompressionLevel.DEFAULT) -> CompressionResult:
        if isinstance(data, str):
            data = data.encode('utf-8')
        
        original_size = len(data)
        checksum = hashlib.md5(data).hexdigest()
        
        compressor = self.compressors.get(algorithm)
        if not compressor:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
        
        compressed = compressor.compress(data, level)
        
        return CompressionResult(
            data=compressed,
            original_size=original_size,
            compressed_size=len(compressed),
            algorithm=algorithm,
            level=level,
            checksum=checksum
        )

    def decompress(self, data: bytes, algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP) -> DecompressionResult:
        compressor = self.compressors.get(algorithm)
        if not compressor:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
        
        compressed_size = len(data)
        
        try:
            decompressed = compressor.decompress(data)
            checksum = hashlib.md5(decompressed).hexdigest()
            return DecompressionResult(
                data=decompressed,
                compressed_size=compressed_size,
                decompressed_size=len(decompressed),
                checksum=checksum,
                valid=True
            )
        except Exception as e:
            logger.error(f"Decompression failed: {e}")
            return DecompressionResult(
                data=b"",
                compressed_size=compressed_size,
                decompressed_size=0,
                checksum="",
                valid=False
            )

    def compress_file(self, input_path: str, output_path: str, algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP, level: CompressionLevel = CompressionLevel.DEFAULT) -> CompressionResult:
        with open(input_path, 'rb') as f:
            data = f.read()
        
        result = self.compress(data, algorithm, level)
        
        with open(output_path, 'wb') as f:
            f.write(result.data)
        
        return result

    def decompress_file(self, input_path: str, output_path: str, algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP) -> DecompressionResult:
        with open(input_path, 'rb') as f:
            data = f.read()
        
        result = self.decompress(data, algorithm)
        
        if result.valid:
            with open(output_path, 'wb') as f:
                f.write(result.data)
        
        return result

    def compress_json(self, data: Any, algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP) -> str:
        json_str = json.dumps(data)
        result = self.compress(json_str, algorithm)
        return base64.b64encode(result.data).decode('utf-8')

    def decompress_json(self, data: str, algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP) -> Any:
        compressed = base64.b64decode(data)
        result = self.decompress(compressed, algorithm)
        if result.valid:
            return json.loads(result.data.decode('utf-8'))
        return None

    def estimate_ratio(self, data: Union[bytes, str], algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP) -> float:
        if isinstance(data, str):
            data = data.encode('utf-8')
        
        sample_size = min(len(data), 10000)
        sample = data[:sample_size]
        
        result = self.compress(sample, algorithm, CompressionLevel.DEFAULT)
        return result.ratio


class StreamCompressor:
    def __init__(self, algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP, level: CompressionLevel = CompressionLevel.DEFAULT):
        self.algorithm = algorithm
        self.level = level
        self._compressor = None
        self._init_compressor()

    def _init_compressor(self):
        if self.algorithm == CompressionAlgorithm.GZIP:
            self._compressor = gzip.GzipFile(mode='wb', fileobj=io.BytesIO(), compresslevel=self.level.value)
        elif self.algorithm == CompressionAlgorithm.ZLIB:
            self._compressor = zlib.compressobj(self.level.value)

    def compress_chunk(self, chunk: bytes) -> bytes:
        if self.algorithm == CompressionAlgorithm.ZLIB:
            return self._compressor.compress(chunk)
        return b""

    def flush(self) -> bytes:
        if self.algorithm == CompressionAlgorithm.ZLIB:
            return self._compressor.flush()
        return b""


class StreamDecompressor:
    def __init__(self, algorithm: CompressionAlgorithm = CompressionAlgorithm.GZIP):
        self.algorithm = algorithm
        self._decompressor = None
        self._init_decompressor()

    def _init_decompressor(self):
        if self.algorithm == CompressionAlgorithm.ZLIB:
            self._decompressor = zlib.decompressobj()

    def decompress_chunk(self, chunk: bytes) -> bytes:
        if self.algorithm == CompressionAlgorithm.ZLIB:
            return self._decompressor.decompress(chunk)
        return b""


def compress(data: Union[bytes, str], algorithm: str = "gzip") -> bytes:
    manager = CompressionManager()
    algo = CompressionAlgorithm(algorithm)
    result = manager.compress(data, algo)
    return result.data


def decompress(data: bytes, algorithm: str = "gzip") -> bytes:
    manager = CompressionManager()
    algo = CompressionAlgorithm(algorithm)
    result = manager.decompress(data, algo)
    return result.data


def example_usage():
    manager = CompressionManager()
    
    text = "Hello, World! " * 1000
    
    result = manager.compress(text, CompressionAlgorithm.GZIP)
    print(f"GZIP: {result.original_size} -> {result.compressed_size} ({result.ratio*100:.1f}% reduction)")
    
    result = manager.compress(text, CompressionAlgorithm.ZLIB)
    print(f"ZLIB: {result.original_size} -> {result.compressed_size} ({result.ratio*100:.1f}% reduction)")
    
    result = manager.compress(text, CompressionAlgorithm.DEFLATE)
    print(f"DEFLATE: {result.original_size} -> {result.compressed_size} ({result.ratio*100:.1f}% reduction)")
    
    compressed = manager.compress(text, CompressionAlgorithm.GZIP)
    decompressed = manager.decompress(compressed.data, CompressionAlgorithm.GZIP)
    print(f"\nDecompressed valid: {decompressed.valid}")
    print(f"Checksum match: {compressed.checksum == decompressed.checksum}")
    
    data = {"users": [{"name": "Alice", "age": 30} for _ in range(100)]}
    compressed_json = manager.compress_json(data)
    print(f"\nCompressed JSON length: {len(compressed_json)}")
    
    ratio = manager.estimate_ratio(text)
    print(f"Estimated compression ratio: {ratio*100:.1f}%")

