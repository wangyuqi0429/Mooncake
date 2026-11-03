"""Descriptor validation without executing device code."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import torch

from mooncake.sparse_copy import sparse_copy


def descriptor(address, dtype, size=1):
    value = MagicMock(spec=torch.Tensor)
    value.device = SimpleNamespace(type="npu", index=0)
    value.ndim = 1
    value.dtype = dtype
    value.is_contiguous.return_value = True
    value.numel.return_value = size
    value.element_size.return_value = 8 if dtype == torch.int64 else 4
    value.data_ptr.return_value = address
    return value


def test_status_cannot_alias_count_or_length_descriptors():
    src = descriptor(4096, torch.int64)
    dst = descriptor(8192, torch.int64)
    lengths = descriptor(12288, torch.int32)
    count = descriptor(16384, torch.int32)
    for address in (12288, 16384, 4100):
        status = descriptor(address, torch.int32)
        with pytest.raises(ValueError, match="overlap"):
            sparse_copy(src, dst, lengths, count, status)
        status.zero_.assert_not_called()


def test_mismatched_capacity_is_rejected_before_launch():
    with pytest.raises(ValueError, match="capacities"):
        sparse_copy(
            descriptor(4096, torch.int64, 2),
            descriptor(8192, torch.int64, 1),
            descriptor(12288, torch.int32, 2),
            descriptor(16384, torch.int32),
            descriptor(20480, torch.int32),
        )


def test_external_stream_cannot_select_a_different_or_unknown_npu():
    arguments = (
        descriptor(4096, torch.int64), descriptor(8192, torch.int64),
        descriptor(12288, torch.int32), descriptor(16384, torch.int32), descriptor(20480, torch.int32),
    )
    for stream in (SimpleNamespace(device_index=1), SimpleNamespace()):
        with pytest.raises(ValueError, match="external stream"):
            sparse_copy(*arguments, stream=stream)
