"""Read-only layout metadata contract; requires an installed Mooncake binding."""

from dataclasses import FrozenInstanceError
from unittest.mock import MagicMock, patch

import pytest
import torch

from mooncake.shared_segment import (
    SharedSegment,
    SharedSegmentError,
    _BlockSpec,
    _build_layout,
)


def test_block_layout_matches_interleaved_entries_without_padding_payload():
    specs = {
        "k": _BlockSpec(2, (2, 4), torch.bfloat16, 16),
        "v": _BlockSpec(3, (3, 4), torch.bfloat16, 24),
    }
    offsets, stride, total = _build_layout(specs, sorted(specs))
    native = MagicMock()
    native.device_addr.return_value = 8192
    native.size.return_value = total
    segment = SharedSegment(native, specs, offsets, stride, 0)
    layout = segment.block_layout("v")
    assert layout.offset == 4096
    assert layout.entry_stride == 8192
    assert layout.nbytes == 24
    assert layout.count == 3
    assert layout.shape == (3, 4)
    assert segment.device_addr() == 8192
    with pytest.raises(FrozenInstanceError):
        layout.offset = 0
    with pytest.raises(SharedSegmentError):
        segment.block_layout("unknown")


def test_host_only_mapping_has_no_device_address():
    native = MagicMock()
    native.device_addr.return_value = 0
    segment = SharedSegment(native, {}, {}, 0, 0)
    assert segment.device_addr() == 0


def test_host_views_do_not_use_device_address_when_domains_differ():
    spec = _BlockSpec(1, (2, 4), torch.bfloat16, 16)
    native = MagicMock()
    native.device_addr.return_value = 8192
    native.base_addr.return_value = 4096
    segment = SharedSegment(native, {"k": spec}, {"k": 0}, 4096, 0)
    with patch("mooncake.shared_segment._tensor_from_host_addr") as host:
        segment.host_tensors("k")
    host.assert_called_once_with(4096, 16, (2, 4), torch.bfloat16, segment)
