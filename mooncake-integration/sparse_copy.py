"""Native device-address sparse bytes, using the caller's current NPU stream.

Descriptors and pointed-to buffers must remain alive until their consumer stream
has completed, and throughout any graph using them. Source/destination ranges
must be registered device mappings and must not overlap within a batch. This
module moves bytes only; callers own allocation, layout, selection and bounds.
Pointed-to ranges must not overlap descriptors or status storage: concurrent
self-modification of descriptors is unsupported.
"""

from __future__ import annotations

from contextlib import nullcontext

import torch


def sparse_copy(
    srcs: torch.Tensor,
    dsts: torch.Tensor,
    lengths: torch.Tensor,
    count: torch.Tensor,
    status: torch.Tensor,
    *,
    stream=None,
    reset_status: bool = True,
) -> None:
    """Launch a fixed-capacity device descriptor batch on the current stream.

    Entries with zero length perform no access. Dynamic values must satisfy
    0 <= count <= capacity and nonnegative lengths. Status is reset each call:
    bit 1 reports invalid count (no accesses), bit 2 negative lengths (that item
    is skipped). Callers must check status after execution before consuming
    outputs. This entry cannot infer allocation capacities from raw pointers.
    """
    if srcs.device.type != "npu" or any(t.device != srcs.device for t in (dsts, lengths, count, status)):
        raise ValueError("Sparse copy descriptors must be on one NPU")
    if any(not t.is_contiguous() or t.ndim != 1 for t in (srcs, dsts, lengths, count, status)):
        raise ValueError("Sparse copy descriptors must be contiguous one-dimensional tensors")
    if srcs.dtype != torch.int64 or dsts.dtype != torch.int64:
        raise ValueError("Sparse copy addresses must use int64")
    if lengths.dtype not in (torch.int32, torch.int64) or count.dtype not in (torch.int32, torch.int64):
        raise ValueError("Sparse copy lengths/count must use int32 or int64")
    if count.numel() != 1 or srcs.numel() != dsts.numel() or srcs.numel() != lengths.numel():
        raise ValueError("Sparse copy descriptor capacities must match and count must contain one element")
    if status.numel() != 1 or status.dtype != torch.int32:
        raise ValueError("Sparse copy status must be one int32 element")
    status_start = status.data_ptr()
    status_end = status_start + status.numel() * status.element_size()
    for descriptor in (srcs, dsts, lengths, count):
        start = descriptor.data_ptr()
        end = start + descriptor.numel() * descriptor.element_size()
        if descriptor.numel() and status_start < end and start < status_end:
            raise ValueError("Sparse copy status must not overlap descriptor storage")
    if stream is not None:
        stream_device = getattr(stream, "device", None)
        if isinstance(stream_device, int):
            matches = stream_device == srcs.device.index
        elif stream_device is not None:
            matches = stream_device == srcs.device
        else:
            index = getattr(stream, "device_index", None)
            matches = isinstance(index, int) and index == srcs.device.index
        if not matches:
            raise ValueError("Sparse copy external stream must belong to the descriptor NPU")
    from ._sparse_copy_kernel import sparse_copy_kernel

    with torch.npu.device(srcs.device):
        with torch.npu.stream(stream) if stream is not None else nullcontext():
            if reset_status:
                status.zero_()
            sparse_copy_kernel[(max(srcs.numel(), 1),)](
                srcs, dsts, lengths, count, status, CAPACITY=srcs.numel(), TILE=1024
            )
