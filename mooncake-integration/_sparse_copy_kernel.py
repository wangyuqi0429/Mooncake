# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright contributors to the vLLM project
# Adapted from vllm-ascend/vllm_ascend/ops/triton/batch_memcpy.py,
# itself adapted from vLLM v1 worker mamba_utils.py.
"""Ascend byte-range kernel; imported lazily by sparse_copy after setup."""

import triton
import triton.language as tl


@triton.jit
def sparse_copy_kernel(srcs, dsts, lengths, count, status, CAPACITY: tl.constexpr, TILE: tl.constexpr):
    item = tl.program_id(0)
    valid_count = tl.load(count)
    if item == 0 and (valid_count < 0 or valid_count > CAPACITY):
        tl.atomic_or(status, 1)
    if valid_count >= 0 and valid_count <= CAPACITY and item < valid_count:
        length = tl.load(lengths + item).to(tl.int64)
        if length < 0:
            tl.atomic_or(status, 2)
        # Resolve each dynamic pointer once, outside the byte loop. All
        # addresses must already be device-accessible (HostRegister or VMM).
        source = tl.load(srcs + item).to(tl.uint64).to(tl.pointer_type(tl.uint8))
        destination = tl.load(dsts + item).to(tl.uint64).to(tl.pointer_type(tl.uint8))
        indices = tl.arange(0, TILE)
        offset = tl.full((), 0, tl.int64)
        while offset < length:
            mask = indices + offset < length
            values = tl.load(source + offset + indices, mask=mask, other=0)
            tl.store(destination + offset + indices, values, mask=mask)
            offset += TILE
