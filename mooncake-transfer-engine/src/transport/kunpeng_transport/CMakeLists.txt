file(GLOB UB_SOURCES "*.cpp" "urma/urma_endpoint.cpp" "ub_allocator.cpp")

# Check if liburma.so exists
find_library(URMA_LIBRARY urma PATHS /usr/lib64)

# Build ub_transport with real URMA library if available
# Never include mock_urma_api.cpp in ub_transport library
# For test, we will use mock_urma_api.cpp directly
if (URMA_LIBRARY)
    add_library(ub_transport OBJECT ${UB_SOURCES})
    target_link_libraries(ub_transport
            PUBLIC
            ${URMA_LIBRARY}
    )
    message(STATUS "Using real URMA library: ${URMA_LIBRARY}")
else()
    # If liburma.so not found, we'll need to handle this differently
    # For now, just build without mock_urma_api.cpp
    list(APPEND UB_SOURCES "urma/mock_urma.cpp")
    add_library(ub_transport OBJECT ${UB_SOURCES})
    message(WARNING "Not Found liburma.so building ub_transport with Mock URMA library")
endif()
target_include_directories(ub_transport
        PUBLIC
        ${urma_INCLUDE_DIR}
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../../../../mooncake-integration/store
)

# Probe whether the URMA headers expose urma_seg_cfg_t::is_gpu_seg, required
# for the gdr-peermem GPU-Direct path. When absent (older UMDK), the build
# still succeeds; gdr-peermem is simply unavailable and the transport falls
# back to host/staging modes only.
include(CheckCXXSourceCompiles)
set(_UB_SAVED_CMAKE_REQUIRED_INCLUDES ${CMAKE_REQUIRED_INCLUDES})
set(CMAKE_REQUIRED_INCLUDES ${urma_INCLUDE_DIR})
check_cxx_source_compiles("
#include <urma_api.h>
int main() {
    urma_seg_cfg_t cfg = {};
    cfg.is_gpu_seg = 1;
    return 0;
}
" HAVE_URMA_SEG_CFG_IS_GPU_SEG)
set(CMAKE_REQUIRED_INCLUDES ${_UB_SAVED_CMAKE_REQUIRED_INCLUDES})
unset(_UB_SAVED_CMAKE_REQUIRED_INCLUDES)
if(HAVE_URMA_SEG_CFG_IS_GPU_SEG)
    target_compile_definitions(ub_transport
            PRIVATE HAVE_URMA_SEG_CFG_IS_GPU_SEG=1)
    message(STATUS "URMA headers expose urma_seg_cfg_t::is_gpu_seg; gdr-peermem enabled")
else()
    message(STATUS "URMA headers lack urma_seg_cfg_t::is_gpu_seg; gdr-peermem disabled (host/staging only)")
endif()

target_link_libraries(ub_transport
        PRIVATE
        JsonCpp::JsonCpp
        glog::glog
        pthread
        SpDiag::spdiag_lib
)