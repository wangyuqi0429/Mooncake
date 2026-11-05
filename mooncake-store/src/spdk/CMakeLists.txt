set(SPDK_WRAPPER_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/spdk_wrapper.cpp
)

# Modified By Yida (v3): NoF GPU Direct——buffer 类型识别 + SPDK memory
# domain/CUDA provider 管理
if(USE_NOF_URMA)
  list(APPEND SPDK_WRAPPER_SOURCES
      ${CMAKE_CURRENT_SOURCE_DIR}/nof_memory_domain.cpp
  )
endif()

set(SPDK_WRAPPER_SOURCES ${SPDK_WRAPPER_SOURCES} PARENT_SCOPE)