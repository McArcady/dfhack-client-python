cmake_minimum_required(VERSION 3.13)
project(blendwarf NONE)

set(PROTOC-BIN "protoc")
set(PROTO-DIR-CORE    "${CMAKE_CURRENT_SOURCE_DIR}/../dfhack/library/proto")
set(PROTO-DIR-PLUGINS "${CMAKE_CURRENT_SOURCE_DIR}/../dfhack/plugins/proto")
set(PY-DIR "${CMAKE_CURRENT_BINARY_DIR}/py_export")

add_custom_command(OUTPUT protos
  COMMAND mkdir -p ${PY-DIR}
  COMMAND touch ${PY-DIR}/__init__.py
  COMMAND "${PROTOC-BIN}"
  -I${PROTO-DIR-CORE}
  -I${PROTO-DIR-PLUGINS}
  --python_out=${PY-DIR}
  ${PROTO-DIR-PLUGINS}/RemoteFortressReader.proto
  ${PROTO-DIR-PLUGINS}/ItemdefInstrument.proto
  ${PROTO-DIR-CORE}/CoreProtocol.proto
  ${PROTO-DIR-CORE}/Basic.proto
  ${PROTO-DIR-CORE}/BasicApi.proto
)

add_custom_target(DONE ALL
  DEPENDS protos
)
