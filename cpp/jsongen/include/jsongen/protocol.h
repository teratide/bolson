#pragma once

#include <variant>

#include "jsongen/raw_protocol.h"
#include "jsongen/zmq_protocol.h"

namespace jsongen {

using StreamProtocol = std::variant<RawProtocol, ZMQProtocol>;

}