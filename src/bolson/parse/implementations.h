// Copyright 2020 Teratide B.V.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <CLI/CLI.hpp>

#include "bolson/parse/arrow.h"
#include "bolson/parse/custom/battery.h"
#include "bolson/parse/custom/trip.h"
#include "bolson/parse/fpga/battery.h"
#include "bolson/parse/fpga/trip.h"
#include "bolson/parse/opae/battery.h"
#include "bolson/parse/opae/trip.h"

namespace bolson::parse {

/// Available parser implementations.
enum class Impl {
  ARROW,         ///< A CPU version based on Arrow's internal JSON parser using RapidJSON.
  OPAE_BATTERY,  ///< An FPGA version for the "battery status" schema using Intel OPAE.
  OPAE_TRIP,     ///< An FPGA version for for the "trip report" schema using Intel OPAE.
  CUSTOM_BATTERY,  ///< A hand-optimized CPU converter for the "battery status" schema
  CUSTOM_TRIP,     ///< A hand-optimized CPU converter for the "battery status" schema
  FPGA_BATTERY,    ///< An FPGA version for the "battery status" schema using Fletcher.
  FPGA_TRIP,       ///< An FPGA version for the "trip report" schema using Fletcher.
};

/// All parser options.
struct ParserOptions {
  // Would have been nice to use a variant, but it doesn't work nicely with the CLI stuff.
  Impl impl = Impl::ARROW;
  ArrowOptions arrow;
  opae::BatteryOptions opae_battery;
  opae::TripOptions opae_trip;
  custom::BatteryOptions custom_battery;
  custom::TripOptions custom_trip;
  fpga::BatteryOptions fpga_battery;
  fpga::TripOptions fpga_trip;

  static auto impls_map() -> std::map<std::string, parse::Impl> {
    static std::map<std::string, parse::Impl> result = {
        {"arrow", parse::Impl::ARROW},
        {"opae-battery", parse::Impl::OPAE_BATTERY},
        {"opae-trip", parse::Impl::OPAE_TRIP},
        {"custom-battery", parse::Impl::CUSTOM_BATTERY},
        {"custom-trip", parse::Impl::CUSTOM_TRIP},
        {"fpga-battery", parse::Impl::FPGA_BATTERY},
        {"fpga-trip", parse::Impl::FPGA_TRIP}};
    return result;
  }
};

inline void AddParserOptions(CLI::App* sub, ParserOptions* opts) {
  sub->add_option("-p,--parser", opts->impl,
                  "Parser implementation. FPGA parsers have fixed schema.")
      ->transform(CLI::CheckedTransformer(ParserOptions::impls_map(), CLI::ignore_case))
      ->default_val(parse::Impl::ARROW);

  parse::AddArrowOptionsToCLI(sub, &opts->arrow);
  parse::opae::AddBatteryOptionsToCLI(sub, &opts->opae_battery);
  parse::opae::AddTripOptionsToCLI(sub, &opts->opae_trip);
  parse::custom::AddBatteryOptionsToCLI(sub, &opts->custom_battery);
  parse::custom::AddTripOptionsToCLI(sub, &opts->custom_trip);
  parse::fpga::AddBatteryOptionsToCLI(sub, &opts->fpga_battery);
  parse::fpga::AddTripOptionsToCLI(sub, &opts->fpga_trip);
}

inline auto ToString(const Impl& impl) -> std::string {
  switch (impl) {
    case Impl::ARROW:
      return "Arrow (CPU)";
    case Impl::OPAE_BATTERY:
      return "OPAE battery status (FPGA)";
    case Impl::OPAE_TRIP:
      return "OPAE trip report (FPGA)";
    case Impl::CUSTOM_BATTERY:
      return "Custom battery status (CPU)";
    case Impl::CUSTOM_TRIP:
      return "Custom trip report (CPU)";
    case Impl::FPGA_BATTERY:
      return "Fletcher battery status (FPGA)";
    case Impl::FPGA_TRIP:
      return "Fletcher trip report (FPGA)";
  }
  // C++ why
  return "Corrupt bolson::parse::Impl enum value.";
}

}  // namespace bolson::parse