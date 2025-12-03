"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";

import { LoadByRegionChart } from "../components/charts/LoadByRegionChart";
import { HeatwavesByZoneChart } from "../components/charts/HeatwavesByZoneChart";
import { ExtremeHeatLoadTable } from "../components/charts/ExtremeHeatLoadTable";
import { DryVsRainyChart } from "../components/charts/DryVsRainyChart";
import { DateRangeFilter } from "../components/filters/DateRangeFilter";
import { useDateFilter } from "../contexts/DateFilterContext";

type Region = "all" | "North" | "South" | "West" | "Houston";

export default function Home() {
  const router = useRouter();
  const { dateRange } = useDateFilter();

  // filter state
  const [regionForHourly, setRegionForHourly] = useState<Region>("all");
  const [zoneForHeatwaves, setZoneForHeatwaves] = useState<Region>("all");
  const [regionForDryRainy, setRegionForDryRainy] = useState<Region>("all");

  // nav menu state
  const [isMenuOpen, setIsMenuOpen] = useState(false);

  const handleAnalysisClick = () => {
    router.refresh(); // “reload” analysis page
    setIsMenuOpen(false);
  };

  const handleForecastingClick = () => {
    router.push("/forecasting"); // later create app/forecasting/page.tsx
    setIsMenuOpen(false);
  };

  return (
    <div className="min-h-screen bg-[#f5f5f7]">
      {/* Top banner - Penn themed */}
      <header className="relative w-full bg-[#011F5B] text-white px-10 py-4 shadow-md">
        <div className="flex items-center justify-between w-full">
          {/* Left: app name */}
          <span className="text-2xl font-semibold tracking-tight">
            Forecast.AI
          </span>

          {/* Right: UPenn + nav menu */}
          <div className="flex items-center gap-4">
            <span className="hidden sm:inline text-sm tracking-wide text-[#C1D3EB]">
              University of Pennsylvania
            </span>

            {/* Hamburger button */}
            <button
              type="button"
              onClick={() => setIsMenuOpen((prev) => !prev)}
              className="flex flex-col justify-center items-center gap-[4px] w-9 h-9 rounded-md bg-white/10 hover:bg-white/20 transition"
              aria-label="Open navigation menu"
            >
              <span className="w-5 h-[2px] bg-white rounded-full" />
              <span className="w-5 h-[2px] bg-white rounded-full" />
              <span className="w-5 h-[2px] bg-white rounded-full" />
            </button>
          </div>

          {/* Dropdown menu */}
          {isMenuOpen && (
            <div className="absolute right-10 top-14 w-40 rounded-md bg-white text-sm text-gray-800 shadow-lg border border-gray-200 z-10">
              <button
                type="button"
                onClick={handleAnalysisClick}
                className="w-full text-left px-3 py-2 hover:bg-gray-100"
              >
                Analysis
              </button>
              <button
                type="button"
                onClick={handleForecastingClick}
                className="w-full text-left px-3 py-2 hover:bg-gray-100 border-t border-gray-200"
              >
                Forecasting
              </button>
            </div>
          )}
        </div>
      </header>

      {/* Main content area */}
      <main className="p-8">
        {/* Date Filter */}
        <div className="mb-6">
          <DateRangeFilter />
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-10">
          {/* Top Left – has region filter */}
          <div className="h-80 rounded-xl border border-[#d3dbe8] bg-white flex flex-col pt-4 px-4 shadow-sm">
            <span className="text-[#011F5B] font-semibold text-center mb-2">
              Electricity Hourly Load by Region
            </span>
            <div className="flex-1">
              <LoadByRegionChart
                selectedRegion={regionForHourly}
                dateRange={dateRange}
              />
            </div>
            <div className="mt-3 flex justify-start items-center">
              <label className="mr-2 text-xs text-gray-600">Region:</label>
              <select
                className="border border-gray-300 rounded px-2 py-1 text-xs"
                value={regionForHourly}
                onChange={(e) =>
                  setRegionForHourly(e.target.value as Region)
                }
              >
                <option value="all">All Regions</option>
                <option value="North">North</option>
                <option value="South">South</option>
                <option value="West">West</option>
                <option value="Houston">Houston</option>
              </select>
            </div>
          </div>

          {/* Top Right – has zone filter */}
          <div className="h-80 rounded-xl border border-[#d3dbe8] bg-white flex flex-col pt-4 px-4 shadow-sm">
            <span className="text-[#011F5B] font-semibold text-center mb-2">
              ERCOT Heatwave Strikes by Zone
            </span>
            <div className="flex-1">
              <HeatwavesByZoneChart
                selectedZone={zoneForHeatwaves}
                dateRange={dateRange}
              />
            </div>
            <div className="mt-3 flex justify-start items-center">
              <label className="mr-2 text-xs text-gray-600">Zone:</label>
              <select
                className="border border-gray-300 rounded px-2 py-1 text-xs"
                value={zoneForHeatwaves}
                onChange={(e) =>
                  setZoneForHeatwaves(e.target.value as Region)
                }
              >
                <option value="all">All Zones</option>
                <option value="North">North</option>
                <option value="South">South</option>
                <option value="West">West</option>
                <option value="Houston">Houston</option>
              </select>
            </div>
          </div>

          {/* Bottom Left – no region filter */}
          <div className="h-80 rounded-xl border border-[#d3dbe8] bg-white flex flex-col pt-4 px-4 shadow-sm">
            <span className="text-[#011F5B] font-semibold text-center mb-2">
              Electricity Load on Extreme-Heat Days
            </span>
            <div className="flex-1 overflow-hidden">
              <ExtremeHeatLoadTable dateRange={dateRange} />
            </div>
          </div>

          {/* Bottom Right – has region filter */}
          <div className="h-80 rounded-xl border border-[#d3dbe8] bg-white flex flex-col pt-4 px-4 shadow-sm">
            <span className="text-[#011F5B] font-semibold text-center mb-2">
              Average Electricity Load for Dry vs. Rainy Days
            </span>
            <div className="flex-1">
              <DryVsRainyChart
                selectedRegion={regionForDryRainy}
                dateRange={dateRange}
              />
            </div>
            <div className="mt-3 flex justify-start items-center">
              <label className="mr-2 text-xs text-gray-600">Region:</label>
              <select
                className="border border-gray-300 rounded px-2 py-1 text-xs"
                value={regionForDryRainy}
                onChange={(e) =>
                  setRegionForDryRainy(e.target.value as Region)
                }
              >
                <option value="all">All Regions</option>
                <option value="North">North</option>
                <option value="South">South</option>
                <option value="West">West</option>
                <option value="Houston">Houston</option>
              </select>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
