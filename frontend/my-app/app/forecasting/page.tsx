"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";

import { HistoricalForecastCard } from "../../components/forecasting/HistoricalForecastCard";
import { MetricsCard } from "../../components/forecasting/MetricsCard";
import { OutlierDetectionCard } from "../../components/forecasting/OutlierDetectionCard";
import { ForecastConfigCard } from "../../components/forecasting/ForecastConfigCard";
import { DateRangeFilter } from "../../components/filters/DateRangeFilter";
import { useDateFilter } from "../../contexts/DateFilterContext";


export default function ForecastingPage() {
  const router = useRouter();
  const { dateRange } = useDateFilter();
  const [isMenuOpen, setIsMenuOpen] = useState(false);
  const [selectedModel, setSelectedModel] = useState("Statistical Model");

  const handleAnalysisClick = () => {
    router.push("/"); // back to analysis page
    setIsMenuOpen(false);
  };

  const handleForecastingClick = () => {
    router.refresh(); // already on forecasting
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

      {/* Main content */}
      <main className="p-8">
        {/* Date Filter */}
        <div className="mb-6">
          <DateRangeFilter />
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-10">
          <HistoricalForecastCard dateRange={dateRange} selectedModel={selectedModel} />
          <MetricsCard dateRange={dateRange} selectedModel={selectedModel} />
          <OutlierDetectionCard dateRange={dateRange} />
          <ForecastConfigCard dateRange={dateRange} selectedModel={selectedModel} onModelChange={setSelectedModel} />
        </div>
      </main>
    </div>
  );
}
