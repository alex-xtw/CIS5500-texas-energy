"use client";

import { useState } from "react";
import {
  LineChart,
  Line,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";

type Region = "North" | "South" | "West" | "Houston" | "All";

const historicalForecastData = {
  North: [
    { time: "Day 1", historical: 2600, forecast: 2650 },
    { time: "Day 2", historical: 2700, forecast: 2750 },
    { time: "Day 3", historical: 2900, forecast: 2880 },
    { time: "Day 4", historical: 3100, forecast: 3050 },
    { time: "Day 5", historical: 3000, forecast: 3020 },
  ],
  South: [
    { time: "Day 1", historical: 2500, forecast: 2480 },
    { time: "Day 2", historical: 2600, forecast: 2630 },
    { time: "Day 3", historical: 2750, forecast: 2780 },
    { time: "Day 4", historical: 2950, forecast: 2920 },
    { time: "Day 5", historical: 2850, forecast: 2870 },
  ],
  West: [
    { time: "Day 1", historical: 2300, forecast: 2320 },
    { time: "Day 2", historical: 2400, forecast: 2430 },
    { time: "Day 3", historical: 2550, forecast: 2520 },
    { time: "Day 4", historical: 2650, forecast: 2680 },
    { time: "Day 5", historical: 2600, forecast: 2610 },
  ],
  Houston: [
    { time: "Day 1", historical: 2800, forecast: 2820 },
    { time: "Day 2", historical: 2950, forecast: 2970 },
    { time: "Day 3", historical: 3100, forecast: 3120 },
    { time: "Day 4", historical: 3250, forecast: 3230 },
    { time: "Day 5", historical: 3150, forecast: 3180 },
  ],
} as const;

const regionOptions: Region[] = ["All", "North", "South", "West", "Houston"];

export function HistoricalForecastCard() {
  const [selectedRegion, setSelectedRegion] = useState<Region>("All");

  const chartRegionKey =
    selectedRegion === "All" ? "North" : selectedRegion;

  const chartData =
    historicalForecastData[chartRegionKey as Exclude<Region, "All">];

  return (
    <div className="h-80 rounded-xl border border-[#d3dbe8] bg-white flex flex-col pt-4 px-4 shadow-sm">
      <div className="flex items-center justify-between mb-2">
        <span className="text-[#011F5B] font-semibold text-center flex-1">
          Historical vs Forecasted Load by Region
        </span>
        <div className="flex items-center gap-2">
          <label className="text-xs text-gray-600">Region:</label>
          <select
            className="border border-gray-300 rounded px-2 py-1 text-xs"
            value={selectedRegion}
            onChange={(e) =>
              setSelectedRegion(e.target.value as Region)
            }
          >
            {regionOptions.map((r) => (
              <option key={r} value={r}>
                {r}
              </option>
            ))}
          </select>
        </div>
      </div>
      <div className="flex-1">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart
            data={chartData}
            margin={{ top: 10, right: 20, left: 0, bottom: 0 }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="time" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Line
              type="monotone"
              dataKey="historical"
              stroke="#011F5B"
              name="Historical"
            />
            <Line
              type="monotone"
              dataKey="forecast"
              stroke="#990000"
              name="Forecast"
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
