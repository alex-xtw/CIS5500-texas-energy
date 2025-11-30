"use client";

import { useState } from "react";
import {
  ScatterChart,
  Scatter,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  ReferenceLine,
  ResponsiveContainer,
} from "recharts";

type Region = "North" | "South" | "West" | "Houston" | "All";

const residualsData = [
  { time: "Day 1", region: "North", residual: 30 },
  { time: "Day 2", region: "North", residual: 45 },
  { time: "Day 3", region: "North", residual: -20 },
  { time: "Day 4", region: "North", residual: 140 }, // outlier
  { time: "Day 5", region: "North", residual: -15 },

  { time: "Day 1", region: "South", residual: -25 },
  { time: "Day 2", region: "South", residual: 35 },
  { time: "Day 3", region: "South", residual: -40 },
  { time: "Day 4", region: "South", residual: -130 }, // outlier
  { time: "Day 5", region: "South", residual: 20 },
];

const RESIDUAL_SD = 40;
const UPPER = 3 * RESIDUAL_SD;
const LOWER = -3 * RESIDUAL_SD;

const regionOptions: Region[] = ["All", "North", "South", "West", "Houston"];

export function OutlierDetectionCard() {
  const [outlierRegion, setOutlierRegion] = useState<Region>("All");

  const residualsFiltered =
    outlierRegion === "All"
      ? residualsData
      : residualsData.filter((r) => r.region === outlierRegion);

  const isOutlier = (residual: number) =>
    residual > UPPER || residual < LOWER;

  const outliers = residualsFiltered.filter((r) =>
    isOutlier(r.residual)
  );

  return (
    <div className="h-80 rounded-xl border border-[#d3dbe8] bg-white flex flex-col pt-4 px-4 shadow-sm">
      <div className="flex items-center justify-between mb-2">
        <span className="text-[#011F5B] font-semibold">
          Outlier Detection (±3 SD)
        </span>
        <div className="flex items-center gap-2">
          <label className="text-xs text-gray-600">Region:</label>
          <select
            className="border border-gray-300 rounded px-2 py-1 text-xs"
            value={outlierRegion}
            onChange={(e) =>
              setOutlierRegion(e.target.value as Region)
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

      <div className="flex-1 flex">
        {/* Left: residual scatter */}
        <div className="flex-1">
          <ResponsiveContainer width="100%" height="100%">
            <ScatterChart margin={{ top: 10, right: 20, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="time" type="category" name="Day" />
              <YAxis dataKey="residual" name="Residual (MW)" />
              <Tooltip />
              <ReferenceLine
                y={UPPER}
                stroke="#999"
                strokeDasharray="4 4"
              />
              <ReferenceLine
                y={LOWER}
                stroke="#999"
                strokeDasharray="4 4"
              />
              <Scatter
                name="Residuals"
                data={residualsFiltered}
                fill="#011F5B"
              />
            </ScatterChart>
          </ResponsiveContainer>
        </div>

        {/* Right: outlier list */}
        <div className="w-40 ml-3 text-xs">
          <div className="font-semibold mb-1">Outliers</div>
          <div className="border border-gray-200 rounded p-2 h-full overflow-auto">
            {outliers.length === 0 && (
              <div className="text-gray-500">None detected</div>
            )}
            {outliers.map((r, idx) => (
              <div
                key={idx}
                className="flex items-center justify-between mb-1"
              >
                <span className="text-red-600 font-bold mr-1">X</span>
                <span>
                  {r.time} ({r.residual.toFixed(0)})
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
